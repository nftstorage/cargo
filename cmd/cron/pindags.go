package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"sync"
	"sync/atomic"

	"github.com/ipfs/go-cid"
	fslock "github.com/ipfs/go-fs-lock"
	ipfsapi "github.com/ipfs/go-ipfs-api"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

type pinRes struct {
	dagSize uint64
	cid     cid.Cid
	refs    []cid.Cid
}

type stats struct {
	pinned *uint64
	failed *uint64
	refs   *uint64
	size   *uint64
}

const pinDagsName = "pin-dags"

var pinDags = &cli.Command{
	Usage: "Pin and analyze DAGs locally",
	Name:  pinDagsName,
	Flags: []cli.Flag{
		&cli.UintFlag{
			Name:  "skip-dags-aged",
			Usage: "If a dag is older than that many days - ignore it",
			Value: 5,
		},
	},
	Action: func(cctx *cli.Context) error {

		lkCLose, err := fslock.Lock(os.TempDir(), pinDagsName)
		if err != nil {
			return err
		}
		defer lkCLose.Close()

		log.Info("begin pinning round")

		ctx, closer := context.WithCancel(cctx.Context)
		defer closer()

		db, err := connectDb(cctx)
		if err != nil {
			return err
		}

		pinsToDo := make(map[cid.Cid]struct{}, bufPresize)

		rows, err := db.Query(
			ctx,
			`
			SELECT cid_v1 FROM cargo.dags WHERE
				size_actual IS NULL
					AND
				entry_last_updated > ( NOW() - $1::INTERVAL )
			ORDER BY entry_created DESC -- ensure newest arrivals are attempted first
			`,
			fmt.Sprintf("%d days", cctx.Uint("skip-dags-aged")),
		)
		if err != nil {
			return err
		}
		var cidStr string
		for rows.Next() {
			if err = rows.Scan(&cidStr); err != nil {
				return err
			}
			c, err := cid.Parse(cidStr)
			if err != nil {
				return err
			}
			pinsToDo[c] = struct{}{}
		}

		total := stats{
			pinned: new(uint64),
			failed: new(uint64),
			refs:   new(uint64),
			size:   new(uint64),
		}

		defer func() {
			log.Infow("finished",
				"pinned", atomic.LoadUint64(total.pinned),
				"failed", atomic.LoadUint64(total.failed),
				"referencedBlocks", atomic.LoadUint64(total.refs),
				"bytes", atomic.LoadUint64(total.size),
			)
		}()

		maxWorkers := len(pinsToDo)
		if maxWorkers == 0 {
			return nil
		} else if maxWorkers > cctx.Int("ipfs-api-max-workers") {
			maxWorkers = cctx.Int("ipfs-api-max-workers")
		}

		toPinCh := make(chan cid.Cid, 2*maxWorkers)
		errCh := make(chan error, maxWorkers)

		log.Infof("about to pin and analyze %d dags", len(pinsToDo))

		go func() {
			defer close(toPinCh) // signal to workers to quit

			lastPct := uint64(101)
			for c := range pinsToDo {
				select {
				case toPinCh <- c:
					if ShowProgress && 100*atomic.LoadUint64(total.pinned)/uint64(len(pinsToDo)) != lastPct {
						lastPct = 100 * atomic.LoadUint64(total.pinned) / uint64(len(pinsToDo))
						fmt.Fprintf(os.Stderr, "%d%%\r", lastPct)
					}
				case e, isOpen := <-errCh:
					if isOpen && e != nil {
						errCh <- e
					}
					return
				}
			}
		}()

		var wg sync.WaitGroup
		for maxWorkers > 0 {
			maxWorkers--
			wg.Add(1)
			go func() {
				defer wg.Done()
				api := ipfsApi(cctx)

				for {
					c, chanOpen := <-toPinCh
					if !chanOpen {
						return
					}

					if err := pinAndAnalyze(ctx, api, db, c, total); err != nil {
						errCh <- err
						return
					}
				}
			}()
		}

		wg.Wait()
		if ShowProgress {
			defer fmt.Fprint(os.Stderr, "100%\n")
		}

		close(errCh)
		return <-errCh
	},
}

type dagStat struct {
	Size      uint64
	NumBlocks uint64
}
type refEntry struct {
	Ref string
	Err string
}

func pinAndAnalyze(ctx context.Context, api *ipfsapi.Shell, db *pgxpool.Pool, rootCid cid.Cid, total stats) (err error) {

	var tx pgx.Tx

	defer func() {
		if err != nil {

			atomic.AddUint64(total.failed, 1)

			if tx != nil {
				tx.Rollback(ctx) // no error checks
			}

			// Timeouts are non-fatal
			if ue, castOk := err.(*url.Error); castOk && ue.Timeout() {
				log.Warnf("Aborting '%s' of '%s' due to timeout: %s", ue.Op, ue.URL, ue.Unwrap().Error())
				err = nil
			}
		} else if tx != nil {
			err = tx.Commit(ctx)
		}
	}()

	err = api.Request("pin/add").Arguments(rootCid.String()).Exec(ctx, nil)
	if err != nil {
		return err
	}

	ds := new(dagStat)
	err = api.Request("dag/stat").Arguments(rootCid.String()).Option("progress", "false").Exec(ctx, ds)
	if err != nil {
		return err
	}

	if ds.NumBlocks > 1 {

		refs := make([][]interface{}, 0, 256)

		var resp *ipfsapi.Response
		resp, err = api.Request("refs").Arguments(rootCid.String()).Option("unique", "true").Option("recursive", "true").Send(ctx)

		dec := json.NewDecoder(resp.Output)
		for {
			ref := new(refEntry)
			if decErr := dec.Decode(&ref); decErr != nil {
				if decErr == io.EOF {
					break
				}
				err = decErr
				return err
			}
			if ref.Err != "" {
				err = xerrors.New(ref.Err)
				return err
			}

			var refCid cid.Cid
			refCid, err = cid.Parse(ref.Ref)
			if err != nil {
				return err
			}

			refs = append(refs, []interface{}{
				cidv1(rootCid).String(),
				cidv1(refCid).String(),
			})
		}

		tx, err = db.Begin(ctx)
		if err != nil {
			return err
		}

		_, err = tx.CopyFrom(
			ctx,
			pgx.Identifier{"cargo", "refs"},
			[]string{"cid_v1", "ref_v1"},
			pgx.CopyFromRows(refs),
		)
		if err != nil {
			return err
		}

		atomic.AddUint64(total.refs, uint64(len(refs)))
	}

	if tx != nil {
		_, err = tx.Exec(
			ctx,
			`UPDATE cargo.dags SET size_actual = $1 WHERE cid_v1 = $2`,
			ds.Size,
			cidv1(rootCid).String(),
		)
	} else {
		_, err = db.Exec(
			ctx,
			`UPDATE cargo.dags SET size_actual = $1 WHERE cid_v1 = $2`,
			ds.Size,
			cidv1(rootCid).String(),
		)
	}
	if err != nil {
		return err
	}

	atomic.AddUint64(total.pinned, 1)
	atomic.AddUint64(total.size, ds.Size)
	return nil
}
