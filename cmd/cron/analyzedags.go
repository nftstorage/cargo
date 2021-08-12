package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-cid"
	ipfsapi "github.com/ipfs/go-ipfs-api"
	"github.com/jackc/pgx/v4"
	"github.com/urfave/cli/v2"
	"golang.org/x/sys/unix"
	"golang.org/x/xerrors"
)

type stats struct {
	analyzed *uint64
	failed   *uint64
	refs     *uint64
	size     *uint64
}

var analyzeDags = &cli.Command{
	Usage: "Analyze DAGs after pinning them locally",
	Name:  "analyze-dags",
	Flags: []cli.Flag{
		&cli.UintFlag{
			Name:  "skip-dags-aged",
			Usage: "If a dag is older than that many days - skip over it",
			Value: 5,
		},
	},
	Action: func(cctx *cli.Context) error {

		var ctxCloser func()
		cctx.Context, ctxCloser = context.WithCancel(cctx.Context)
		defer ctxCloser()

		var res struct{ Keys map[string]ipfsapi.PinInfo }
		if err := ipfsAPI(cctx).Request("pin/ls").Option("type", "recursive").Option("quiet", "true").Exec(cctx.Context, &res); err != nil {
			return err
		}
		systemPins := make(map[cid.Cid]struct{}, len(res.Keys))
		for cidStr := range res.Keys {
			c, err := cid.Parse(cidStr)
			if err != nil {
				return err
			}
			systemPins[cidv1(c)] = struct{}{}
		}

		rows, err := db.Query(
			cctx.Context,
			`
			SELECT
					cid_v1,
					( entry_last_updated < ( NOW() - $1::INTERVAL ) ) AS dag_too_old,
					EXISTS (
						SELECT 42
							FROM cargo.dag_sources ds
							JOIN cargo.sources s USING ( srcid )
						WHERE
							d.cid_v1 = ds.cid_v1
								AND
							ds.entry_removed IS NULL
								AND
							( s.weight IS NULL OR s.weight >= 0 )
					) AS src_is_active
				FROM cargo.dags d
			WHERE size_actual IS NULL
			ORDER BY
				(
					SELECT MAX( COALESCE( s.weight, 100 ))
						FROM cargo.dag_sources ds JOIN cargo.sources s USING ( srcid )
					WHERE d.cid_v1 = ds.cid_v1
				) DESC,
				entry_created DESC -- ensure newest arrivals are attempted first
			`,
			fmt.Sprintf("%d days", cctx.Uint("skip-dags-aged")),
		)
		if err != nil {
			return err
		}

		pinsSeen := cid.NewSet()
		dagsToProcess := make([]cid.Cid, 0, 128<<10)
		dagsToDownloadAndProcess := make([]cid.Cid, 0, 64<<10)

		var alreadyKnownCount int64
		var cidStr string
		var dagTooOld, srcIsActive bool
		for rows.Next() {
			if err = rows.Scan(&cidStr, &dagTooOld, &srcIsActive); err != nil {
				return err
			}
			c, err := cid.Parse(cidStr)
			if err != nil {
				return err
			}
			if !pinsSeen.Visit(c) {
				// duplicate
				continue
			}

			if _, alreadyPinned := systemPins[c]; alreadyPinned {
				// we already have it ( a sweeper picked it up )
				// goes straight to the process queue, regardless of status/age
				alreadyKnownCount++
				dagsToProcess = append(dagsToProcess, c)
			} else if dagTooOld {
				// sorry, old yeller
				continue
			} else if srcIsActive {
				// we are instructed to try to download it right away
				// goes to the queue right after the "already present"
				dagsToDownloadAndProcess = append(dagsToDownloadAndProcess, c)
			}
			// everything else can wait until the sweeper picks it up OR the source (re)activates
		}
		if err := rows.Err(); err != nil {
			return err
		}

		total := stats{
			analyzed: new(uint64),
			failed:   new(uint64),
			refs:     new(uint64),
			size:     new(uint64),
		}

		defer func() {
			log.Infow("summary",
				"totalSystemPins", len(systemPins),
				"prepinnedBySweeper", alreadyKnownCount,
				"analyzed", atomic.LoadUint64(total.analyzed),
				"failed", atomic.LoadUint64(total.failed),
				"referencedBlocks", atomic.LoadUint64(total.refs),
				"bytes", atomic.LoadUint64(total.size),
			)
		}()

		todoCount := uint64(len(dagsToDownloadAndProcess) + len(dagsToProcess))
		toAnalyzeCh := make(chan cid.Cid, todoCount)
		// process in order - first what we already have, then everything else
		for _, c := range dagsToProcess {
			toAnalyzeCh <- c
		}
		for _, c := range dagsToDownloadAndProcess {
			toAnalyzeCh <- c
		}
		close(toAnalyzeCh)

		log.Infof("about to analyze/pin %d dags", len(toAnalyzeCh))

		workerCount := len(toAnalyzeCh)
		if workerCount == 0 {
			return nil
		} else if workerCount > cctx.Int("ipfs-api-max-workers") {
			workerCount = cctx.Int("ipfs-api-max-workers")
		}

		workerStates := make([]*atomic.Value, workerCount)
		doneCh := make(chan struct{}, workerCount) // this effectively emulates a sync.WaitGroup
		errCh := make(chan error, workerCount)
		for i := 0; i < workerCount; i++ {
			state := new(atomic.Value)
			workerStates[i] = state

			go func() {
				defer func() {
					state.Store("exitted")
					doneCh <- struct{}{}
				}()

				for {
					c, chanOpen := <-toAnalyzeCh
					if !chanOpen {
						return
					}
					err := pinAndAnalyze(cctx, c, total, state)
					if err != nil {
						errCh <- err
						return
					}
				}
			}()
		}

		dumpWorkerState := make(chan os.Signal, 1)
		signal.Notify(dumpWorkerState, unix.SIGUSR1)

		var progressTick <-chan time.Time
		lastPct := uint64(101)
		if showProgress {
			fmt.Fprint(os.Stderr, "0%\r")
			t := time.NewTicker(250 * time.Millisecond)
			progressTick = t.C
			defer t.Stop()
		}

		var workerError error

	watchdog:
		for {
			select {

			case <-progressTick:
				curPct := 100 * (atomic.LoadUint64(total.failed) + atomic.LoadUint64(total.analyzed)) / todoCount
				if curPct != lastPct {
					lastPct = curPct
					fmt.Fprintf(os.Stderr, "%d%%\r", lastPct)
				}

			case <-dumpWorkerState:
				for i := 0; i < len(workerStates); i++ {
					s := workerStates[i].Load().(string)
					if s != "exitted" {
						log.Infof("Worker #%d: %s", i, s)
					}
				}

			case <-doneCh:
				workerCount--
				if workerCount == 0 {
					break watchdog
				}

			case <-cctx.Context.Done():
				break watchdog

			case workerError = <-errCh:
				ctxCloser()
				break watchdog
			}
		}

		// wg.Wait()
		for workerCount > 0 {
			<-doneCh
			workerCount--
		}
		close(errCh) // no writers remain
		if showProgress {
			defer fmt.Fprint(os.Stderr, "100%\n")
		}

		if workerError != nil {
			return workerError
		}
		if err := <-errCh; err != nil {
			return err
		}
		return cctx.Context.Err()
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

func pinAndAnalyze(cctx *cli.Context, rootCid cid.Cid, total stats, currentState *atomic.Value) (err error) {

	api := ipfsAPI(cctx)

	// open a tx only when/if we need one, do not hold up pg connections
	var tx pgx.Tx

	defer func() {

		if err != nil {
			atomic.AddUint64(total.failed, 1)
			// Timeouts during analysis are non-fatal, but still logged as an error
			if ue, castOk := err.(*url.Error); castOk && ue.Timeout() {
				log.Errorf("aborting '%s' of '%s' due to timeout: %s", ue.Op, ue.URL, ue.Unwrap().Error())
				err = nil
			}
		}

		// pull in a possible cancel if any
		if err == nil {
			err = cctx.Context.Err()
		}

		if err != nil {
			if tx != nil {
				tx.Rollback(context.Background()) //nolint:errcheck
			}
		} else if tx != nil {
			err = tx.Commit(cctx.Context)
		}

		currentState.Store("idle")
	}()

	currentState.Store("API /pin/add " + rootCid.String())
	if err = api.Request("pin/add").Arguments(rootCid.String()).Exec(cctx.Context, nil); err != nil {
		// If we fail to even pin: move on without an error ( we didn't write anything to the DB yet )
		atomic.AddUint64(total.failed, 1)
		msg := fmt.Sprintf("failure to pin %s: %s", rootCid, err)
		if ue, castOk := err.(*url.Error); castOk && ue.Timeout() {
			log.Debug(msg)
		} else {
			log.Error(msg)
		}
		return nil
	}

	// We got that far: means we have the pin
	// Allow for obscenely long stat/refs times
	api.SetTimeout(time.Second * time.Duration(cctx.Uint("ipfs-api-timeout")) * 15)

	currentState.Store("API /dag/stat " + rootCid.String())
	ds := new(dagStat)
	err = api.Request("dag/stat").Arguments(rootCid.String()).Option("progress", "false").Exec(cctx.Context, ds)
	if err != nil {
		return err
	}

	if ds.NumBlocks > 1 {

		currentState.Store("API /refs " + rootCid.String())
		resp, err := api.Request("refs").Arguments(rootCid.String()).Option("unique", "true").Option("recursive", "true").Send(cctx.Context)
		if err != nil {
			return err
		}

		dec := json.NewDecoder(resp.Output)
		refs := make([][]interface{}, 0, 256)
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

		currentState.Store("DbWrite refs " + rootCid.String())
		tx, err = db.Begin(cctx.Context)
		if err != nil {
			return err
		}

		_, err = tx.CopyFrom(
			cctx.Context,
			pgx.Identifier{"cargo", "refs"},
			[]string{"cid_v1", "ref_v1"},
			pgx.CopyFromRows(refs),
		)
		if err != nil {
			return err
		}

		atomic.AddUint64(total.refs, uint64(len(refs)))
	}

	updSQL := `UPDATE cargo.dags SET size_actual = $1 WHERE cid_v1 = $2`
	updArgs := []interface{}{ds.Size, cidv1(rootCid).String()}

	currentState.Store("DbWrite size_actual " + rootCid.String())
	if tx != nil {
		_, err = tx.Exec(cctx.Context, updSQL, updArgs...)
	} else {
		_, err = db.Exec(cctx.Context, updSQL, updArgs...)
	}
	if err != nil {
		return err
	}

	atomic.AddUint64(total.analyzed, 1)
	atomic.AddUint64(total.size, ds.Size)
	return nil
}
