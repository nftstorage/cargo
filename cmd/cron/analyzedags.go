package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/ipfs/go-cid"
	ipfsapi "github.com/ipfs/go-ipfs-api"
	"github.com/jackc/pgx/v4"
	"github.com/urfave/cli/v2"
	"golang.org/x/sys/unix"
	"golang.org/x/xerrors"
)

type stats struct {
	analyzed  *uint64
	unpinned  *uint64
	failed    *uint64
	refBlocks *uint64
	size      *uint64
}

var analyzeDags = &cli.Command{
	Usage: "Analyze DAGs after pinning them locally",
	Name:  "analyze-dags",
	Flags: []cli.Flag{
		&cli.UintFlag{
			Name:  "skip-dags-aged-hours",
			Usage: "If a dag is not pinned and older than that many hours - do not wait for a pin in-process",
			Value: 2,
		},
		&cli.BoolFlag{
			Name:  "prepinned-only",
			Usage: "Rely on the out-of-band sweepers to pin all content, focusing on analyzis only",
		},
		&cli.BoolFlag{
			Name:  "unpin-after-analysis",
			Usage: "Remove ipfs daemon pin after successfully persisting dag stats",
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
			fmt.Sprintf("%d hours", cctx.Uint("skip-dags-aged-hours")),
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
			} else if srcIsActive && !cctx.Bool("prepinned-only") {
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
			analyzed:  new(uint64),
			unpinned:  new(uint64),
			failed:    new(uint64),
			refBlocks: new(uint64),
			size:      new(uint64),
		}

		defer func() {
			log.Infow("summary",
				"totalSystemPins", len(systemPins),
				"prepinnedBySweeper", alreadyKnownCount,
				"analyzed", atomic.LoadUint64(total.analyzed),
				"unpinned", atomic.LoadUint64(total.unpinned),
				"failed", atomic.LoadUint64(total.failed),
				"referencedBlocks", atomic.LoadUint64(total.refBlocks),
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

		log.Infof("about to analyze %d dags", len(toAnalyzeCh))

		workerCount := len(toAnalyzeCh)
		if workerCount == 0 {
			return nil
		} else if workerCount > cctx.Int("ipfs-api-max-workers") {
			workerCount = cctx.Int("ipfs-api-max-workers")
		}

		workerStates := make([]*atomic.Value, workerCount)
		retCh := make(chan error, workerCount)
		for i := 0; i < workerCount; i++ {
			state := new(atomic.Value)
			workerStates[i] = state

			go func() {
				state.Store("idle")
				defer state.Store("exitted")
				retCh <- func() error {
					for {
						c, chanOpen := <-toAnalyzeCh
						if !chanOpen {
							return nil
						}
						err := pinAndAnalyze(cctx, c, total, state)
						if err != nil {
							return err
						}
					}
				}()
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

		// wg.Wait() + abort on error
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
						log.Infof("Worker % 5s: %s",
							fmt.Sprintf("#%d", i),
							s,
						)
					}
				}

			case err := <-retCh:
				if err != nil {
					ctxCloser()
					if workerError == nil {
						workerError = err
					}
				}
				workerCount--
				if workerCount == 0 {
					break watchdog
				}
			}
		}
		if showProgress {
			defer fmt.Fprint(os.Stderr, "100%\n")
		}

		if workerError != nil {
			return workerError
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

var ipfsShutdownErrorRegex = regexp.MustCompile(`promise channel was closed$`)

func pinAndAnalyze(cctx *cli.Context, rootCid cid.Cid, total stats, currentState *atomic.Value) (err error) {

	ctx, ctxCloser := context.WithCancel(cctx.Context)
	defer ctxCloser()

	api := ipfsAPI(cctx)

	defer func() {
		if err != nil {
			atomic.AddUint64(total.failed, 1)
		}
		currentState.Store("idle")
	}()

	if !cctx.Bool("prepinned-only") {
		currentState.Store("API /pin/add " + rootCid.String())
		if err = api.Request("pin/add").Arguments(rootCid.String()).Exec(ctx, nil); err != nil {
			// If we fail to even pin: move on without an error ( we didn't write anything to the DB yet )
			atomic.AddUint64(total.failed, 1)
			msg := fmt.Sprintf("failure to pin %s: %s", rootCid, err)

			if ipfsShutdownErrorRegex.MatchString(msg) {
				log.Debug(msg)
			} else if ue, castOk := err.(*url.Error); castOk && ue.Timeout() {
				log.Debug(msg)
			} else {
				log.Error(msg)
			}

			return nil
		}
	}

	// We got that far: means we have the pin
	// Allow for obscenely long stat/refs times
	/*
		( this is not even hypothetical, timing below with *hot* caches 😿 )

		~$ time ipfs dag stat --progress=false bafybeiaysi4s6lnjev27ln5icwm6tueaw2vdykrtjkwiphwekaywqhcjze
			Size: 351956725764, NumBlocks: 20684378

			real    108m13.260s
			user    0m18.150s
			sys     0m0.434s

	*/
	api.SetTimeout(3 * time.Hour)

	workerCount := 2
	retCh := make(chan error, workerCount) // this effectively doubles as a sync.WaitGroup

	ds := new(dagStat)
	refs := make([][]interface{}, 0, 1024) // interface{} as these go directly into pgx.CopyFrom

	currentState.Store("API (/dag/stat + /refs) " + rootCid.String())
	go func() {
		retCh <- api.Request("dag/stat").Arguments(rootCid.String()).Option("progress", "false").Option("offline", true).Exec(ctx, ds)
	}()
	go func() {
		retCh <- func() error {

			resp, err := api.Request("refs").Arguments(rootCid.String()).Option("unique", "true").Option("recursive", "true").Option("offline", true).Send(ctx)
			if err != nil {
				return err
			}

			dec := json.NewDecoder(resp.Output)
			for {

				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					// go on
				}

				ref := new(refEntry)
				if decErr := dec.Decode(&ref); decErr != nil {
					if decErr == io.EOF {
						return nil
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
					refCid.String(),
				})

				if (len(refs) % 1024) == 0 {
					currentState.Store(fmt.Sprintf("API (/dag/stat + /refs) %s (%d unique refs processed)", rootCid.String(), len(refs)))
				}
			}

		}()
	}()

	// wg.Wait() + abort on error
	var workerError error
	for {
		err := <-retCh

		if err != nil {
			ctxCloser()
			if workerError == nil {
				workerError = err
			}
		}

		workerCount--
		if workerCount == 0 {
			break
		}
	}
	if workerError != nil {
		return workerError
	}

	currentState.Store(fmt.Sprintf("DbWrite size(%s) + refs(%s) %s", humanize.Comma(int64(ds.Size)), humanize.Comma(int64(len(refs))), rootCid.String()))
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		// pull in a possible cancel if any
		if err == nil {
			err = ctx.Err()
		}

		if err == nil {
			err = tx.Commit(ctx)
		}

		if err != nil {

			tx.Rollback(context.Background()) //nolint:errcheck

		} else {

			if cctx.Bool("unpin-after-analysis") {
				currentState.Store(fmt.Sprintf("pin/rm %s", rootCid.String()))
				unpinErr := api.Request("pin/rm").Arguments(rootCid.String()).Exec(context.Background(), nil)
				if unpinErr != nil {
					log.Warnf("unpinning of %s after successful analysis failed: %s", rootCid.String(), unpinErr)
				} else {
					atomic.AddUint64(total.unpinned, 1)
				}
			}

			atomic.AddUint64(total.analyzed, 1)
			atomic.AddUint64(total.refBlocks, uint64(len(refs)))
			atomic.AddUint64(total.size, ds.Size)
		}
	}()

	if len(refs) > 0 {
		// raise default timeout for the transaction scope: the reflist could be *massive*
		_, err = tx.Exec(cctx.Context, fmt.Sprintf(`SET LOCAL statement_timeout = %d`, (2*time.Hour).Milliseconds()))
		if err != nil {
			return err
		}

		_, err = tx.CopyFrom(
			cctx.Context,
			pgx.Identifier{"cargo", "refs"},
			[]string{"cid_v1", "ref_cid"},
			pgx.CopyFromRows(refs),
		)
		if err != nil {
			return err
		}
	}

	_, err = tx.Exec(
		ctx,
		`UPDATE cargo.dags SET size_actual = $1, entry_analyzed = NOW() WHERE cid_v1 = $2`,
		ds.Size,
		cidv1(rootCid).String(),
	)
	return err
}
