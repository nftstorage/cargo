package main

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/go-dagaggregator-unixfs"
	"github.com/filecoin-project/go-dagaggregator-unixfs/lib/rambs"
	commcid "github.com/filecoin-project/go-fil-commcid"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	filabi "github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	ipfsapi "github.com/ipfs/go-ipfs-api"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	exchangeoffline "github.com/ipfs/go-ipfs-exchange-offline"
	ipfsfiles "github.com/ipfs/go-ipfs-files"
	"github.com/ipfs/go-merkledag"
	"github.com/jackc/pgx/v4"
	sha256simd "github.com/minio/sha256-simd"
	"github.com/mitchellh/go-homedir"
	"github.com/multiformats/go-multihash"
	"github.com/tmthrgd/tmpfile"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

const (
	defaultAggregateSettleDelayHours = 1
	estimatedSingleBlockCarOverhead  = 3 + 38 + 1             // > 16k car-frame + blake2b cid v1 + off-by-one
	targetMaxSize                    = uint64(34_000_000_000) // tatget bytes of payload including approximated car overhead *excluding* aggregation overhead
	aggregateType                    = "DagAggregate UnixFS"

	unixReadable = os.FileMode(0644)
)

var targetMinSizeSoft, targetMinSizeHard uint64
var concurrentExports, settleDelayHours, forceAgeHours uint
var captureAggregateCandidatesSnapshot bool
var carExportDir string

type pendingDag struct {
	aggentry  dagaggregator.AggregateDagEntry
	srcid     int64
	timeStamp time.Time
}

type aggregateResult struct {
	standaloneEntries []dagaggregator.AggregateDagEntry
	manifestEntries   []*dagaggregator.ManifestDagEntry
	carSize           uint64
	carPieceSize      filabi.PaddedPieceSize
	carRoot           cid.Cid
	carCommp          cid.Cid
	carSha256         []byte
	carMd5            []byte
}

type runningTotals struct {
	newAggregatesTotal       *uint64
	dagsAggregatedStandalone *uint64
	dagsAggregatedTotal      *uint64
}

var aggregateDags = &cli.Command{
	Usage: "Aggregate available dags if any",
	Name:  "aggregate-dags",
	Flags: []cli.Flag{
		&cli.PathFlag{
			Required:    true,
			Name:        "export-dir",
			Usage:       "A pre-existing directory with sufficient space to export .car files into",
			Destination: &carExportDir,
		},
		&cli.Uint64Flag{
			Name:        "min-size-soft",
			Usage:       "The included payload should not be smaller than this",
			Value:       24_000_000_000,
			Destination: &targetMinSizeSoft,
		},
		&cli.Uint64Flag{
			Name:        "min-size-hard",
			Usage:       "The resulting car file CAN NOT be smaller than this",
			Value:       (16<<30)/128*127 + 1,
			Destination: &targetMinSizeHard,
		},
		&cli.UintFlag{
			Name:        "max-concurrent-exports",
			Usage:       "Maximum amount of exports that can run at the same time (IO-bound)",
			Value:       8,
			Destination: &concurrentExports,
		},
		&cli.UintFlag{
			Name:        "settle-delay-hours",
			Usage:       "Amount of hours before considering an entry for inclusion",
			Value:       defaultAggregateSettleDelayHours,
			Destination: &settleDelayHours,
		},
		&cli.UintFlag{
			Name:        "force-aggregation-hours",
			Usage:       "When the pending set includes a CID that many hours old, mix in preexisting aggregates to force a new one",
			Value:       12,
			Destination: &forceAgeHours,
		},
		&cli.BoolFlag{
			Name:  "skip-pinning",
			Usage: "do not pin resulting aggregates - rely on out-of-band advertisers",
		},
		&cli.BoolFlag{
			Name:  "unpin-sources",
			Usage: "remove the pins of all members of a successful aggregation",
		},
		&cli.BoolFlag{
			Name:        "snapshot-aggregate-candidates",
			Usage:       "(debug) capture a materialized view of the available candidate list",
			Destination: &captureAggregateCandidatesSnapshot,
		},
	},
	Action: func(cctx *cli.Context) error {

		var err error
		carExportDir, err = homedir.Expand(carExportDir)
		if err != nil {
			return err
		}
		if st, err := os.Stat(carExportDir); err != nil || !st.IsDir() {
			if err == nil {
				err = xerrors.Errorf("filemode %s is not a directory", st.Mode().String())
			}
			return xerrors.Errorf("check of '%s' failed: %w", carExportDir, err)
		}

		ctx, closer := context.WithCancel(cctx.Context)
		defer closer()

		toAggRemaining, forceTimeboxedAggregation, dagSourcesCount, err := aggregationCandidates(ctx)
		if err != nil {
			return err
		}

		var standaloneCandidateBytes uint64
		for _, pending := range toAggRemaining {
			standaloneCandidateBytes += pending.aggentry.UniqueBlockCumulativeSize + pending.aggentry.UniqueBlockCount*estimatedSingleBlockCarOverhead
		}
		standaloneCandidateCount := len(toAggRemaining)
		log.Infof("%s standalone aggregation candidates found, projected to weigh %s bytes",
			humanize.Comma(int64(standaloneCandidateCount)),
			humanize.Comma(int64(standaloneCandidateBytes)),
		)

		stats := runningTotals{
			newAggregatesTotal:       new(uint64),
			dagsAggregatedStandalone: new(uint64),
			dagsAggregatedTotal:      new(uint64),
		}
		var lastRoundAgg []dagaggregator.AggregateDagEntry
		defer func() {
			log.Infow("summary",
				"initialCandidates", standaloneCandidateCount,
				"uniqueCandidateSources", dagSourcesCount,
				"forceTimeboxedAggregation", forceTimeboxedAggregation,
				"aggregatesAssembled", *stats.newAggregatesTotal,
				"dagsAggregatedStandalone", *stats.dagsAggregatedStandalone,
				"dagsAggregatedTotal", *stats.dagsAggregatedTotal,
			)
		}()

		// first aggregation pass
		// loop until we arrive at definitive lack of data
		aggBundles := make([][]dagaggregator.AggregateDagEntry, 0, 128)
		for len(toAggRemaining) > 0 {
			var runBytes uint64
			curRoundSources := make(map[int64]struct{})

			// reset
			lastRoundAgg = make([]dagaggregator.AggregateDagEntry, 0, len(toAggRemaining))

			// run forward through the ordered list, until we overflow
			for len(toAggRemaining) > 0 &&
				runBytes+
					toAggRemaining[0].aggentry.UniqueBlockCumulativeSize+
					toAggRemaining[0].aggentry.UniqueBlockCount*estimatedSingleBlockCarOverhead <= targetMaxSize {
				d := toAggRemaining[0]
				runBytes += d.aggentry.UniqueBlockCumulativeSize + d.aggentry.UniqueBlockCount*estimatedSingleBlockCarOverhead
				curRoundSources[d.srcid] = struct{}{}
				lastRoundAgg = append(lastRoundAgg, d.aggentry)
				toAggRemaining = toAggRemaining[1:]
			}

			// common code to reuse twice below
			runBackwardsThroughRemaining := func(extraSkipFunc func(i int) bool) {
				for i := len(toAggRemaining) - 1; runBytes < targetMinSizeSoft && i >= 0; i-- {
					d := toAggRemaining[i]

					if runBytes+
						d.aggentry.UniqueBlockCumulativeSize+
						d.aggentry.UniqueBlockCount*estimatedSingleBlockCarOverhead > targetMaxSize || extraSkipFunc(i) {
						continue
					}

					runBytes += d.aggentry.UniqueBlockCumulativeSize + d.aggentry.UniqueBlockCount*estimatedSingleBlockCarOverhead
					lastRoundAgg = append(lastRoundAgg, d.aggentry)
					toAggRemaining = toAggRemaining[:i+copy(toAggRemaining[i:], toAggRemaining[i+1:])]
				}
			}

			// now run backwards, to "pad up" the list with small dags from the sources in current round
			runBackwardsThroughRemaining(func(i int) bool { _, seen := curRoundSources[toAggRemaining[i].srcid]; return !seen })

			// not enough - try to pad up with anything at all that fits
			if runBytes < targetMinSizeSoft {
				runBackwardsThroughRemaining(func(int) bool { return false })
			}

			// we can't find enough to make it worthwhile for this bundle
			// assemble the next one instead
			if runBytes < targetMinSizeHard ||
				(!forceTimeboxedAggregation && runBytes < targetMinSizeSoft) {
				continue
			}

			// We have enough to aggregate!
			aggBundles = append(aggBundles, lastRoundAgg)
		}

		//
		// go through the proposed bundles in parallel, see what makes it
		undersizedInvalidCars, err := reifyAggregateCars(cctx, stats, false, aggBundles)
		if err != nil {
			return err
		}

		//
		// recombination step
		// a lot of cars deduplicate to a mere fraction of their payload size 😿
		//

		if len(lastRoundAgg) > 0 {
			// if we had some leftovers, model them as a "virtual undersized car"
			// they might get merged somewhere too
			// ( a bit icky since we do not know the correct size yet, but meh... )
			var pseudoCarSize uint64
			for i := range lastRoundAgg {
				pseudoCarSize += lastRoundAgg[i].UniqueBlockCumulativeSize + lastRoundAgg[i].UniqueBlockCount*estimatedSingleBlockCarOverhead
			}
			undersizedInvalidCars = append(undersizedInvalidCars, aggregateResult{
				standaloneEntries: lastRoundAgg,
				carSize:           pseudoCarSize,
			})
			lastRoundAgg = lastRoundAgg[:0]
		}

		// keep looping as long as we ended up with fewer than last time
		lastRoundRetried := 1 + len(undersizedInvalidCars)
		for lastRoundRetried > len(undersizedInvalidCars) && len(undersizedInvalidCars) > 1 {

			lastRoundRetried = len(undersizedInvalidCars)

			sort.Slice(undersizedInvalidCars, func(i, j int) bool {
				return undersizedInvalidCars[i].carSize < undersizedInvalidCars[j].carSize
			})
			var standaloneCount int64
			sizeStrings := make([]string, len(undersizedInvalidCars))
			for i, u := range undersizedInvalidCars {
				sizeStrings[i] = humanize.Comma(int64(u.carSize))
				standaloneCount += int64(len(u.standaloneEntries))
			}
			log.Infof("attempting to recombine %s standalone dags from %s undersized cars/groups with lengths:  %s",
				humanize.Comma(standaloneCount),
				humanize.Comma(int64(len(undersizedInvalidCars))),
				strings.Join(sizeStrings, "  "),
			)

			shardSizes := make([][]string, 0)
			aggBundles = make([][]dagaggregator.AggregateDagEntry, 0)
			for len(undersizedInvalidCars) > 1 {
				var runBytes uint64

				// assume uniform-ish distribution of large/small
				// alternate between smallest and largest for recombination
				// ( *must* start from smallest, which is sorted first )
				targets := make([]int, 0)
				maxIdx := len(undersizedInvalidCars) - 1
				for halfIdx := 0; halfIdx <= maxIdx/2; halfIdx++ {

					if runBytes+undersizedInvalidCars[halfIdx].carSize <= targetMaxSize {
						runBytes += undersizedInvalidCars[halfIdx].carSize
						targets = append(targets, halfIdx)
					}
					if halfIdx != maxIdx-halfIdx &&
						runBytes+undersizedInvalidCars[maxIdx-halfIdx].carSize <= targetMaxSize {
						runBytes += undersizedInvalidCars[maxIdx-halfIdx].carSize
						targets = append(targets, maxIdx-halfIdx)
					}
				}

				// we can't do anything more this round
				if len(targets) < 2 || runBytes < targetMinSizeHard {
					break
				}

				// We have enough to retry!
				// splice out the targets, record a bundle
				newEntry := make([]dagaggregator.AggregateDagEntry, 0, len(targets))
				newShardSizeList := make([]string, 0, len(targets))
				sort.Slice(targets, func(i, j int) bool { return targets[j] < targets[i] })
				for _, j := range targets {
					u := undersizedInvalidCars[j]
					undersizedInvalidCars = undersizedInvalidCars[:j+copy(undersizedInvalidCars[j:], undersizedInvalidCars[j+1:])]

					newEntry = append(newEntry, u.standaloneEntries...)
					newShardSizeList = append(newShardSizeList, fmt.Sprintf("%s(%d)",
						humanize.Comma(int64(u.carSize)),
						len(u.standaloneEntries),
					))
				}
				aggBundles = append(aggBundles, newEntry)
				shardSizes = append(shardSizes, newShardSizeList)
			}

			if len(aggBundles) > 0 {
				bundleShards := make([]string, len(shardSizes))
				for i := range shardSizes {
					bundleShards[i] = fmt.Sprintf("[ %s ]", strings.Join(shardSizes[i], " + "))
				}

				log.Infof("retrying %d recombination candidates: %s",
					len(aggBundles),
					strings.Join(bundleShards, "  "),
				)

				newInvalidCars, err := reifyAggregateCars(cctx, stats, false, aggBundles)
				if err != nil {
					return err
				}
				undersizedInvalidCars = append(undersizedInvalidCars, newInvalidCars...)
			}
		}

		//
		// we did something OR we are not under sufficient pressure OR nothing to do: enough for this run
		if *stats.newAggregatesTotal > 0 || !forceTimeboxedAggregation || len(undersizedInvalidCars) == 0 {
			return nil
		}

		//
		// rehydration step
		// If we did not manage to do anything at all, and we are under pressure,
		// just select the least-replicated content and mix it with whatever is
		// available. Ugly but meh...
		// ( at this stage we are guaranteed to be less-than-hard-minimum-undeduped )
		if len(undersizedInvalidCars) > 1 {
			return xerrors.Errorf("impossible: rehydration attempt with more than 1 undersized car")
		}

		log.Info("forcing time-boxed rehydration: retrieving list of preexisting already-packaged standalone dags")

		finalDitchAgg := undersizedInvalidCars[0].standaloneEntries
		if err := cargoDb.BeginTxFunc(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(rotx pgx.Tx) error {
			if _, err := rotx.Exec(ctx, fmt.Sprintf(`SET LOCAL statement_timeout = %d`, (2*time.Hour).Milliseconds())); err != nil {
				return err
			}

			rows, err := rotx.Query(
				ctx,
				`
				WITH dag_candidates AS (
					SELECT
							ae.cid_v1,
							d.size_actual
						FROM cargo.aggregate_entries ae
						JOIN cargo.dags d USING ( cid_v1 )
						LEFT JOIN cargo.deals de -- this inflates the replica_count, conflating 0 with 1 ( always 1 ), which is ok
							ON de.aggregate_cid = ae.aggregate_cid AND de.status != 'terminated'
					WHERE
						-- don't go with big dags, don't risk it
						d.size_actual > 0 AND d.size_actual < $1
							AND
						-- do not republish deleted/de-prioritized dags
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
						)
					GROUP BY ( ae.cid_v1, d.size_actual )
					ORDER BY COUNT(*)
					LIMIT $2
				)
				SELECT
						d.cid_v1,
						d.size_actual,
						( SELECT 1+COUNT(*) FROM cargo.refs sr WHERE sr.cid_v1 = d.cid_v1 ) AS node_count
					FROM dag_candidates d
					LEFT JOIN cargo.refs r
						ON d.cid_v1 = r.ref_cid
				WHERE r.ref_cid IS NULL -- not part of anything else
				ORDER BY RANDOM()
				`,
				targetMinSizeHard,
				1_000_000,
			)
			if err != nil {
				return err
			}
			defer rows.Close()

			// run through *everything* attempting to pack things as tightly as possible
			// ( up to 1 mil records )
			runBytes := undersizedInvalidCars[0].carSize
			for rows.Next() {
				var ae dagaggregator.AggregateDagEntry
				var cidStr string
				if err = rows.Scan(&cidStr, &ae.UniqueBlockCumulativeSize, &ae.UniqueBlockCount); err != nil {
					return err
				}

				// will overflow, nope
				if runBytes+
					ae.UniqueBlockCumulativeSize+
					ae.UniqueBlockCount*estimatedSingleBlockCarOverhead > targetMaxSize {
					continue
				}

				ae.RootCid, err = cid.Parse(cidStr)
				if err != nil {
					return err
				}

				// good, let's try it!
				finalDitchAgg = append(finalDitchAgg, ae)
				runBytes += ae.UniqueBlockCumulativeSize + ae.UniqueBlockCount*estimatedSingleBlockCarOverhead
			}
			return rows.Err()
		}); err != nil {
			return err
		}

		// if it works - it works
		_, err = reifyAggregateCars(cctx, stats, true, [][]dagaggregator.AggregateDagEntry{finalDitchAgg})
		return err
	},
}

func aggregationCandidates(ctx context.Context) ([]pendingDag, bool, int, error) {

	// the ORDER BY is critical so that we can group same-source dags together
	masterListSQL := fmt.Sprintf(
		`
		SELECT
			cand.*,
			( SELECT 1+COUNT(*) FROM cargo.refs r WHERE r.cid_v1 = cand.cid_v1 ) AS node_count
		FROM ( %s ) cand
		ORDER BY weight DESC NULLS FIRST, srcid, size_actual DESC, cid_v1
		`,
		eligibleForAggregationSQL(targetMaxSize, settleDelayHours),
	)

	if captureAggregateCandidatesSnapshot {
		mvName := `cargo.debug_aggregate_candidates_snapshot__` + time.Now().Format("2006_01_02__15_04_05")
		_, err := cargoDb.Exec(ctx, fmt.Sprintf("CREATE MATERIALIZED VIEW %s AS\n%s", mvName, masterListSQL))
		if err != nil {
			return nil, false, 0, err
		}
		masterListSQL = `SELECT * FROM ` + mvName
	}

	rotx, err := cargoDb.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, false, 0, err
	}
	defer rotx.Rollback(context.Background()) //nolint:errcheck

	_, err = rotx.Exec(ctx, fmt.Sprintf(`SET LOCAL statement_timeout = %d`, (6*time.Hour).Milliseconds()))
	if err != nil {
		return nil, false, 0, err
	}

	rows, err := rotx.Query(ctx, masterListSQL)
	if err != nil {
		return nil, false, 0, err
	}
	defer rows.Close()

	seenDags := cid.NewSet()
	seenSources := make(map[int64]struct{})
	pendingDags := make([]pendingDag, 0, 256<<10)
	var oldestPending *pendingDag

	for rows.Next() {
		var pending pendingDag
		var cidStr string

		if err = rows.Scan(&pending.srcid, &cidStr, &pending.aggentry.UniqueBlockCumulativeSize, &pending.timeStamp, nil, nil, nil, &pending.aggentry.UniqueBlockCount); err != nil {
			return nil, false, 0, err
		}

		pending.aggentry.RootCid, err = cid.Parse(cidStr)
		if err != nil {
			return nil, false, 0, err
		}

		if oldestPending == nil || pending.timeStamp.Before(oldestPending.timeStamp) {
			oldestPending = &pending
		}

		seenSources[pending.srcid] = struct{}{}

		// register first as-ordered occurrence of CID only
		if seenDags.Visit(pending.aggentry.RootCid) {
			pendingDags = append(pendingDags, pending)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, false, 0, err
	}

	var forceTimeboxedAggregation bool
	if oldestPending != nil && forceAgeHours > 0 {
		if oldestPending.timeStamp.Before(time.Now().Add(-1 * time.Hour * time.Duration(forceAgeHours))) {
			forceTimeboxedAggregation = true
			log.Infof(
				"forcing time-boxed aggregation: entry %s timestamped %s is older than requested %d hour cutoff",
				oldestPending.aggentry.RootCid,
				oldestPending.timeStamp,
				forceAgeHours,
			)
		}
	}

	return pendingDags, forceTimeboxedAggregation, len(seenSources), nil
}

func eligibleForAggregationSQL(targetMaxSize uint64, settleDelayHours uint) string {
	return fmt.Sprintf(
		`
		-- bend over backwards through *multiple* set-subtractions, because again: NO FUCKING HINTS
			(
				SELECT
						ds.srcid,
						ds.cid_v1,
						d.size_actual,
						d.entry_analyzed,
						ds.entry_created,
						s.project,
						s.weight
					FROM cargo.dag_sources ds
					JOIN cargo.dags d USING ( cid_v1 )
					JOIN cargo.sources s USING ( srcid )
					-- not yet aggregated anti-join (IS NULL below)
					LEFT JOIN cargo.aggregate_entries ae USING ( cid_v1 )
				WHERE
					-- only analysed entries (FIXME for now do not deal with oversizes/that comes later)
					( d.size_actual IS NOT NULL AND d.size_actual <= %[1]d )
						AND
					-- no inactive sources
					( s.weight >= 0 OR s.weight IS NULL )
						AND
					-- not yet aggregated
					ae.cid_v1 IS NULL

			-- exclude everything that is "too young"
			-- give enough time for metadata/containing dags to trickle in too, allowing for source outages
			EXCEPT

				SELECT
						ds.srcid,
						ds.cid_v1,
						d.size_actual,
						d.entry_analyzed,
						ds.entry_created,
						s.project,
						s.weight
					FROM cargo.dag_sources ds
					JOIN cargo.dags d USING ( cid_v1 )
					JOIN cargo.sources s USING ( srcid )
					LEFT JOIN cargo.aggregate_entries ae USING ( cid_v1 )
					LEFT JOIN cargo.refs r
							ON ds.cid_v1 = r.cid_v1
					LEFT JOIN cargo.dags chd
							ON r.ref_cid = chd.cid_v1
					LEFT JOIN cargo.dag_sources chds
							ON chd.cid_v1 = chds.cid_v1
					LEFT JOIN cargo.aggregate_entries chae
							ON chd.cid_v1 = chae.cid_v1
				WHERE
					-- no inactive sources
					( s.weight >= 0 OR s.weight IS NULL )
						AND
					-- neither parent nor children aggregated
					( ae.cid_v1 IS NULL AND chae.cid_v1 IS NULL )
						AND
					d.entry_analyzed > ( NOW() - '%[2]s'::INTERVAL )
						AND
					(
						chd.entry_analyzed IS NULL
							OR
						(
							-- same source is important to ascertain timing
							ds.srcid = chds.srcid
									AND
							chd.entry_analyzed > ( NOW() - '%[2]s'::INTERVAL )
						)
					)
			)
		`,
		targetMaxSize,
		fmt.Sprintf("%d hours", settleDelayHours),
	)
}

var reifyRoundsCount int

func reifyAggregateCars(cctx *cli.Context, stats runningTotals, timeboxingActive bool, aggBundles [][]dagaggregator.AggregateDagEntry) ([]aggregateResult, error) {

	reifyRoundsCount++

	if len(aggBundles) == 0 {
		return nil, nil
	}

	log.Infof("ROUND %d: reifying %s aggregates as car files", reifyRoundsCount-1, humanize.Comma(int64(len(aggBundles))))

	var ctxCloser func()
	oldCtx := cctx.Context
	cctx.Context, ctxCloser = context.WithCancel(oldCtx)
	defer func() {
		cctx.Context = oldCtx
		ctxCloser()
	}()

	// we know how much work there is, just prepare it
	todoCh := make(chan []dagaggregator.AggregateDagEntry, len(aggBundles))
	sort.Slice(aggBundles, func(i, j int) bool {
		switch {
		case len(aggBundles[i]) != len(aggBundles[j]):
			return len(aggBundles[j]) < len(aggBundles[i])
		default:
			return aggBundles[i][0].RootCid.String() < aggBundles[j][0].RootCid.String()
		}
	})
	for _, b := range aggBundles {
		todoCh <- b
	}
	close(todoCh)

	var undersizedMu sync.Mutex
	undersized := make([]aggregateResult, 0, len(aggBundles))

	var wg sync.WaitGroup
	errCh := make(chan error, concurrentExports)

	for i := uint(0); i < concurrentExports; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-cctx.Context.Done():
					return
				case toAgg, chanOpen := <-todoCh:

					if !chanOpen {
						return
					}
					res, err := aggregateAndAnalyze(cctx, carExportDir, toAgg, timeboxingActive)
					if err != nil {
						errCh <- err
						ctxCloser()
						return
					}

					if res.carSize < targetMinSizeHard {
						undersizedMu.Lock()
						undersized = append(undersized, *res)
						undersizedMu.Unlock()
					} else {
						// we did properly save an aggregate car file, bump counts
						atomic.AddUint64(stats.newAggregatesTotal, 1)
						atomic.AddUint64(stats.dagsAggregatedStandalone, uint64(len(toAgg)))
						atomic.AddUint64(stats.dagsAggregatedTotal, uint64(len(res.manifestEntries)))
					}
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)

	if err := <-errCh; err != nil {
		return nil, err
	}

	if len(undersized) == 0 {
		return nil, nil
	}

	return undersized, nil
}

func aggregateAndAnalyze(cctx *cli.Context, outDir string, toAgg []dagaggregator.AggregateDagEntry, isTimeboxed bool) (*aggregateResult, error) {
	ctx, ctxCloser := context.WithCancel(cctx.Context)
	defer ctxCloser()

	res := &aggregateResult{
		standaloneEntries: make([]dagaggregator.AggregateDagEntry, len(toAgg)),
	}
	var projectedSize int64
	initialRoots := make([]string, len(toAgg))
	for i := range toAgg {
		projectedSize += int64(toAgg[i].UniqueBlockCumulativeSize + toAgg[i].UniqueBlockCount*estimatedSingleBlockCarOverhead)
		res.standaloneEntries[i] = toAgg[i]
		initialRoots[i] = toAgg[i].RootCid.String()
	}

	// Add all the "free" parts that happen to be included via larger dags
	log.Infof("determining included-dags for aggregate formed from %d initial roots", len(initialRoots))
	err := cargoDb.BeginTxFunc(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(rotx pgx.Tx) error {

		_, err := rotx.Exec(ctx, fmt.Sprintf(`SET LOCAL statement_timeout = %d`, (2*time.Hour).Milliseconds()))
		if err != nil {
			return err
		}

		rows, err := rotx.Query(
			ctx,
			`
			SELECT
					d.cid_v1,
					d.size_actual,
					( SELECT 1+COUNT(*) FROM cargo.refs sr WHERE sr.cid_v1 = d.cid_v1 ) AS node_count
				FROM cargo.refs r, cargo.dags d
			WHERE
				r.cid_v1 = ANY( $1::TEXT[] )
					AND
				r.ref_cid = d.cid_v1
					AND
				d.size_actual > 0
			`,
			initialRoots,
		)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var extraAgg dagaggregator.AggregateDagEntry
			var cidStr string
			if err = rows.Scan(&cidStr, &extraAgg.UniqueBlockCumulativeSize, &extraAgg.UniqueBlockCount); err != nil {
				return err
			}
			extraAgg.RootCid, err = cid.Parse(cidStr)
			if err != nil {
				return err
			}
			toAgg = append(toAgg, extraAgg)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		rows.Close()

		return nil
	})
	if err != nil {
		return nil, err
	}

	ramBs := new(rambs.RamBs)
	ramDs := merkledag.NewDAGService(blockservice.New(ramBs, exchangeoffline.Exchange(ramBs)))

	res.carRoot, res.manifestEntries, err = dagaggregator.Aggregate(ctx, ramDs, toAgg)
	if err != nil {
		return nil, err
	}
	aggLabel := fmt.Sprintf("aggregate %s dagcount [%d]%d",
		res.carRoot,
		len(initialRoots),
		len(res.manifestEntries),
	)

	//
	var countBlocks, countBytes int64
	akc, _ := ramBs.AllKeysChan(ctx)
	for c := range akc {
		b, _ := ramBs.Get(c)
		countBlocks++
		countBytes += int64(len(b.RawData()))
	}
	log.Infof("%s: writing out %s intermediate blocks weighing %s bytes to ipfs daemon", aggLabel, humanize.Comma(countBlocks), humanize.Comma(countBytes))
	if err = writeoutBlocks(cctx, ramBs); err != nil {
		return nil, err
	}

	//
	projectedSize += countBytes
	log.Infof("%s: writing out projected %s bytes as car export, calculating commP and other hashes",
		aggLabel,
		humanize.Comma(projectedSize),
	)

	carTmpFile, realFile, err := tmpfile.TempFile(outDir)
	if err != nil {
		return nil, err
	}
	defer carTmpFile.Close() //nolint:errcheck
	if realFile {
		os.Remove(carTmpFile.Name()) //nolint:errcheck
		return nil, xerrors.New("TempFile() did not create an anonymous temp file as expected")
	}

	workerCount := 3

	doneCh := make(chan struct{}, workerCount) // this effectively emulates a sync.WaitGroup
	errCh := make(chan error, 1+1+2)           // exporter has defers

	api := ipfsAPI(cctx)
	api.SetTimeout(16 * time.Hour) // yes, obscene, but plausible: bafybeifg2u5gedbeo2fio24fpy7sozsxyppdo4h2tvdvvax2p3nxmb6hpu took 14h :cryingbear:

	var toUnpinOnError string
	if !cctx.Bool("skip-pinning") {
		toUnpinOnError = res.carRoot.String()
		defer func() {
			if toUnpinOnError != "" {
				msg := fmt.Sprintf("unpinning %s after unsuccessful export", toUnpinOnError)
				err := api.Request("pin/rm").Arguments(toUnpinOnError).Option("offline", true).Exec(context.Background(), nil) // non-interruptable context
				if err != nil {
					msg += " failed: " + err.Error()
				}
				log.Warn(msg)
			}
		}()
	}

	//
	// async ref-walker ( this speeds up things considerably )
	// we do not use the results in any way, this just ensures we are pulling things with fanout as fast as we can
	go func() {
		defer func() { doneCh <- struct{}{} }()

		resp, err := api.Request("refs").Arguments(res.carRoot.String()).Option("unique", "true").Option("recursive", "true").Send(ctx)
		if err != nil {
			errCh <- err
		} else {
			defer resp.Output.Close() //nolint:errcheck
			_, err = io.Copy(io.Discard, resp.Output)
			if err != nil {
				errCh <- err
			}
		}
	}()

	//
	// async pinner, must start it either way to populate doneCh
	go func() {
		defer func() { doneCh <- struct{}{} }()

		if cctx.Bool("skip-pinning") {
			return
		}

		err := api.Request("pin/add").Arguments(res.carRoot.String()).Exec(ctx, nil)
		if err != nil {
			errCh <- err
		}
	}()

	//
	// async exporter ( concurent with above, traverses in same order )
	go func() {

		var apiresp *ipfsapi.Response
		defer func() {
			if apiresp != nil {
				if err := apiresp.Close(); err != nil {
					errCh <- err
				}
			}
			doneCh <- struct{}{}
		}()

		// wrap function to make returns easier
		err := func() error {
			var err error

			apiresp, err = api.Request("dag/export").Arguments(res.carRoot.String()).Send(ctx)
			if err != nil {
				return err
			}

			cp := new(commp.Calc)
			sha := sha256simd.New()
			md5 := md5.New()
			sz, err := io.CopyBuffer(
				io.MultiWriter(carTmpFile, cp, sha, md5),
				apiresp.Output,
				make([]byte, 32<<20),
			)
			if err != nil {
				return err
			}
			res.carSize = uint64(sz)

			res.carSha256 = sha.Sum(make([]byte, 0, 32))
			res.carMd5 = md5.Sum(make([]byte, 0, 20))

			rawCommp, paddedSize, err := cp.Digest()
			if err != nil {
				return err
			}
			if paddedSize > 32<<30 {
				return xerrors.Errorf("unexpectedly produced an oversized car file of %s bytes", humanize.Comma(int64(res.carSize)))
			}
			res.carCommp, err = commcid.DataCommitmentV1ToCID(rawCommp)
			if err != nil {
				return err
			}
			res.carPieceSize = filabi.PaddedPieceSize(paddedSize)

			return nil
		}()

		if err != nil {
			errCh <- err
		}
	}()

	var workerError error
watchdog:
	for {
		select {

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

	for workerCount > 0 {
		<-doneCh
		workerCount--
	}
	close(errCh) // no writers remain

	if workerError != nil {
		return nil, workerError
	}
	if err := <-errCh; err != nil {
		return nil, err
	}
	if err := cctx.Context.Err(); err != nil {
		return nil, err
	}

	// if it is too small - don't save it
	if res.carSize < targetMinSizeHard {
		log.Warnf("%s: UNDERSIZED car is only %s bytes (%.2f%% of projected), under a minimum of %s",
			aggLabel,
			humanize.Comma(int64(res.carSize)),
			float64(100*res.carSize)/float64(projectedSize),
			humanize.Comma(int64(targetMinSizeHard)),
		)
		return res, nil
	}

	//
	// whoa - everything worked!!!
	log.Infof("%s: persisting records in database", aggLabel)

	type aggregateMetadata struct {
		dagaggregator.ManifestPreamble
		Timeboxed bool   `json:"timeboxed,omitempty"`
		Sha256sum string `json:"sha256hex"`
		Md5sum    string `json:"md5hex"`
	}

	aggMeta, err := json.Marshal(aggregateMetadata{
		ManifestPreamble: dagaggregator.ManifestPreamble{
			RecordType: dagaggregator.RecordType(aggregateType),
			Version:    dagaggregator.CurrentManifestPreamble.Version,
		},
		Timeboxed: isTimeboxed,
		Sha256sum: fmt.Sprintf("%x", res.carSha256),
		Md5sum:    fmt.Sprintf("%x", res.carMd5),
	})
	if err != nil {
		return nil, err
	}
	root := res.carRoot.String()

	tx, err := cargoDb.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err == nil {
			err = ctx.Err()
		}
		if err != nil && tx != nil {
			tx.Rollback(context.Background()) //nolint:errcheck
		}
	}()

	_, err = tx.Exec(ctx, fmt.Sprintf(`SET LOCAL statement_timeout = %d`, (2*time.Hour).Milliseconds()))
	if err != nil {
		return nil, err
	}

	if _, err = tx.Exec(
		ctx,
		`
		INSERT INTO cargo.aggregates ( "aggregate_cid", "piece_cid", "export_size", "metadata" )
			VALUES ( $1, $2, $3, $4 )
		ON CONFLICT DO NOTHING
		`,
		root,
		res.carCommp.String(),
		res.carSize,
		aggMeta,
	); err != nil {
		return nil, err
	}

	sourcesToUnpin := make(chan string, len(res.manifestEntries))
	links := make([][]interface{}, 0, len(res.manifestEntries))
	for _, e := range res.manifestEntries {
		sourcesToUnpin <- e.DagCidV1
		links = append(links, []interface{}{
			root,
			e.DagCidV1,
			fmt.Sprintf("Links/%d/Hash/Links/%d/Hash/Links/%d/Hash", e.PathIndexes[0], e.PathIndexes[1], e.PathIndexes[2]),
		})
	}
	close(sourcesToUnpin)
	if _, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"cargo", "aggregate_entries"},
		[]string{"aggregate_cid", "cid_v1", "datamodel_selector"},
		pgx.CopyFromRows(links),
	); err != nil {
		return nil, err
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}

	// disarm defers higher up
	toUnpinOnError = ""
	tx = nil

	// all done: reify file
	fn := fmt.Sprintf("%s/%x_%s.car", outDir, res.carMd5, res.carCommp.String())
	err = tmpfile.Link(carTmpFile, fn) // likelihood of failure here is nonexistent
	if err != nil {
		return nil, err
	}

	os.Chmod(fn, unixReadable) //nolint:errcheck

	log.Infof("%s: successfully recorded and reified %s bytes (%.2f%% of projected) at %s",
		aggLabel,
		humanize.Comma(int64(res.carSize)),
		float64(100*res.carSize)/float64(projectedSize),
		fn,
	)

	if cctx.Bool("unpin-sources") {
		log.Infof("unpinning %d source dags", len(sourcesToUnpin))
		workerCount := len(sourcesToUnpin)
		if cfgMax := cctx.Int("ipfs-api-max-workers"); workerCount > cfgMax {
			workerCount = cfgMax
		}
		var wg sync.WaitGroup
		for workerCount > 0 {
			workerCount--
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					cidStr, chanOpen := <-sourcesToUnpin
					if !chanOpen {
						return
					}
					// nearly everything would have been unpinned already, so no error checks
					// sadly this is somewhat slow, but oh well... better be extra proactive
					api.Request("pin/rm").Arguments(cidStr).Option("offline", true).Exec(ctx, nil) //nolint:errcheck
				}
			}()
		}
		wg.Wait()
	}

	return res, nil
}

// pulls cids from an AllKeysChan and sends them concurrently via multiple workers to an API
func writeoutBlocks(cctx *cli.Context, bs blockstore.Blockstore) error {

	ctx, shutdownWorkers := context.WithCancel(cctx.Context)
	defer shutdownWorkers()

	akc, err := bs.AllKeysChan(ctx)
	if err != nil {
		return err
	}

	maxWorkers := cctx.Int("ipfs-api-max-workers")
	finishCh := make(chan struct{}, 1)
	errCh := make(chan error, maxWorkers)

	// WaitGroup as we want everyone to fully "quit" before we return
	var wg sync.WaitGroup

	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			api := ipfsAPI(cctx)

			for {
				select {

				case <-ctx.Done():
					// something caused us to stop, whatever it is parent knows why
					return

				case c, chanOpen := <-akc:

					if !chanOpen {
						select {
						case finishCh <- struct{}{}:
						default:
							// if we can't signal feeder is done - someone else already did
						}
						return
					}

					blk, err := bs.Get(c)
					if err != nil {
						errCh <- err
						return
					}

					// copied entirety of ipfsapi.BlockPut() to be able to pass in our own ctx 🤮
					res := new(struct{ Key string })
					err = api.Request("block/put").
						Option("offline", true).
						Option("format", cid.CodecToStr[c.Prefix().Codec]).
						Option("mhtype", multihash.Codes[c.Prefix().MhType]).
						Option("mhlen", c.Prefix().MhLength).
						Body(
							ipfsfiles.NewMultiFileReader(
								ipfsfiles.NewSliceDirectory([]ipfsfiles.DirEntry{
									ipfsfiles.FileEntry(
										"",
										ipfsfiles.NewBytesFile(blk.RawData()),
									),
								}),
								true,
							),
						).
						Exec(ctx, res)
					// end of 🤮

					if err != nil {
						errCh <- err
						return
					}

					if res.Key != c.String() {
						errCh <- xerrors.Errorf("unexpected cid mismatch after /block/put: expected %s but got %s", c, res.Key)
						return
					}
				}
			}
		}()
	}

	var workerError error
watchdog:
	for {
		select {

		case <-finishCh:
			break watchdog

		case <-cctx.Context.Done():
			break watchdog

		case workerError = <-errCh:
			shutdownWorkers()
			break watchdog
		}
	}

	wg.Wait()
	close(errCh)

	if workerError != nil {
		return workerError
	}
	if err := <-errCh; err != nil {
		return err
	}
	return cctx.Context.Err()
}
