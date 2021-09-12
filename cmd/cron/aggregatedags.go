package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
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
	"golang.org/x/net/context/ctxhttp"
	"golang.org/x/xerrors"
)

const (
	defaultAggregateSettleDelayHours = 1
	targetMaxSize                    = uint64(34_000_000_000) // bytes of payload *before* .car overhead
	aggregateType                    = "DagAggregate UnixFS"

	unixReadable = os.FileMode(0644)
)

var targetMinSizeSoft, targetMinSizeHard uint64
var concurrentExports, settleDelayHours, forceAgeHours uint
var captureAggregateCandidatesSnapshot bool
var carExportDir string

type pendingDag struct {
	aggentry    dagaggregator.AggregateDagEntry
	srcid       int64
	sourceStamp time.Time
}

type aggregateResult struct {
	standaloneEntries []dagaggregator.AggregateDagEntry
	manifestEntries   []*dagaggregator.ManifestDagEntry
	carSize           uint64
	carPieceSize      filabi.PaddedPieceSize
	carRoot           cid.Cid
	carCommp          cid.Cid
	carSha256         []byte
}

type runningTotals struct {
	newAggregatesTotal       *uint64
	dagsAggregatedStandalone *uint64
	dagsAggregatedTotal      *uint64
}

type bidBotRequest struct {
	AggregateCid      string           `json:"payloadCid"`
	PieceCid          string           `json:"pieceCid"`
	PaddedPieceSize   int64            `json:"pieceSize"`
	ReplicationFactor uint             `json:"repFactor"`
	DealStartTime     time.Time        `json:"deadline"`
	DataSource        bidBotDatasource `json:"carURL"`
}
type bidBotDatasource struct {
	URL string `json:"url"`
}
type bidBotResponse struct {
	ID           string
	AggregateCid cid.Cid `json:"cid"`
	StatusCode   string  `json:"status_code"`
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

		masterListSQL := eligibleForAggregationSQL(targetMaxSize, settleDelayHours)
		// fmt.Println(masterListSQL)

		var rows pgx.Rows
		if !captureAggregateCandidatesSnapshot {
			rows, err = db.Query(ctx, masterListSQL)
		} else {
			mvName := `cargo.debug_aggregate_candidates_snapshot__` + time.Now().Format("2006_01_02__15_04_05")
			_, err = db.Exec(ctx, fmt.Sprintf("CREATE MATERIALIZED VIEW %s AS\n%s", mvName, masterListSQL))
			if err != nil {
				return err
			}
			rows, err = db.Query(ctx, `SELECT * FROM `+mvName)
		}
		if err != nil {
			return err
		}

		statsDags := make(map[string]struct{})
		statsSources := make(map[int64]struct{})
		toAggRemaining := make([]pendingDag, 0, 256<<10)

		var initialBytes uint64
		var forceTimeboxedAggregation bool
		forceCutoff := time.Now().Add(-1 * time.Hour * time.Duration(forceAgeHours))
		for rows.Next() {
			var pending pendingDag
			var cidStr string

			if err = rows.Scan(nil, &pending.srcid, &cidStr, &pending.aggentry.UniqueBlockCumulativeSize, &pending.aggentry.UniqueBlockCount, &pending.sourceStamp); err != nil {
				return err
			}

			pending.aggentry.RootCid, err = cid.Parse(cidStr)
			if err != nil {
				return err
			}

			if forceAgeHours > 0 {
				forceTimeboxedAggregation = forceTimeboxedAggregation || (pending.sourceStamp.Before(forceCutoff))
			}

			statsSources[pending.srcid] = struct{}{}
			if _, existing := statsDags[pending.aggentry.RootCid.String()]; existing {
				continue // register first occurrence of CID only, note: we record all sources on line above
			}

			statsDags[pending.aggentry.RootCid.String()] = struct{}{}
			toAggRemaining = append(toAggRemaining, pending)
			initialBytes += pending.aggentry.UniqueBlockCumulativeSize
		}
		if err := rows.Err(); err != nil {
			return err
		}

		stats := runningTotals{
			newAggregatesTotal:       new(uint64),
			dagsAggregatedStandalone: new(uint64),
			dagsAggregatedTotal:      new(uint64),
		}
		var lastRoundAgg []dagaggregator.AggregateDagEntry
		defer func() {
			log.Infow("summary",
				"initialCandidates", len(statsDags),
				"uniqueCandidateSources", len(statsSources),
				"aggregatesAssembled", *stats.newAggregatesTotal,
				"dagsAggregatedStandalone", *stats.dagsAggregatedStandalone,
				"dagsAggregatedTotal", *stats.dagsAggregatedTotal,
			)
		}()

		log.Infof("%s standalone aggregation candidates found, projected to weigh %s bytes",
			humanize.Comma(int64(len(toAggRemaining))),
			humanize.Comma(int64(initialBytes)),
		)
		if len(toAggRemaining) == 0 {
			return nil
		}

		// first aggregation pass
		// loop until we arrive at definitive lack of data
		aggBundles := make([][]dagaggregator.AggregateDagEntry, 0, 128)
		for {
			var runBytes uint64
			curRoundSources := make(map[int64]struct{})

			// reset
			lastRoundAgg = make([]dagaggregator.AggregateDagEntry, 0, len(toAggRemaining))

			// run forward through the ordered list, until we overflow
			for len(toAggRemaining) > 0 && runBytes+toAggRemaining[0].aggentry.UniqueBlockCumulativeSize <= targetMaxSize {
				d := toAggRemaining[0]
				runBytes += d.aggentry.UniqueBlockCumulativeSize
				curRoundSources[d.srcid] = struct{}{}
				lastRoundAgg = append(lastRoundAgg, d.aggentry)
				toAggRemaining = toAggRemaining[1:]
			}

			// common code to reuse twice below
			runBackwardsThroughRemaining := func(extraSkipFunc func(i int) bool) {
				for i := len(toAggRemaining) - 1; runBytes < targetMinSizeSoft && i >= 0; i-- {
					d := toAggRemaining[i]

					if runBytes+d.aggentry.UniqueBlockCumulativeSize > targetMaxSize || extraSkipFunc(i) {
						continue
					}

					runBytes += d.aggentry.UniqueBlockCumulativeSize
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

			// we can't find enough to make it worthwhile: close shop until next time
			if runBytes < targetMinSizeHard ||
				(!forceTimeboxedAggregation && runBytes < targetMinSizeSoft) {
				break
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
			// ( a bit icky since we do not know the correc size yet, but meh... )
			var pseudoCarSize uint64
			for i := range lastRoundAgg {
				pseudoCarSize += lastRoundAgg[i].UniqueBlockCumulativeSize
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
			log.Infof("attempting to recombine %s standalone dags from %s undersized cars/groups with lengths: %s",
				humanize.Comma(standaloneCount),
				humanize.Comma(int64(len(undersizedInvalidCars))),
				strings.Join(sizeStrings, "  "),
			)

			shardSizes := make([][]string, 0)
			aggBundles = make([][]dagaggregator.AggregateDagEntry, 0)
			for {
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

		log.Info("forcing time-boxed rehydration from preexisting already-packaged standalone dags")

		rows, err = db.Query(
			ctx,
			`
			WITH dag_candidates AS (
				SELECT
						ae.cid_v1,
						d.size_actual,
						COUNT(*) AS replica_count
					FROM cargo.aggregate_entries ae
					JOIN cargo.dags d
						ON ae.cid_v1 = d.cid_v1
					LEFT JOIN cargo.refs r
						ON ae.cid_v1 = r.ref_cid
					LEFT JOIN cargo.deals de -- this inflates the replica_count, conflating 0 with 1 ( always 1 ), which is ok
						ON de.aggregate_cid = ae.aggregate_cid AND de.status != 'terminated'
				WHERE
					-- not part of anything else
					r.ref_cid IS NULL
						AND
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
				ORDER BY replica_count
				LIMIT $2
			)
			SELECT
					d.cid_v1,
					d.size_actual,
					( SELECT 1+COUNT(*) FROM cargo.refs sr WHERE sr.cid_v1 = d.cid_v1 ) AS node_count
				FROM dag_candidates d
			ORDER BY RANDOM()
			`,
			targetMinSizeHard,
			1_000_000,
		)
		if err != nil {
			return err
		}

		// run through *everything* attempting to pack things as tightly as possible
		// ( up to 1 mil records )
		runBytes := undersizedInvalidCars[0].carSize
		finalDitchAgg := undersizedInvalidCars[0].standaloneEntries
		for rows.Next() {
			var ae dagaggregator.AggregateDagEntry
			var cidStr string
			if err = rows.Scan(&cidStr, &ae.UniqueBlockCumulativeSize, &ae.UniqueBlockCount); err != nil {
				return err
			}

			// will overflow, nope
			if runBytes+ae.UniqueBlockCumulativeSize > targetMaxSize {
				continue
			}

			ae.RootCid, err = cid.Parse(cidStr)
			if err != nil {
				return err
			}

			// good, let's try it!
			finalDitchAgg = append(finalDitchAgg, ae)
			runBytes += ae.UniqueBlockCumulativeSize
		}
		if err := rows.Err(); err != nil {
			return err
		}

		// if it works - it works
		_, err = reifyAggregateCars(cctx, stats, true, [][]dagaggregator.AggregateDagEntry{finalDitchAgg})
		return err
	},
}

func eligibleForAggregationSQL(targetMaxSize uint64, settleDelayHours uint) string {
	return fmt.Sprintf(
		`
		WITH active_sources AS (
			SELECT
					s.project,
					ds.srcid,
					MIN(ds.entry_created) AS oldest_unaggregated,
					COALESCE( s.weight, 100 ) AS weight
				FROM cargo.dag_sources ds
				JOIN cargo.sources s USING ( srcid )
				JOIN cargo.dags d USING ( cid_v1 )
				LEFT JOIN cargo.aggregate_entries ae USING ( cid_v1 )
			WHERE
				( d.size_actual IS NOT NULL AND d.size_actual <= %[1]d ) -- only analysed entries (FIXME for now do not deal with oversizes/that comes later)
					AND
				( s.weight >= 0 OR s.weight IS NULL )
					AND
				ds.entry_removed IS NULL
					AND
				ae.cid_v1 IS NULL
			GROUP BY s.project, ds.srcid, weight
		)
		SELECT
				s.project,
				s.srcid,
				d.cid_v1,
				d.size_actual,
				( SELECT 1+COUNT(*) FROM cargo.refs r WHERE r.cid_v1 = d.cid_v1 ) AS node_count,
				ds.entry_last_updated
			FROM cargo.dag_sources ds
			JOIN cargo.dags d USING ( cid_v1 )
			JOIN active_sources s USING ( srcid )
			-- not yet aggregated anti-join (IS NULL below)
			LEFT JOIN cargo.aggregate_entries ae USING ( cid_v1 )
		WHERE
			ds.entry_removed IS NULL
				AND
			-- only analysed entries (FIXME for now do not deal with oversizes/that comes later)
			( d.size_actual IS NOT NULL AND d.size_actual <= %[1]d )
				AND
			-- not yet aggregated
			ae.cid_v1 IS NULL
				AND
			-- exclude members of something else *that is subject to aggregation*
			NOT EXISTS (
				SELECT 42
					FROM cargo.refs r
					JOIN cargo.dag_sources rds USING ( cid_v1 )
					JOIN cargo.dags rd USING ( cid_v1 )
					JOIN active_sources asrc USING ( srcid )
					LEFT JOIN cargo.aggregate_entries rae USING ( cid_v1 )
				WHERE
					r.ref_cid = d.cid_v1
						AND
					rds.entry_removed IS NULL
						AND
					rd.size_actual <= %[1]d
						AND
					rae.aggregate_cid IS NULL
			)
				AND
			-- give enough time for metadata/containing dags to trickle in too, allowing for outages
			(
				LEAST( ds.entry_created, d.entry_created ) <= ( NOW() - '%[2]s'::INTERVAL )
					OR
				EXISTS (
					SELECT 42
						FROM cargo.refs sr
						JOIN cargo.dags sd
							ON sr.ref_cid = sd.cid_v1
						JOIN cargo.dag_sources sds
							ON sr.ref_cid = sds.cid_v1 AND ds.srcid = sds.srcid
					WHERE
						ds.cid_v1 = sr.cid_v1
							AND
						LEAST( sds.entry_created, sd.entry_created ) <= ( NOW() - '%[2]s'::INTERVAL )
				)
			)
		ORDER BY s.weight DESC, s.oldest_unaggregated, s.srcid, d.size_actual DESC, d.cid_v1
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
		projectedSize += int64(toAgg[i].UniqueBlockCumulativeSize)
		res.standaloneEntries[i] = toAgg[i]
		initialRoots[i] = toAgg[i].RootCid.String()
	}

	// Add all the "free" parts that happen to be included via larger dags
	rows, err := db.Query(
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
		return nil, err
	}
	for rows.Next() {
		var extraAgg dagaggregator.AggregateDagEntry
		var cidStr string
		if err = rows.Scan(&cidStr, &extraAgg.UniqueBlockCumulativeSize, &extraAgg.UniqueBlockCount); err != nil {
			return nil, err
		}
		extraAgg.RootCid, err = cid.Parse(cidStr)
		if err != nil {
			return nil, err
		}
		toAgg = append(toAgg, extraAgg)
	}
	if err := rows.Err(); err != nil {
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
	log.Infof("%s: writing out projected %s bytes as car export, calculating commP and sha256",
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
	api.SetTimeout(2*time.Hour - 5*time.Minute) // yes, obscene, but plausible

	var toUnpinOnError string
	if !cctx.Bool("skip-pinning") {
		toUnpinOnError = res.carRoot.String()
		defer func() {
			if toUnpinOnError != "" {
				msg := fmt.Sprintf("unpinning %s after unsuccessful export", toUnpinOnError)
				err := api.Request("pin/rm").Arguments(toUnpinOnError).Exec(context.Background(), nil) // non-interruptable context
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
			sz, err := io.CopyBuffer(
				io.MultiWriter(carTmpFile, cp, sha),
				apiresp.Output,
				make([]byte, 32<<20),
			)
			if err != nil {
				return err
			}
			res.carSize = uint64(sz)

			res.carSha256 = sha.Sum(make([]byte, 0, 32))

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

	// wg.Wait()
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
		Timeboxed bool `json:",omitempty"`
	}

	aggMeta, err := json.Marshal(aggregateMetadata{
		ManifestPreamble: dagaggregator.ManifestPreamble{
			RecordType: dagaggregator.RecordType(aggregateType),
			Version:    dagaggregator.CurrentManifestPreamble.Version,
		},
		Timeboxed: isTimeboxed,
	})
	if err != nil {
		return nil, err
	}
	root := res.carRoot.String()

	tx, err := db.Begin(ctx)
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

	if _, err = tx.Exec(
		ctx,
		`
		INSERT INTO cargo.aggregates ( "aggregate_cid", "piece_cid", "sha256hex", "export_size", "metadata" )
			VALUES ( $1, $2, $3, $4, $5 )
		ON CONFLICT DO NOTHING
		`,
		root,
		res.carCommp.String(),
		fmt.Sprintf("%x", res.carSha256),
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
	fn := fmt.Sprintf("%s/%s_%s.car", outDir, root, res.carCommp.String())
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
					api.Request("pin/rm").Arguments(cidStr).Exec(ctx, nil) //nolint:errcheck
				}
			}()
		}
		wg.Wait()
	}

	// Aggregate ready to go, fire off a bidbot request
	// The request is opportunistic, only log its errors, do not fail
	if err := notifyBidBot(cctx, bidBotRequest{
		AggregateCid:    root,
		PieceCid:        res.carCommp.String(),
		PaddedPieceSize: int64(res.carPieceSize),
	}); err != nil {
		log.Errorf("failed to register aggregate with BidBot, proceeding nevertheless: %s", err)
	}

	return res, nil
}

var datasourceTemplate *template.Template

func notifyBidBot(cctx *cli.Context, reqData bidBotRequest) error {

	//
	// FIXME - bidbot is not concurrency-friendly at present, hold a pg-side lock
	tx, err := db.Begin(cctx.Context)
	if err != nil {
		return xerrors.Errorf("unable to open workaround-lock-transaction: %w", err)
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck
	if _, err = tx.Exec(cctx.Context, `SELECT PG_ADVISORY_XACT_LOCK( 123456 )`); err != nil {
		return xerrors.Errorf("unable to advisory-lock: %w", err)
	}
	// END FIXME
	//

	if datasourceTemplate == nil {
		datasourceTemplate, err = template.New("").Parse(cctx.String("aggregate-location-template"))
		if err != nil {
			return xerrors.Errorf("unable to parse template '%s': %w", cctx.String("aggregate-location-template"), err)
		}
	}
	datasourceURL := new(bytes.Buffer)
	if err = datasourceTemplate.Execute(datasourceURL, reqData); err != nil {
		return err
	}

	reqData.ReplicationFactor = cctx.Uint("bidbot-replication-factor")
	reqData.DealStartTime = time.Now().Add(time.Hour * time.Duration(cctx.Uint("bidbot-deadline-hours")))
	reqData.DataSource = bidBotDatasource{URL: datasourceURL.String()}
	reqJSON, err := json.Marshal(reqData)
	if err != nil {
		return err
	}

	resp, err := ctxhttp.Post(
		cctx.Context,
		retryingClient(cctx.String("bidbot-token")),
		cctx.String("bidbot-api"),
		"application/json",
		bytes.NewReader(reqJSON),
	)
	if err != nil {
		return err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return xerrors.Errorf("request to BidBot failed with code %d\n%s", resp.StatusCode, string(body))
	}

	var respData bidBotResponse
	if err = json.Unmarshal(body, &respData); err != nil {
		return xerrors.Errorf("unable to parse BidBot response: %s\n%s", err, string(body))
	}

	log.Infow("bidbot registration successful",
		"bidbotID", respData.ID,
		"bidbotStatus", respData.StatusCode,
		"aggregateCid", respData.AggregateCid.String(),
		"repFactor", reqData.ReplicationFactor,
		"deadline", reqData.DealStartTime,
	)

	return nil
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
