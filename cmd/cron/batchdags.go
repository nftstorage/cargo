package main

import (
	"context"
	"os"
	"sync"

	"github.com/filecoin-project/go-dagaggregator-unixfs"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	datastoresync "github.com/ipfs/go-datastore/sync"
	fslock "github.com/ipfs/go-fs-lock"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	exchangeoffline "github.com/ipfs/go-ipfs-exchange-offline"
	"github.com/ipfs/go-merkledag"
	"github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
)

const aggregateDagsName = "aggregate-dags"

var aggregateDags = &cli.Command{
	Usage: "Aggregate available dags if any",
	Name:  aggregateDagsName,
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {

		lkCLose, err := fslock.Lock(os.TempDir(), aggregateDagsName)
		if err != nil {
			return err
		}
		defer lkCLose.Close()

		log.Info("begin car aggregation round")

		ctx, closer := context.WithCancel(cctx.Context)
		defer closer()

		db, err := connectDb(cctx)
		if err != nil {
			return err
		}

		toAgg := make([]dagaggregator.AggregateDagEntry, 0, 256<<10)

		rows, err := db.Query(
			ctx,
			`
			SELECT
					s.cid_original,
					d.size_actual,
					( SELECT 1+COUNT(*) FROM cargo.refs r WHERE r.cid_v1 = d.cid_v1 )
				FROM cargo.sources s, cargo.dags d
			WHERE
				s.entry_removed IS NULL
					AND
				d.cid_v1 = s.cid_v1
					AND
				d.size_actual IS NOT NULL
					AND
				d.size_actual < 34000000000
					AND
				NOT EXISTS (
					SELECT 42
						FROM cargo.batch_entries be, cargo.batches b
					WHERE
							be.cid_v1 = s.cid_v1
								AND
							be.batch_cid = b.batch_cid
								AND
							b.metadata->>'type' = 'DagAggregate UnixFS'
				)
			ORDER BY size_actual DESC
			`,
		)
		if err != nil {
			return err
		}

		for rows.Next() {
			var dag dagaggregator.AggregateDagEntry
			var cidStr string

			if err = rows.Scan(&cidStr, &dag.UniqueBlockCumulativeSize, &dag.UniqueBlockCount); err != nil {
				return err
			}
			dag.RootCid, err = cid.Parse(cidStr)
			if err != nil {
				return err
			}

			toAgg = append(toAgg, dag)
		}

		log.Infof("%d candidate dags found", len(toAgg))

		// run through them backwards, heaviest first
		log.Panicw("selection implementation pending:", "first dag", toAgg[0].RootCid.String(), "size", *toAgg[0].UniqueBlockCumulativeSize)

		ramBs := blockstore.NewBlockstore(datastoresync.MutexWrap(datastore.NewMapDatastore()))
		ramDs := merkledag.NewDAGService(blockservice.New(ramBs, exchangeoffline.Exchange(ramBs)))

		rootCid, err := dagaggregator.Aggregate(ramDs, toAgg)
		if err != nil {
			return nil
		}

		newBlocksCh, err := ramBs.AllKeysChan(ctx)
		if err != nil {
			return err
		}

		var wg sync.WaitGroup
		maxWorkers := cctx.Int("ipfs-api-max-workers")
		errCh := make(chan error, maxWorkers)

		for maxWorkers > 0 {
			maxWorkers--
			wg.Add(1)
			go func() {
				defer wg.Done()
				api := ipfsApi(cctx)

				for {
					c, isOpen := <-newBlocksCh
					if !isOpen {
						return
					}

					blk, err := ramBs.Get(c)
					if err != nil {
						errCh <- err
						return
					}

					_, err = api.BlockPut(
						blk.RawData(),
						cid.CodecToStr[c.Prefix().Codec],
						multihash.Codes[c.Prefix().MhType],
						c.Prefix().MhLength,
					)
					if err != nil {
						errCh <- err
						return
					}
				}
			}()
		}

		wg.Wait()
		if len(errCh) > 0 {
			return <-errCh
		}

		log.Infof("%s", rootCid)

		return nil
	},
}
