package main

import (
	"context"
	"sync"

	"github.com/filecoin-project/go-dagaggregator-unixfs"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	exchangeoffline "github.com/ipfs/go-ipfs-exchange-offline"
	"github.com/ipfs/go-merkledag"
	"github.com/multiformats/go-multihash"
	"github.com/nftstorage/dagcargo/lib/rambs"
	"github.com/urfave/cli/v2"
)

var aggregateDags = &cli.Command{
	Usage: "Aggregate available dags if any",
	Name:  "aggregate-dags",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {

		ctx, closer := context.WithCancel(cctx.Context)
		defer closer()

		toAgg := make([]dagaggregator.AggregateDagEntry, 0, 256<<10)

		db, err := connectDb(cctx)
		if err != nil {
			return err
		}

		rows, err := db.Query(
			ctx,
			`
			SELECT
					d.cid_v1,
					d.size_actual,
					( SELECT 1+COUNT(*) FROM cargo.refs r WHERE r.cid_v1 = d.cid_v1 )
				FROM cargo.dags d
			WHERE
				EXISTS ( SELECT 42 FROM cargo.sources s WHERE d.cid_v1 = s.cid_v1 AND s.entry_removed IS NULL )
					AND
				d.size_actual IS NOT NULL
					AND
				d.size_actual < 34000000000
					AND
				NOT EXISTS (
					SELECT 42
						FROM cargo.batch_entries be, cargo.batches b
					WHERE
						be.cid_v1 = d.cid_v1
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
		log.Panicw("selection implementation pending:", "first dag", toAgg[0].RootCid.String(), "size", toAgg[0].UniqueBlockCumulativeSize)

		ramBs := new(rambs.RamBs)
		ramDs := merkledag.NewDAGService(blockservice.New(ramBs, exchangeoffline.Exchange(ramBs)))

		rootCid, err := dagaggregator.Aggregate(ctx, ramDs, toAgg)
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
				api := ipfsAPI(cctx)

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
