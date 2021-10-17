package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	filaddr "github.com/filecoin-project/go-address"
	filabi "github.com/filecoin-project/go-state-types/abi"
	filprovider "github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

type filClient struct {
	robust           filaddr.Address
	dataCapRemaining *filabi.StoragePower
}
type filDeal struct {
	pieceCid     cid.Cid
	aggregateCid cid.Cid
	status       string
}

var trackDeals = &cli.Command{
	Usage: "Track state of filecoin deals related to known PieceCIDs",
	Name:  "track-deals",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {

		ctx, closer := context.WithCancel(cctx.Context)
		defer closer()

		clientLookup := make(map[filaddr.Address]filClient, 32)
		knownDeals := make(map[int64]filDeal)
		aggCidLookup := make(map[cid.Cid]cid.Cid)
		rows, err := cargoDb.Query(
			ctx,
			`
			SELECT a.aggregate_cid, a.piece_cid, d.deal_id, d.status
				FROM cargo.aggregates a
				LEFT JOIN cargo.deals d USING ( aggregate_cid )
			`,
		)
		if err != nil {
			return err
		}
		for rows.Next() {
			var aCidStr string
			var pCidStr string
			var dealID *int64
			var dealStatus *string

			if err = rows.Scan(&aCidStr, &pCidStr, &dealID, &dealStatus); err != nil {
				return err
			}
			aCid, err := cid.Parse(aCidStr)
			if err != nil {
				return err
			}
			pCid, err := cid.Parse(pCidStr)
			if err != nil {
				return err
			}

			if dealID != nil {
				knownDeals[*dealID] = filDeal{
					pieceCid:     pCid,
					aggregateCid: aCid,
					status:       *dealStatus,
				}
			}
			aggCidLookup[pCid] = aCid
		}
		if err := rows.Err(); err != nil {
			return err
		}

		var newDealCount, terminatedDealCount int
		dealTotals := make(map[string]int64)
		defer func() {
			log.Infow("summary",
				"knownPieces", len(aggCidLookup),
				"relatedDeals", dealTotals,
				"newlyAdded", newDealCount,
				"newlyTerminated", terminatedDealCount,
			)
		}()

		if len(aggCidLookup) == 0 {
			return nil
		}

		log.Infof("checking the status of %s known Piece CIDs", humanize.Comma(int64(len(aggCidLookup))))

		api, apiClose, err := lotusAPI(cctx)
		if err != nil {
			return xerrors.Errorf("connecting to lotus failed: %w", err)
		}
		defer apiClose()

		lts, err := lotusLookbackTipset(cctx, api)
		if err != nil {
			return err
		}

		log.Infow("retrieving Market Deals from", "state", lts.Key(), "epoch", lts.Height(), "wallTime", time.Unix(int64(lts.Blocks()[0].Timestamp), 0))
		deals, err := api.StateMarketDeals(ctx, lts.Key())
		if err != nil {
			return err
		}
		log.Infof("retrieved %s state deal records", humanize.Comma(int64(len(deals))))

		for dealIDString, d := range deals {
			aggCid, known := aggCidLookup[d.Proposal.PieceCID]
			if !known {
				continue
			}

			dealID, err := strconv.ParseInt(dealIDString, 10, 64)
			if err != nil {
				return err
			}

			var initialEncounter bool
			if _, known := knownDeals[dealID]; !known {
				initialEncounter = true
			} else {
				// at the end whatever remains is not in SMA list, thus will be marked "terminated"
				delete(knownDeals, dealID)
			}

			_, err = cargoDb.Exec(
				ctx,
				`
				INSERT INTO cargo.providers ( provider ) VALUES ( $1 )
					ON CONFLICT ( provider ) DO NOTHING
				`,
				d.Proposal.Provider.String(),
			)
			if err != nil {
				return err
			}

			if _, found := clientLookup[d.Proposal.Client]; !found {
				var fc filClient

				fc.robust, err = api.StateAccountKey(ctx, d.Proposal.Client, lts.Key())
				if err != nil {
					return err
				}

				fc.dataCapRemaining, err = api.StateVerifiedClientStatus(ctx, fc.robust, lts.Key())
				if err != nil {
					return err
				}
				if fc.dataCapRemaining == nil {
					z := filabi.NewStoragePower(0)
					fc.dataCapRemaining = &z
				}

				_, err = cargoDb.Exec(
					ctx,
					`
					INSERT INTO cargo.clients ( client, filp_available ) VALUES ( $1, $2 )
						ON CONFLICT ( client ) DO UPDATE SET
							filp_available = EXCLUDED.filp_available
					`,
					fc.robust.String(),
					fc.dataCapRemaining.Int64(),
				)
				if err != nil {
					return err
				}

				clientLookup[d.Proposal.Client] = fc
			}

			var statusMeta *string
			var sectorStart *filabi.ChainEpoch
			status := "published"
			if d.State.SectorStartEpoch > 0 {
				sectorStart = &d.State.SectorStartEpoch
				status = "active"
				m := fmt.Sprintf(
					"containing sector active as of %s at epoch %d",
					mainnetTime(d.State.SectorStartEpoch).Format("2006-01-02 15:04:05"),
					d.State.SectorStartEpoch,
				)
				statusMeta = &m
			} else if d.Proposal.StartEpoch+filprovider.WPoStChallengeWindow < lts.Height() {
				// if things are lookback+one deadlines late: they are never going to make it
				status = "terminated"
				m := fmt.Sprintf(
					"containing sector missed expected sealing epoch %d",
					d.Proposal.StartEpoch,
				)
				statusMeta = &m
			}

			dealTotals[status]++
			if initialEncounter {
				if status == "terminated" {
					terminatedDealCount++
				} else {
					newDealCount++
				}
			}

			_, err = cargoDb.Exec(
				ctx,
				`
				INSERT INTO cargo.deals ( aggregate_cid, client, provider, deal_id, start_epoch, end_epoch, status, status_meta, sector_start_epoch )
					VALUES ( $1, $2, $3, $4, $5, $6, $7, $8, $9 )
				ON CONFLICT ( deal_id ) DO UPDATE SET
					status = EXCLUDED.status,
					status_meta = EXCLUDED.status_meta,
					sector_start_epoch = COALESCE( EXCLUDED.sector_start_epoch, cargo.deals.sector_start_epoch )
				`,
				aggCid.String(),
				clientLookup[d.Proposal.Client].robust.String(),
				d.Proposal.Provider.String(),
				dealID,
				d.Proposal.StartEpoch,
				d.Proposal.EndEpoch,
				status,
				statusMeta,
				sectorStart,
			)
			if err != nil {
				return err
			}
		}

		// we may have some terminations ( no longer in the market state )
		toFail := make([]int64, 0, len(knownDeals))
		for dID, d := range knownDeals {
			dealTotals["terminated"]++
			if d.status == "terminated" {
				continue
			}
			terminatedDealCount++
			toFail = append(toFail, dID)
		}
		if len(toFail) > 0 {
			_, err = cargoDb.Exec(
				ctx,
				`
				UPDATE cargo.deals SET
					status = $1,
					status_meta = $2
				WHERE
					deal_id = ANY ( $3::BIGINT[] )
						AND
					status != 'terminated'
				`,
				`terminated`,
				`deal no longer part of market-actor state`,
				toFail,
			)
			if err != nil {
				return err
			}
		}

		return nil
	},
}
