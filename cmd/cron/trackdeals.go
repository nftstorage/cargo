package main

import (
	"context"
	"strconv"
	"time"

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
		rows, err := db.Query(
			ctx,
			`
			SELECT a.aggregate_cid, a.piece_cid, d.deal_id
				FROM cargo.aggregates a
				LEFT JOIN cargo.deals d
					ON a.aggregate_cid = d.aggregate_cid AND d.status != 'terminated'
			`,
		)
		if err != nil {
			return err
		}
		for rows.Next() {
			var aCidStr string
			var pCidStr string
			var dealID *int64

			if err = rows.Scan(&aCidStr, &pCidStr, &dealID); err != nil {
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
				}
			}

			aggCidLookup[pCid] = aCid
		}
		if err := rows.Err(); err != nil {
			return err
		}

		var currentDealCount, newDealCount int
		defer func() {
			log.Infow("summary", "knownPieces", len(aggCidLookup), "currentKnownDeals", currentDealCount, "newDeals", newDealCount, "terminatedDeals", len(knownDeals))
		}()

		if len(aggCidLookup) == 0 {
			return nil
		}

		log.Infof("checking the status of %d active deals and %d known Piece CIDs", len(knownDeals), len(aggCidLookup))

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
		log.Infof("retrieved %d active deal records", len(deals))

		for dealIDString, d := range deals {
			aggCid, known := aggCidLookup[d.Proposal.PieceCID]
			if !known {
				continue
			}

			dealID, err := strconv.ParseInt(dealIDString, 10, 64)
			if err != nil {
				return err
			}

			currentDealCount++
			if _, known := knownDeals[dealID]; !known {
				newDealCount++
			} else {
				// whatevr remains is not in SMA list, thus "terminated"
				delete(knownDeals, dealID)
			}

			_, err = db.Exec(
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

				_, err = db.Exec(
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

			status := "published"
			if d.State.SectorStartEpoch > 0 {
				status = "active"
			} else if d.Proposal.StartEpoch+filprovider.WPoStChallengeWindow < lts.Height() {
				// if things are lookback+one deadlines late: they are never going to make it
				status = "terminated"
			}

			_, err = db.Exec(
				ctx,
				`
				INSERT INTO cargo.deals ( aggregate_cid, client, provider, status, deal_id, epoch_start, epoch_end )
					VALUES ( $1, $2, $3, $4, $5, $6, $7 )
				ON CONFLICT ( deal_id ) DO UPDATE SET
					status = EXCLUDED.status
				`,
				aggCid.String(),
				clientLookup[d.Proposal.Client].robust.String(),
				d.Proposal.Provider.String(),
				status,
				dealID,
				d.Proposal.StartEpoch,
				d.Proposal.EndEpoch,
			)
			if err != nil {
				return err
			}
		}

		// we have some terminations ( no longer in the market state )
		if len(knownDeals) > 0 {
			toFail := make([]int64, 0, len(knownDeals))
			for dID := range knownDeals {
				toFail = append(toFail, dID)
			}

			_, err = db.Exec(
				ctx,
				`
				UPDATE cargo.deals SET status = $1 WHERE deal_id = ANY ( $2::BIGINT[] )
				`,
				`terminated`,
				toFail,
			)
			if err != nil {
				return err
			}
		}

		return nil
	},
}
