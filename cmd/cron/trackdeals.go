package main

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var pieceCount, dealCount int
var trackDeals = &cli.Command{
	Usage: "Track state of filecoin deals related to known PieceCIDs",
	Name:  "track-deals",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {

		ctx, closer := context.WithCancel(cctx.Context)
		defer closer()

		db, err := connectDb(cctx)
		if err != nil {
			return err
		}
		knownPieceCIDs := make(map[cid.Cid]cid.Cid)
		rows, err := db.Query(
			ctx,
			`SELECT piece_cid, batch_cid FROM cargo.batches WHERE piece_cid IS NOT NULL`,
		)
		if err != nil {
			return err
		}
		for rows.Next() {
			var pCidStr string
			var bCidStr string

			if err = rows.Scan(&pCidStr, &bCidStr); err != nil {
				return err
			}
			pCid, err := cid.Parse(pCidStr)
			if err != nil {
				return err
			}
			bCid, err := cid.Parse(bCidStr)
			if err != nil {
				return err
			}
			knownPieceCIDs[pCid] = bCid
		}

		pieceCount = len(knownPieceCIDs)
		defer func() {
			log.Infow("summary", "knownPieces", pieceCount, "totalDeals", dealCount)
		}()

		if pieceCount == 0 {
			return nil
		}

		log.Infof("checking the status of %d known Piece CIDs", pieceCount)

		api, apiClose, err := lotusAPI(cctx)
		if err != nil {
			return xerrors.Errorf("connecting to lotus failed: %w", err)
		}
		defer apiClose()

		lts, err := lotusLookbackTipset(cctx, api)
		if err != nil {
			return err
		}

		log.Infof("retrieving Market Deals from state %s at epoch %d", lts.Key().String(), lts.Height())
		deals, err := api.StateMarketDeals(ctx, lts.Key())
		if err != nil {
			return err
		}
		log.Infof("retrieved %d active deal records", len(deals))

		for dealID, d := range deals {
			batchCid, known := knownPieceCIDs[d.Proposal.PieceCID]
			if !known {
				continue
			}

			dealCount++

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

			epochStart := new(int64)
			epochEnd := new(int64)

			if d.Proposal.StartEpoch > 0 {
				*epochStart = int64(d.Proposal.StartEpoch)
			}
			if d.Proposal.EndEpoch > 0 {
				*epochEnd = int64(d.Proposal.EndEpoch)
			}

			status := "published"
			if d.State.SectorStartEpoch > 0 {
				status = "active"
				*epochStart = int64(d.State.SectorStartEpoch)
			}

			_, err = db.Exec(
				ctx,
				`
				INSERT INTO cargo.deals ( batch_cid, provider, status, deal_id, epoch_start, epoch_end )
					VALUES ( $1, $2, $3, $4, $5, $6 )
				ON CONFLICT ( deal_id ) DO UPDATE SET
					status = EXCLUDED.status,
					epoch_start = EXCLUDED.epoch_start,
					epoch_end = EXCLUDED.epoch_end
				`,
				batchCid.String(),
				d.Proposal.Provider.String(),
				status,
				dealID,
				epochStart,
				epochEnd,
			)
			if err != nil {
				return err
			}
		}

		return nil
	},
}
