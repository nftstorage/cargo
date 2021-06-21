package main

import (
	"context"
	"encoding/json"
	"os"

	"github.com/filecoin-project/lotus/chain/actors/builtin/market"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
)

type dealList map[uint64]struct {
	Proposal market.DealProposal
	State    market.DealState
}

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

		// FIXME
		// curl -s http://127.0.0.1:1234/rpc/v0 -X POST -H "Content-Type: application/json" --data '{ "jsonrpc": "2.0", "id":1, "method": "Filecoin.StateMarketDeals", "params": [ null ] }' | less
		inputFh, err := os.Open("deals.json")
		if err != nil {
			return err
		}
		var deals dealList
		if err = json.NewDecoder(inputFh).Decode(&deals); err != nil {
			return err
		}

		for dealId, d := range deals {
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
				dealId,
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
