package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/cloudflare/cloudflare-go"
	"github.com/davecgh/go-spew/spew"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

type statusDealEntry struct {
	Status             *string    `json:"status"`
	LastChanged        *time.Time `json:"lastChanged"`
	LastChangedUnix    *int64     `json:"lastChangedUnix"`
	BatchRootCid       *string    `json:"batchRootCid,omitempty"`
	PieceCid           *string    `json:"pieceCid,omitempty"`
	Network            *string    `json:"network,omitempty"`
	Miner              *string    `json:"miner,omitempty"`
	ChainDealID        *uint64    `json:"chainDealID,omitempty"`
	DatamodelSelector  *string    `json:"datamodelSelector,omitempty"`
	DealActivation     *time.Time `json:"dealActivation,omitempty"`
	DealActivationUnix *int64     `json:"dealActivationUnix,omitempty"`
	DealExpiration     *time.Time `json:"dealExpiration,omitempty"`
	DealExpirationUnix *int64     `json:"dealExpirationUnix,omitempty"`
}

type statusUpdate struct {
	cidv1    string // internal
	key      string
	metadata struct {
		Queued     uint64 `json:"queued"`
		Proposing  uint64 `json:"proposing"`
		Accepted   uint64 `json:"accepted"`
		Failed     uint64 `json:"failed"`
		Published  uint64 `json:"published"`
		Active     uint64 `json:"active"`
		Terminated uint64 `json:"terminated"`
	}
	value        []*statusDealEntry
	valueEncoded string
}

var lastPct, countPending, countUpdated int

var exportStatus = &cli.Command{
	Usage: "Export status of individual NFTs to external databases",
	Name:  "export-status",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {

		ctx, closer := context.WithCancel(cctx.Context)
		defer closer()

		defer func() { log.Infow("summary", "updated", countUpdated) }()
		lastPct = 101

		db, err := connectDb(cctx)
		if err != nil {
			return err
		}

		toUpdateCond := `
			d.size_actual IS NOT NULL
				AND
			d.size_actual < 34000000000
				AND
			( d.entry_last_exported IS NULL OR d.entry_last_updated > d.entry_last_exported )
		`

		err = db.QueryRow(ctx, `SELECT COUNT(*) FROM cargo.sources s JOIN cargo.dags d USING ( cid_v1 ) WHERE `+toUpdateCond).Scan(&countPending)
		if err != nil {
			return err
		}
		if countPending == 0 {
			return nil
		}

		log.Infof("updating status of approximately %d entries", countPending)

		rows, err := db.Query(
			ctx,
			fmt.Sprintf(`
				SELECT
						s.cid_original,
						s.cid_v1,
						( CASE WHEN EXISTS ( SELECT 42 FROM cargo.batch_entries be WHERE be.cid_v1 = s.cid_v1 ) THEN 0 ELSE 1 END ) AS queued,
						0 AS proposing,
						0 AS accepted,
						0 AS failed,
						( SELECT COUNT(DISTINCT(de.deal_id)) FROM cargo.batch_entries be, cargo.deals de WHERE s.cid_v1 = be.cid_v1 AND be.batch_cid = de.batch_cid AND de.status = 'published' ) AS published,
						( SELECT COUNT(DISTINCT(de.deal_id)) FROM cargo.batch_entries be, cargo.deals de WHERE s.cid_v1 = be.cid_v1 AND be.batch_cid = de.batch_cid AND de.status = 'active' ) AS active,
						0 AS terminated,
						de.status,
						d.entry_last_updated,
						be.batch_cid,
						b.piece_cid,
						de.provider,
						de.deal_id,
						be.datamodel_selector,
						de.epoch_start,
						de.epoch_end
					FROM cargo.sources s
					JOIN cargo.dags d USING ( cid_v1 )
					LEFT JOIN cargo.batch_entries be USING ( cid_v1 )
					LEFT JOIN cargo.batches b USING ( batch_cid )
					LEFT JOIN cargo.deals de USING ( batch_cid )
				WHERE %s
				ORDER BY s.cid_original -- order is critical to form bulk-update batches
				`,

				toUpdateCond,
			),
		)
		if err != nil {
			return err
		}

		approxBytes := 0
		updates := make(map[string]*statusUpdate, 10000)

		for rows.Next() {

			u := new(statusUpdate)
			du := new(statusDealEntry)
			var eStart, eEnd *int64
			if err = rows.Scan(
				&u.key,
				&u.cidv1,
				&u.metadata.Queued,
				&u.metadata.Proposing,
				&u.metadata.Accepted,
				&u.metadata.Failed,
				&u.metadata.Published,
				&u.metadata.Active,
				&u.metadata.Terminated,
				&du.Status,
				&du.LastChanged,
				&du.BatchRootCid,
				&du.PieceCid,
				&du.Miner,
				&du.ChainDealID,
				&du.DatamodelSelector,
				&eStart,
				&eEnd,
			); err != nil {
				return err
			}

			if _, exists := updates[u.key]; !exists {

				// we are changing the key: encode everything accumulated
				buf := new(bytes.Buffer)
				if err := json.NewEncoder(buf).Encode(u.value); err != nil {
					return err
				}
				u.valueEncoded = buf.String()

				approxBytes += len(u.valueEncoded)

				// see if we grew too big and need to flush
				// 10k entries / 100MiB size
				if len(updates) > 9999 || approxBytes > 95<<20 {
					if err = uploadAndMarkUpdates(cctx, db, updates); err != nil {
						return err
					}
					// reset
					approxBytes = 0
					updates = make(map[string]*statusUpdate, 10000)
				}

				updates[u.key] = u
			}
			u = updates[u.key]

			duU := du.LastChanged.Unix()
			du.LastChangedUnix = &duU

			if du.Status == nil {
				s := "queued"
				du.Status = &s
			} else {
				n := "mainnet"
				du.Network = &n

				if eStart != nil {
					t := mainnetTime(*eStart)
					du.DealActivation = &t
					tu := t.Unix()
					du.DealActivationUnix = &tu
				}
				if eEnd != nil {
					t := mainnetTime(*eEnd)
					du.DealExpiration = &t
					tu := t.Unix()
					du.DealExpirationUnix = &tu
				}
			}

			u.value = append(u.value, du)
		}

		return uploadAndMarkUpdates(cctx, db, updates)
	},
}

func uploadAndMarkUpdates(cctx *cli.Context, db *pgxpool.Pool, updates map[string]*statusUpdate) error {

	toUpd := make(cloudflare.WorkersKVBulkWriteRequest, 0, len(updates))
	updatedCids := make([]string, 0, len(updates))
	for _, u := range updates {

		if u.valueEncoded == "" {
			buf := new(bytes.Buffer)
			if err := json.NewEncoder(buf).Encode(u.value); err != nil {
				return err
			}
			u.valueEncoded = buf.String()
		}

		toUpd = append(toUpd, &cloudflare.WorkersKVPair{
			Key:      u.key,
			Metadata: u.metadata,
			Value:    u.valueEncoded,
		})

		updatedCids = append(updatedCids, u.cidv1)
	}

	dealKvID := cctx.String("cf-kvnamespace-deals")
	if dealKvID == "" {
		return xerrors.New("config `cf-kvnamespace-deals` is not set")
	}

	api, err := cfAPI(cctx)
	if err != nil {
		return err
	}

	r, err := api.WriteWorkersKVBulk(
		cctx.Context,
		dealKvID,
		toUpd,
	)
	if err != nil {
		return xerrors.Errorf("WriteWorkersKVBulk failed: %w", err)
	}
	if !r.Success {
		log.Panicf("unexpected bulk update response:n%s", spew.Sdump(r))
	}

	_, err = db.Exec(
		cctx.Context,
		`UPDATE cargo.dags SET entry_last_exported = NOW() WHERE cid_v1 = ANY ( $1 )`,
		updatedCids,
	)
	if err != nil {
		return err
	}

	countUpdated += len(updatedCids)
	if showProgress && 100*countUpdated/countPending != lastPct {
		lastPct = 100 * countUpdated / countPending
		fmt.Fprintf(os.Stderr, "%d%%\r", lastPct)
	}

	return nil
}
