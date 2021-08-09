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
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

type cfStatusDealEntry struct {
	Status             *string    `json:"status"`
	LastChanged        *time.Time `json:"lastChanged"`
	LastChangedUnix    *int64     `json:"lastChangedUnix"`
	AggregateRootCid   *string    `json:"batchRootCid,omitempty"`
	PieceCid           *string    `json:"pieceCid,omitempty"`
	Network            *string    `json:"network,omitempty"`
	Provider           *string    `json:"miner,omitempty"`
	ChainDealID        *uint64    `json:"chainDealID,omitempty"`
	DatamodelSelector  *string    `json:"datamodelSelector,omitempty"`
	DealActivation     *time.Time `json:"dealActivation,omitempty"`
	DealActivationUnix *int64     `json:"dealActivationUnix,omitempty"`
	DealExpiration     *time.Time `json:"dealExpiration,omitempty"`
	DealExpirationUnix *int64     `json:"dealExpirationUnix,omitempty"`
}

type cfStatusUpdate struct {
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
	value        []*cfStatusDealEntry
	valueEncoded string
}

var oldKvLastPct = 101
var oldKvCountPending, oldKvCountUpdated int
var oldExportStatus = &cli.Command{
	Usage: "Export status of individual DAGs to he legacy CF KV-store",
	Name:  "old-export-status",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {

		ctx, closer := context.WithCancel(cctx.Context)
		defer closer()

		defer func() { log.Infow("summary", "updated", oldKvCountUpdated) }()

		t0 := time.Now()
		err := db.QueryRow(
			ctx,
			`
			SELECT COUNT( DISTINCT ( ds.source_key ) )
				FROM cargo.dag_sources ds
				JOIN cargo.dags d USING ( cid_v1 )
				JOIN cargo.sources s USING ( srcid )
			WHERE
				s.project = 2
					AND
				d.size_actual IS NOT NULL
					AND
				( ds.entry_last_exported IS NULL OR d.entry_last_updated > ds.entry_last_exported )
			`,
		).Scan(&oldKvCountPending)
		if err != nil {
			return err
		}
		if oldKvCountPending == 0 {
			return nil
		}

		log.Infof("updating status of approximately %d entries", oldKvCountPending)

		rows, err := db.Query(
			ctx,
			`
			SELECT
					ds.source_key,
					ds.cid_v1,
					(
						CASE WHEN
							d.size_actual IS NULL
								OR
							ds.entry_removed IS NOT NULL
								OR
							s.weight < 0
								OR
							EXISTS (
								SELECT 42 FROM cargo.aggregate_entries ae, cargo.deals de
								WHERE ds.cid_v1 = ae.cid_v1 AND ae.aggregate_cid = de.aggregate_cid AND de.status IN ( 'active' )
							)
						THEN 0 ELSE 1 END
					) AS queued,
					( SELECT COUNT(de.deal_id) FROM cargo.aggregate_entries ae, cargo.deals de WHERE ds.cid_v1 = ae.cid_v1 AND ae.aggregate_cid = de.aggregate_cid AND de.status = 'published' ) AS published,
					( SELECT COUNT(de.deal_id) FROM cargo.aggregate_entries ae, cargo.deals de WHERE ds.cid_v1 = ae.cid_v1 AND ae.aggregate_cid = de.aggregate_cid AND de.status = 'active' ) AS active,
					( SELECT COUNT(de.deal_id) FROM cargo.aggregate_entries ae, cargo.deals de WHERE ds.cid_v1 = ae.cid_v1 AND ae.aggregate_cid = de.aggregate_cid AND de.status = 'terminated' ) AS terminated,
					de.status,
					COALESCE( de.entry_last_updated, d.entry_last_updated ),
					ae.aggregate_cid,
					a.piece_cid,
					de.provider,
					de.deal_id,
					ae.datamodel_selector,
					de.start_time,
					de.end_time
				FROM cargo.dag_sources ds
				JOIN cargo.sources s USING ( srcid )
				JOIN cargo.dags d USING ( cid_v1 )
				LEFT JOIN cargo.aggregate_entries ae USING ( cid_v1 )
				LEFT JOIN cargo.aggregates a USING ( aggregate_cid )
				LEFT JOIN cargo.deals de USING ( aggregate_cid )
			WHERE
				s.project = 2
					AND
				d.size_actual IS NOT NULL
					AND
				( ds.entry_last_exported IS NULL OR d.entry_last_updated > ds.entry_last_exported )
			ORDER BY ds.source_key -- order is critical to form bulk-update batches
			`,
		)
		if err != nil {
			return err
		}

		var priorKey string
		updates := make(map[string]*cfStatusUpdate, 10000)
		updatesApproxBytes := 0

		for rows.Next() {

			curCidReceiver := new(cfStatusUpdate)
			curDeal := new(cfStatusDealEntry)
			var dStart, dEnd *time.Time
			if err = rows.Scan(
				&curCidReceiver.key,
				&curCidReceiver.cidv1,
				&curCidReceiver.metadata.Queued,
				&curCidReceiver.metadata.Published,
				&curCidReceiver.metadata.Active,
				&curCidReceiver.metadata.Terminated,
				&curDeal.Status,
				&curDeal.LastChanged,
				&curDeal.AggregateRootCid,
				&curDeal.PieceCid,
				&curDeal.Provider,
				&curDeal.ChainDealID,
				&curDeal.DatamodelSelector,
				&dStart,
				&dEnd,
			); err != nil {
				return err
			}

			// this is a new key - since we are ordered we know we are done with the prior one
			if _, exists := updates[curCidReceiver.key]; !exists {

				// deal with prior state if any
				if priorKey != "" {
					// we will be changing the key: encode everything accumulated
					priorRecord := updates[priorKey]
					buf := new(bytes.Buffer)
					if err := json.NewEncoder(buf).Encode(priorRecord.value); err != nil {
						return err
					}
					priorRecord.valueEncoded = buf.String()
					updatesApproxBytes += len(priorRecord.valueEncoded)
				}

				priorKey = curCidReceiver.key

				// see if we grew too big and need to flush
				// 10k entries / 100MiB size ( round down for overhead, can be significant )
				if len(updates) > 9999 || updatesApproxBytes > (85<<20) {
					if err = cfUploadAndMarkUpdates(cctx, t0, updates); err != nil {
						return err
					}
					// reset
					updatesApproxBytes = 0
					updates = make(map[string]*cfStatusUpdate, 10000)
				}

				curCidReceiver.value = make([]*cfStatusDealEntry, 0)
				updates[curCidReceiver.key] = curCidReceiver
			}

			// not a deal and not for queueing ( failed pin or whatever )
			// no dealinfo to add
			if curCidReceiver.metadata.Queued == 0 && curDeal.Status == nil {
				continue
			}

			lcU := curDeal.LastChanged.Unix()
			curDeal.LastChangedUnix = &lcU

			if curDeal.Status == nil {
				s := "queued"
				curDeal.Status = &s
			} else {
				n := "mainnet"
				curDeal.Network = &n

				if dStart != nil {
					curDeal.DealActivation = dStart
					tu := dStart.Unix()
					curDeal.DealActivationUnix = &tu
				}
				if dEnd != nil {
					curDeal.DealExpiration = dEnd
					tu := dEnd.Unix()
					curDeal.DealExpirationUnix = &tu
				}
			}

			updates[priorKey].value = append(updates[priorKey].value, curDeal)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		return cfUploadAndMarkUpdates(cctx, t0, updates)
	},
}

func cfUploadAndMarkUpdates(cctx *cli.Context, updStartTime time.Time, updates map[string]*cfStatusUpdate) error {

	toUpd := make(cloudflare.WorkersKVBulkWriteRequest, 0, len(updates))
	updatedCids := make([]string, 0, len(updates))
	for _, u := range updates {

		if u.valueEncoded == "" {
			if u.value == nil {
				return xerrors.Errorf("unexpected nil value for %s", u.key)
			}
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
		`
		UPDATE cargo.dag_sources ds
			SET entry_last_exported = $1
		FROM cargo.sources s
		WHERE
			s.project = 2
				AND
			ds.srcid = s.srcid
				AND
			ds.cid_v1 = ANY ( $2 )
		`,
		updStartTime,
		updatedCids,
	)
	if err != nil {
		return err
	}

	oldKvCountUpdated += len(updatedCids)
	if showProgress && 100*oldKvCountUpdated/oldKvCountPending != oldKvLastPct {
		oldKvLastPct = 100 * oldKvCountUpdated / oldKvCountPending
		fmt.Fprintf(os.Stderr, "%d%%\r", oldKvLastPct)
	}

	return nil
}
