package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hasura/go-graphql-client"
	"github.com/jackc/pgx/v4"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

const faunaBatchSize = 2048

//
// golang Graphql offerings are a trash-fire
//
type faunaMutateCreateAggregate struct {
	CreateAggregate struct {
		ID string `graphql:"_id"` // we always must pull *something* otherwise gql won't dance
	} `graphql:"createAggregate( data:{ dataCid:$aCid pieceCid:$pCid } )"`
}
type faunaMutateUpsertDeal struct {
	CreateOrUpdateDeal struct {
		ID string `graphql:"_id"` // we always must pull *something* otherwise gql won't dance
	} `graphql:"createOrUpdateDeal( data:{ dataCid:$aCid storageProvider:$provider dealId:$dealID activation:$dealStart renewal:$dealEnd status:$status statusReason:$statusLong } )"`
}
type faunaMutateAddAggregateEntries struct {
	AddAggregateEntries struct {
		ID string `graphql:"_id"` // we always must pull *something* otherwise gql won't dance
	} `graphql:"addAggregateEntries( dataCid:$aCid, entries:$aggEntries )"`
}
type AggregateEntryInput struct { //nolint:revive
	cidKey            string
	Cid               string  `json:"cid"`
	DataModelSelector *string `json:"dataModelSelector"`
	DagSize           int64   `json:"dagSize"`
}

//
// end of trash-fire
//

var exportStatus = &cli.Command{
	Usage: "Export service metrics and status of individual DAG entries to external databases",
	Name:  "export-status",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {

		var wg sync.WaitGroup
		errCh := make(chan error, len(faunaProjects)+1)

		// fire off individual project updates
		for i := range faunaProjects {
			wg.Add(1)
			p := faunaProjects[i]
			go func() {
				defer wg.Done()
				if err := updateDealStates(cctx, p); err != nil {
					errCh <- xerrors.Errorf("failure updating status of %s DAGs: %w", p.label, err)
				}
			}()
		}

		wg.Wait()
		close(errCh) // no more writers
		return <-errCh
	},
}

func updateDealStates(cctx *cli.Context, project faunaProject) error {

	ctx, closer := context.WithCancel(cctx.Context)
	defer closer()

	var countPending, countUpdated, countAggregates, countDeals int
	defer func() {
		log.Infow("summary",
			"project", project.label,
			"dagEntries", countPending,
			"aggregateEntries", countUpdated,
			"relatedAggregates", countAggregates,
			"relatedDeals", countDeals,
		)
	}()

	rotx, err := db.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly, IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return err
	}
	defer rotx.Rollback(context.Background()) //nolint:errcheck

	// update batches can be enormous
	_, err = rotx.Exec(ctx, fmt.Sprintf(`SET LOCAL statement_timeout = %d`, (3*time.Hour).Milliseconds()))
	if err != nil {
		return err
	}

	err = rotx.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
			FROM cargo.dag_sources ds
			JOIN cargo.dags d USING ( cid_v1 )
			JOIN cargo.sources s USING ( srcid )
		WHERE
			s.project = $1
				AND
			( ds.entry_last_exported IS NULL OR d.entry_last_updated > ds.entry_last_exported )
				AND
			EXISTS ( SELECT 42 FROM cargo.aggregate_entries ae WHERE ae.cid_v1 = ds.cid_v1 )
		`,
		project.id,
	).Scan(&countPending)
	if err != nil {
		return err
	}

	log.Infof("updating status of approximately %d DAGs in project %s", countPending, project.label)
	if countPending == 0 {
		return nil
	}

	rows, err := rotx.Query(
		ctx,
		`
		SELECT DISTINCT -- multiple user uploading same cid need a single linkage update
				d.cid_v1,
				d.size_actual,
				COALESCE( ds.details->>'original_cid', d.cid_v1 ) AS original_cid,
				ae.aggregate_cid,
				a.piece_cid,
				ae.datamodel_selector
			FROM cargo.dag_sources ds
			JOIN cargo.sources s USING ( srcid )
			JOIN cargo.dags d USING ( cid_v1 )
			JOIN cargo.aggregate_entries ae USING ( cid_v1 )
			JOIN cargo.aggregates a USING ( aggregate_cid )
		WHERE
			s.project = $1
				AND
			( ds.entry_last_exported IS NULL OR d.entry_last_updated > ds.entry_last_exported )
		ORDER BY ae.aggregate_cid -- order is critical to form bulk-update batches
		`,
		project.id,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	gql, err := faunaClient(cctx, project.label)
	if err != nil {
		return err
	}

	t0 := time.Now()
	var curAggCid string
	curAggEntries := make([]AggregateEntryInput, 0, faunaBatchSize)

	aggSeen := make(map[string]struct{}, 256)
	for rows.Next() {
		var dCidStr, aCidStr, pCidStr, origCid string
		var dmSelector *string
		var dagSize int64
		if err := rows.Scan(
			&dCidStr,
			&dagSize,
			&origCid,
			&aCidStr,
			&pCidStr,
			&dmSelector,
		); err != nil {
			return err
		}

		// always create the aggregate first + update deal states
		if _, seen := aggSeen[aCidStr]; !seen {
			aggSeen[aCidStr] = struct{}{}
			if err := gql.Mutate(ctx, new(faunaMutateCreateAggregate), map[string]interface{}{
				"aCid": graphql.String(aCidStr),
				"pCid": graphql.String(pCidStr),
			}); err != nil && err.Error() != "Instance is not unique." {
				return xerrors.Errorf("createAggregate() %s failed: %w", aCidStr, err)
			}
			countAggregates++

			deals, err := db.Query(
				ctx,
				`
				SELECT dl.deal_id, dl.provider, dl.start_time, dl.end_time, dl.status, dl.status_meta
					FROM cargo.deals dl
				WHERE dl.aggregate_cid = $1
				`,
				aCidStr,
			)
			if err != nil {
				return err
			}
			defer deals.Close()

			for deals.Next() {
				var dealID int64
				var dealStart, dealEnd time.Time
				var provider, dealStatus string
				var dealStatusDesc *string
				if err := deals.Scan(&dealID, &provider, &dealStart, &dealEnd, &dealStatus, &dealStatusDesc); err != nil {
					return err
				}

				if dealStatusDesc == nil {
					dealStatusDesc = new(string)
				}
				dealStatus = strings.Title(dealStatus)

				type Long int64        //nolint:revive
				type DealStatus string //nolint:revive

				if err := gql.Mutate(ctx, new(faunaMutateUpsertDeal), map[string]interface{}{
					"aCid":       graphql.String(aCidStr),
					"provider":   graphql.String(provider),
					"dealID":     Long(dealID),
					"dealStart":  dealStart,
					"dealEnd":    dealEnd,
					"status":     DealStatus(dealStatus),
					"statusLong": graphql.String(*dealStatusDesc),
				}); err != nil {
					return xerrors.Errorf("createOrUpdateDeal() %d status %s for aggregate %s failed: %w", dealID, dealStatus, aCidStr, err)

				}
				countDeals++
			}
			if err := deals.Err(); err != nil {
				return err
			}
		}

		// FIXME - fauna can not possibly ingest this specific aggregate (5+ mil entries)
		// just bail on it: safe given all entries are part of something else as well
		/*
			SELECT COUNT(*)
				FROM cargo.aggregate_entries ae1
				LEFT JOIN cargo.aggregate_entries ae2
					ON
						ae1.cid_v1 = ae2.cid_v1
							AND
						ae1.aggregate_cid != ae2.aggregate_cid
			WHERE
				ae1.aggregate_cid = 'bafybeifaow2p3qrzndvr4dzvms7fun3e3bijuwkwrmzou4zupatyennufy'
					AND
				ae2.aggregate_cid IS NULL
		*/
		if aCidStr == "bafybeifaow2p3qrzndvr4dzvms7fun3e3bijuwkwrmzou4zupatyennufy" {
			continue
		}

		// either the aggregate cid changed or the batch is too big
		if (curAggCid != "" && curAggCid != aCidStr) || len(curAggEntries) >= faunaBatchSize {
			if err := faunaUploadEntriesAndMarkUpdated(ctx, project, gql, t0, curAggCid, curAggEntries); err != nil {
				return err
			}
			countUpdated += len(curAggEntries)
			curAggEntries = curAggEntries[:0]
		}
		curAggCid = aCidStr
		curAggEntries = append(curAggEntries, AggregateEntryInput{
			cidKey:            dCidStr,
			Cid:               origCid,
			DagSize:           dagSize,
			DataModelSelector: dmSelector,
		})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if len(curAggEntries) > 0 {
		if err := faunaUploadEntriesAndMarkUpdated(ctx, project, gql, t0, curAggCid, curAggEntries); err != nil {
			return err
		}
		countUpdated += len(curAggEntries)
	}

	return nil
}

func faunaUploadEntriesAndMarkUpdated(
	ctx context.Context,
	project faunaProject,
	gql *graphql.Client,
	updStartTime time.Time,
	aggCid string,
	entries []AggregateEntryInput,
) (err error) {
	markCidDone := make([]string, 0, len(entries))
	defer func() {
		if len(markCidDone) > 0 {
			_, markErr := db.Exec(
				ctx,
				`
				UPDATE cargo.dag_sources ds
					SET entry_last_exported = $1
				FROM cargo.sources s
				WHERE
					s.project = $2
						AND
					ds.srcid = s.srcid
						AND
					ds.cid_v1 = ANY ( $3 )
				`,
				updStartTime,
				project.id,
				markCidDone,
			)
			if markErr != nil {
				log.Errorf("unexpected error marking entries relating to %d CIDs as updated: %s", len(markCidDone), markErr)
				if err == nil {
					err = markErr
				}
			}
		}
	}()

	err = gql.Mutate(ctx, new(faunaMutateAddAggregateEntries), map[string]interface{}{
		"aCid":       graphql.String(aggCid),
		"aggEntries": entries,
	})
	if err == nil {
		for _, e := range entries {
			markCidDone = append(markCidDone, e.cidKey)
		}
		return nil
	} else if err.Error() == "Missing content CID" {
		log.Warnf("addAggregateEntries() of %d entries to aggregate %s failed with some CIDs allegedly missing, retrying individually", len(entries), aggCid)

		for i := range entries {
			err = gql.Mutate(ctx, new(faunaMutateAddAggregateEntries), map[string]interface{}{
				"aCid":       graphql.String(aggCid),
				"aggEntries": entries[i : i+1],
			})
			if err == nil {
				markCidDone = append(markCidDone, entries[i].cidKey)
			} else if err.Error() == "Missing content CID" {
				log.Errorw("missing content CID", "aggregateCid", aggCid, "entries", entries[i:i+1])
				err = nil // continue because... what else can we do :(
			} else {
				return xerrors.Errorf("addAggregateEntries() of entry %s to aggregate %s failed: %w", entries[i].Cid, aggCid, err)
			}
		}

		// these will *not* match if we bailed at the above `err = nil`
		if len(entries) == len(markCidDone) {
			log.Warnf("addAggregateEntries of %d entries to aggregate %s failed as a batch, but succeeded with each individually... sigh", len(entries), aggCid)
		}
	}

	if err != nil {
		return xerrors.Errorf("addAggregateEntries() of %d entries to aggregate %s failed: %w", len(entries), aggCid, err)
	}

	return nil
}
