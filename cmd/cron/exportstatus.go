package main

import (
	"context"
	"strings"
	"time"

	"github.com/hasura/go-graphql-client"
	"github.com/urfave/cli/v2"
)

const faunaBatchSize = 9_000

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
type Long int64                   //nolint:revive
type DealStatus string            //nolint:revive
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
	Usage: "Export status of individual DAGs to external databases",
	Name:  "export-status",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		for _, p := range faunaProjects {
			if err := updateDealStates(cctx, p); err != nil {
				return err
			}
		}
		return nil
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

	err := db.QueryRow(
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

	rows, err := db.Query(
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
				return err
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

				if err := gql.Mutate(ctx, new(faunaMutateUpsertDeal), map[string]interface{}{
					"aCid":       graphql.String(aCidStr),
					"provider":   graphql.String(provider),
					"dealID":     Long(dealID),
					"dealStart":  dealStart,
					"dealEnd":    dealEnd,
					"status":     DealStatus(strings.Title(dealStatus)),
					"statusLong": graphql.String(*dealStatusDesc),
				}); err != nil && err.Error() != "Instance is not unique." {
					return err
				}
				countDeals++
			}
			if err := deals.Err(); err != nil {
				return err
			}
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

func faunaUploadEntriesAndMarkUpdated(ctx context.Context, project faunaProject, gql *graphql.Client, updStartTime time.Time, aggCid string, entries []AggregateEntryInput) error {

	markDone := make([]string, len(entries))
	for i := range entries {
		markDone[i] = entries[i].cidKey
	}

	if err := gql.Mutate(ctx, new(faunaMutateAddAggregateEntries), map[string]interface{}{
		"aCid":       graphql.String(aggCid),
		"aggEntries": entries,
	}); err != nil {
		return err
	}

	_, err := db.Exec(
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
		markDone,
	)
	return err
}
