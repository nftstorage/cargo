package main

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/hasura/go-graphql-client"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"
	prometheuspush "github.com/prometheus/client_golang/prometheus/push"
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

		var wg sync.WaitGroup
		errCh := make(chan error, len(faunaProjects)+1)

		// fire off individual project updates
		for i := range faunaProjects {
			wg.Add(1)
			p := faunaProjects[i]
			go func() {
				defer wg.Done()
				if err := updateDealStates(cctx, p); err != nil {
					errCh <- err
				}
			}()
		}

		// fire off prometheus update
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := pushPrometheusMetrics(cctx); err != nil {
				errCh <- err
			}
		}()

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

				if dealStatusDesc == nil {
					dealStatusDesc = new(string)
				}
				if err := gql.Mutate(ctx, new(faunaMutateUpsertDeal), map[string]interface{}{
					"aCid":       graphql.String(aCidStr),
					"provider":   graphql.String(provider),
					"dealID":     Long(dealID),
					"dealStart":  dealStart,
					"dealEnd":    dealEnd,
					"status":     DealStatus(strings.Title(dealStatus)),
					"statusLong": graphql.String(*dealStatusDesc),
				}); err != nil {
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

var countPromCounters, countPromGauges int

func pushPrometheusMetrics(cctx *cli.Context) error {

	ctx := cctx.Context
	prom := prometheuspush.New(promUrl, "dagcargo").BasicAuth(promUser, promPass)
	snapshotTx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return err
	}
	defer snapshotTx.Rollback(context.Background()) //nolint:errcheck

	projects := make(map[int64]string)
	projRows, err := snapshotTx.Query(
		ctx,
		`
		WITH projids AS ( SELECT DISTINCT( project ) AS p FROM cargo.sources	)
		SELECT p, CASE
			WHEN p = 0 THEN 'staging.web3.storage'
			WHEN p = 1 THEN 'web3.storage'
			WHEN p = 2 THEN 'nft.storage'
			ELSE p::TEXT
		END
			FROM projids
		`,
	)
	if err != nil {
		return err
	}
	for projRows.Next() {
		var pid int64
		var pname string
		if err := projRows.Scan(&pid, &pname); err != nil {
			return err
		}
		projects[pid] = pname
	}
	if err := projRows.Err(); err != nil {
		return err
	}

	defer func() {
		log.Infow("prometheus push completed",
			"counterMetrics", countPromCounters,
			"gaugeMetrics", countPromGauges,
			"projects", len(projects),
		)
	}()

	if err := exportCounter(
		ctx, snapshotTx, prom,
		`dagcargo_handled_total_dags`, nil,
		`How many unique DAGs did the service handle since inception`,
		`SELECT COUNT(*) FROM cargo.dags`,
	); err != nil {
		return err
	}

	if err := exportCounter(
		ctx, snapshotTx, prom,
		`dagcargo_handled_total_blocks`, nil,
		`How many unique-by-cid blocks did the service handle since inception`,
		`SELECT COUNT(*) FROM ( SELECT DISTINCT( ref_v1 ) FROM cargo.refs ) d`,
	); err != nil {
		return err
	}

	var dealStates []string
	if err := snapshotTx.QueryRow(ctx, `SELECT ARRAY_AGG( DISTINCT( status )) FROM cargo.deals`).Scan(&dealStates); err != nil {
		return err
	}
	for _, status := range dealStates {
		if err := exportGauge(
			ctx, snapshotTx, prom,
			`dagcargo_filecoin_deals`, prometheus.Labels{"status": status}, `Count of filecoin deals for aggregates created by the service`,
			`SELECT COUNT(*) FROM cargo.deals WHERE status = $1`,
			status,
		); err != nil {
			return err
		}
	}

	for projID, projName := range projects {
		projLabel := prometheus.Labels{"project": projName}

		if err := exportGauge(
			ctx, snapshotTx, prom,
			`dagcargo_sources_total_without_uploads`, projLabel, `Count of sources/users that have not yet stored a single DAG`,
			`
			SELECT COUNT(*)
				FROM cargo.sources s
			WHERE
				s.project = $1
					AND
				NOT EXISTS ( SELECT 42 FROM cargo.dag_sources ds WHERE ds.srcid = s.srcid )
			`,
			projID,
		); err != nil {
			return err
		}

		if err := exportCounter(
			ctx, snapshotTx, prom,
			`dagcargo_sources_total_with_uploads`, projLabel, `Count of sources/users that have used the service to store data`,
			`
			SELECT COUNT(*)
				FROM cargo.sources s
			WHERE
				s.project = $1
					AND
				EXISTS ( SELECT 42 FROM cargo.dag_sources ds WHERE ds.srcid = s.srcid )
			`,
			projID,
		); err != nil {
			return err
		}

		if err := exportGauge(
			ctx, snapshotTx, prom,
			`dagcargo_project_stored_items_active`, projLabel, `Count of non-deleted non-pending items stored per project`,
			`
			SELECT COUNT(*)
				FROM cargo.dag_sources ds
				JOIN cargo.sources s USING ( srcid )
				JOIN cargo.dags d USING ( cid_v1 )
			WHERE
				s.project = $1
					AND
				( s.weight IS NULL OR s.weight >= 0 )
					AND
				d.size_actual IS NOT NULL
					AND
				ds.entry_removed IS NULL
			`,
			projID,
		); err != nil {
			return err
		}

		if err := exportGauge(
			ctx, snapshotTx, prom,
			`dagcargo_project_stored_items_deleted`, projLabel, `Count of items marked deleted per project`,
			`
			SELECT COUNT(*)
				FROM cargo.dag_sources ds
				JOIN cargo.sources s USING ( srcid )
			WHERE
				s.project = $1
					AND
				( s.weight IS NULL OR s.weight >= 0 )
					AND
				ds.entry_removed IS NOT NULL
			`,
			projID,
		); err != nil {
			return err
		}

		if err := exportGauge(
			ctx, snapshotTx, prom,
			`dagcargo_project_stored_items_pending`, projLabel, `Count of items pending retrieval from IPFS per project`,
			`
			SELECT COUNT(*)
				FROM cargo.dag_sources ds
				JOIN cargo.sources s USING ( srcid )
				JOIN cargo.dags d USING ( cid_v1 )
			WHERE
				s.project = $1
					AND
				( s.weight IS NULL OR s.weight >= 0 )
					AND
				d.size_actual IS NULL
					AND
				ds.entry_removed IS NULL
			`,
			projID,
		); err != nil {
			return err
		}

		if err := exportGauge(
			ctx, snapshotTx, prom,
			`dagcargo_project_stored_bytes_active`, projLabel, `Amount of known per-DAG-deduplicated bytes stored per project`,
			`
			SELECT SUM(d.size_actual)
				FROM cargo.dag_sources ds
				JOIN cargo.sources s USING ( srcid )
				JOIN cargo.dags d USING ( cid_v1 )
			WHERE
				s.project = $1
					AND
				( s.weight IS NULL OR s.weight >= 0 )
					AND
				d.size_actual IS NOT NULL
					AND
				ds.entry_removed IS NULL
			`,
			projID,
		); err != nil {
			return err
		}

		if err := exportGauge(
			ctx, snapshotTx, prom,
			`dagcargo_project_stored_bytes_deleted`, projLabel, `Amount of known per-DAG-deduplicated bytes retrieved and then marked deleted per project`,
			`
			SELECT SUM(d.size_actual)
				FROM cargo.dag_sources ds
				JOIN cargo.sources s USING ( srcid )
				JOIN cargo.dags d USING ( cid_v1 )
			WHERE
				s.project = $1
					AND
				( s.weight IS NULL OR s.weight >= 0 )
					AND
				d.size_actual IS NOT NULL
					AND
				ds.entry_removed IS NOT NULL
			`,
			projID,
		); err != nil {
			return err
		}

		if err := exportGauge(
			ctx, snapshotTx, prom,
			`dagcargo_project_items_in_active_deals`, projLabel, `Count of aggregated items with at least one active deal per project`,
			`
			SELECT COUNT(*)
				FROM cargo.dag_sources ds
				JOIN cargo.sources s USING ( srcid )
			WHERE
				s.project = $1
					AND
				EXISTS (
					SELECT 42
						FROM cargo.aggregate_entries ae
						JOIN cargo.deals de USING ( aggregate_cid )
					WHERE
						ae.cid_v1 = ds.cid_v1
							AND
						de.status = 'active'
				)
			`,
			projID,
		); err != nil {
			return err
		}

		if err := exportGauge(
			ctx, snapshotTx, prom,
			`dagcargo_project_items_undealt_aggregates`, projLabel, `Count of aggregated items queued for initial deal per project`,
			`
			SELECT COUNT(*)
				FROM cargo.dag_sources ds
				JOIN cargo.sources s USING ( srcid )
			WHERE
				s.project = $1
					AND
				EXISTS ( SELECT 42 FROM cargo.aggregate_entries ae WHERE ae.cid_v1 = ds.cid_v1 )
					AND
				NOT EXISTS (
					SELECT 42
						FROM cargo.aggregate_entries ae
						JOIN cargo.deals de USING ( aggregate_cid )
					WHERE
						ae.cid_v1 = ds.cid_v1
							AND
						de.status = 'active'
				)
			`,
			projID,
		); err != nil {
			return err
		}

		if err := exportGauge(
			ctx, snapshotTx, prom,
			`dagcargo_project_items_unaggregated`, projLabel, `Count of items pending initial aggregate inclusion per project`,
			`
			SELECT COUNT(*)
				FROM cargo.dag_sources ds
				JOIN cargo.sources s USING ( srcid )
				JOIN cargo.dags d USING ( cid_v1 )
				LEFT JOIN cargo.aggregate_entries ae USING ( cid_v1 )
			WHERE
				s.project = $1
					AND
				( s.weight IS NULL OR s.weight >= 0 )
					AND
				d.size_actual IS NOT NULL
					AND
				ds.entry_removed IS NULL
					AND
				ae.cid_v1 IS NULL
			`,
			projID,
		); err != nil {
			return err
		}
	}

	return prom.Push()
}

func exportCounter(
	ctx context.Context,
	tx pgx.Tx,
	prom *prometheuspush.Pusher,
	name string, labels prometheus.Labels,
	help string,
	querySQL string, queryParams ...interface{},
) error {

	var val int64
	if err := tx.QueryRow(ctx, querySQL, queryParams...).Scan(&val); err != nil {
		return err
	}

	c := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help, ConstLabels: labels})
	c.Add(float64(val))
	prom.Collector(c)
	countPromCounters++

	return nil
}

func exportGauge(
	ctx context.Context,
	tx pgx.Tx,
	prom *prometheuspush.Pusher,
	name string, labels prometheus.Labels,
	help string,
	querySQL string, queryParams ...interface{},
) error {

	var val int64
	if err := tx.QueryRow(ctx, querySQL, queryParams...).Scan(&val); err != nil {
		return err
	}

	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help, ConstLabels: labels})
	g.Set(float64(val))
	prom.Collector(g)
	countPromGauges++

	return nil
}
