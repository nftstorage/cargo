package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"
	prometheuspush "github.com/prometheus/client_golang/prometheus/push"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

type cargoMetricType string

var cargoMetricGauge = cargoMetricType("gauge")
var cargoMetricCounter = cargoMetricType("counter")

type cargoMetric struct {
	kind  cargoMetricType
	name  string
	help  string
	query string
}

var workerCount = 24

var pushMetrics = &cli.Command{
	Usage:  "Push service metrics to external collectors",
	Name:   "push-metrics",
	Flags:  []cli.Flag{},
	Action: pushPrometheusMetrics,
}

var metricsList = []cargoMetric{
	//
	// A generalized per-project-aggregating query almost certainly would look like:
	//
	/*

		kind: cargoMetricGauge,
		name: "…",
		help: "…",
		query: `
			WITH
				active_sources AS (
					SELECT * FROM cargo.sources WHERE weight >= 0 OR weight IS NULL
				),
				q AS (
					SELECT s.project, COUNT(*) val
						FROM active_sources s
						…
					WHERE
						…
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
		`,
	*/
	//

	//
	// source/user counts
	{
		kind: cargoMetricGauge,
		name: "dagcargo_sources_total_without_uploads",
		help: "Count of sources/users that have not yet stored a single DAG",
		query: `
			WITH
				active_sources AS (
					SELECT * FROM cargo.sources WHERE weight >= 0 OR weight IS NULL
				),
				q AS (
					SELECT s.project, COUNT(*) val
						FROM active_sources s
					WHERE
						NOT EXISTS (
							SELECT 42
								FROM cargo.dag_sources ds
							WHERE s.srcid = ds.srcid
						)
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
		`,
	},
	{
		kind: cargoMetricCounter,
		name: "dagcargo_sources_total_with_uploads",
		help: "Count of sources/users that have used the service to store data",
		query: `
			WITH
				active_sources AS (
					SELECT * FROM cargo.sources WHERE weight >= 0 OR weight IS NULL
				),
				q AS (
					SELECT s.project, COUNT(*) val
						FROM active_sources s
					WHERE
						EXISTS (
							SELECT 42
								FROM cargo.dag_sources ds
							WHERE s.srcid = ds.srcid
						)
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
		`,
	},

	//
	// dag (item) in various states per source
	{
		kind: cargoMetricGauge,
		name: "dagcargo_project_stored_items_active",
		help: "Count of non-deleted analyzed items stored per project",
		query: `
			WITH
				active_sources AS (
					SELECT * FROM cargo.sources WHERE weight >= 0 OR weight IS NULL
				),
				q AS (
					SELECT s.project, COUNT(*) val
						FROM active_sources s
						JOIN cargo.dag_sources ds USING ( srcid )
						JOIN cargo.dags d USING ( cid_v1 )
					WHERE
						d.size_actual IS NOT NULL
							AND
						ds.entry_removed IS NULL
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
		`,
	},
	{
		kind: cargoMetricGauge,
		name: "dagcargo_project_stored_items_deleted",
		help: "Count of items marked deleted per project",
		query: `
			WITH
				active_sources AS (
					SELECT * FROM cargo.sources WHERE weight >= 0 OR weight IS NULL
				),
				q AS (
					SELECT s.project, COUNT(*) val
						FROM active_sources s
						JOIN cargo.dag_sources ds USING ( srcid )
					WHERE
						ds.entry_removed IS NOT NULL
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
					FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
		`,
	},
	{
		kind: cargoMetricGauge,
		name: "dagcargo_project_stored_items_pending",
		help: "Count of items pending retrieval from IPFS per project",
		query: `
			WITH
				active_sources AS (
					SELECT * FROM cargo.sources WHERE weight >= 0 OR weight IS NULL
				),
				q AS (
					SELECT s.project, COUNT(*) val
						FROM active_sources s
						JOIN cargo.dag_sources ds USING ( srcid )
						JOIN cargo.dags d USING ( cid_v1 )
					WHERE
						d.size_actual IS NULL
							AND
						ds.entry_removed IS NULL
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
		`,
	},
	{
		kind: cargoMetricGauge,
		name: "dagcargo_project_stored_items_oversized",
		help: "Count of items larger than a 32GiB sector",
		query: fmt.Sprintf(
			`
			WITH
				active_sources AS (
					SELECT * FROM cargo.sources WHERE weight >= 0 OR weight IS NULL
				),
				q AS (
					SELECT s.project, COUNT(*) val
						FROM active_sources s
						JOIN cargo.dag_sources ds USING ( srcid )
						JOIN cargo.dags d USING ( cid_v1 )
					WHERE
						d.size_actual > %d
							AND
						ds.entry_removed IS NULL
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
			`,
			targetMaxSize,
		),
	},
	{
		kind: cargoMetricGauge,
		name: "dagcargo_project_stored_items_inactive",
		help: "Count of items exclusively from inactive sources",
		query: `
			WITH
				inactive_sources AS (
					SELECT * FROM cargo.sources WHERE weight < 0 AND source != 'INTERNAL SYSTEM USER'
				),
				q AS (
					SELECT s.project, COUNT(*) val
						FROM inactive_sources s
						JOIN cargo.dag_sources ds USING ( srcid )
						JOIN cargo.dags d USING ( cid_v1 )
					WHERE
						d.size_actual IS NOT NULL
							AND
						ds.entry_removed IS NULL
							AND
						-- ensure an active source doesn't pin the same dag
						NOT EXISTS (
							SELECT 42
								FROM cargo.dag_sources actds
								JOIN cargo.sources acts USING ( srcid )
							WHERE
								actds.cid_v1 = ds.cid_v1
									AND
								( acts.weight >= 0 OR acts.weight IS NULL )
						)
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
			`,
	},
	{
		kind: cargoMetricGauge,
		name: "dagcargo_project_stored_bytes_active",
		help: "Amount of known per-DAG-deduplicated bytes stored per project",
		query: `
			WITH
				active_sources AS (
					SELECT * FROM cargo.sources WHERE weight >= 0 OR weight IS NULL
				),
				q AS (
					SELECT s.project, SUM(d.size_actual) val
						FROM active_sources s
						JOIN cargo.dag_sources ds USING ( srcid )
						JOIN cargo.dags d USING ( cid_v1 )
					WHERE
						d.size_actual IS NOT NULL
							AND
						ds.entry_removed IS NULL
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
		`,
	},
	{
		kind: cargoMetricGauge,
		name: "dagcargo_project_stored_bytes_active_deduplicated",
		help: "Amount of known best-effort-deduplicated bytes stored per project",
		query: `
			WITH
				active_sources AS (
					SELECT * FROM cargo.sources WHERE weight >= 0 OR weight IS NULL
				),
				q AS (
					SELECT s.project, SUM(d.size_actual) val
						FROM active_sources s
						JOIN cargo.dag_sources ds USING ( srcid )
						JOIN cargo.dags d USING ( cid_v1 )
					WHERE
						d.size_actual IS NOT NULL
							AND
						ds.entry_removed IS NULL
							AND
						-- ensure we are not a part of something else active
						NOT EXISTS (
							SELECT 42
								FROM cargo.refs r
								JOIN cargo.dag_sources rds
									ON r.cid_v1 = rds.cid_v1 AND r.ref_cid = d.cid_v1 AND rds.entry_removed IS NULL
								JOIN cargo.sources rs
									ON rds.srcid = rs.srcid AND ( rs.weight >= 0 OR rs.weight IS NULL )
						)
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
		`,
	},
	{
		kind: cargoMetricGauge,
		name: "dagcargo_project_stored_bytes_deleted",
		help: "Amount of known per-DAG-deduplicated bytes retrieved and then marked deleted per project",
		query: `
			WITH
				active_sources AS (
					SELECT * FROM cargo.sources WHERE weight >= 0 OR weight IS NULL
				),
				q AS (
					SELECT s.project, SUM(d.size_actual) val
						FROM active_sources s
						JOIN cargo.dag_sources ds USING ( srcid )
						JOIN cargo.dags d USING ( cid_v1 )
					WHERE
						d.size_actual IS NOT NULL
							AND
						ds.entry_removed IS NOT NULL
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
		`,
	},
	{
		kind: cargoMetricGauge,
		name: "dagcargo_project_stored_bytes_deleted_deduplicated",
		help: "Amount of known best-effort-deduplicated bytes retrieved and then marked deleted per project",
		query: `
			WITH
				active_sources AS (
					SELECT * FROM cargo.sources WHERE weight >= 0 OR weight IS NULL
				),
				q AS (
					SELECT s.project, SUM(d.size_actual) val
						FROM active_sources s
						JOIN cargo.dag_sources ds USING ( srcid )
						JOIN cargo.dags d USING ( cid_v1 )
					WHERE
						d.size_actual IS NOT NULL
							AND
						ds.entry_removed IS NOT NULL
							AND
						-- ensure we are not a part of something else active
						NOT EXISTS (
							SELECT 42
								FROM cargo.refs r
								JOIN cargo.dag_sources rds
									ON r.cid_v1 = rds.cid_v1 AND r.ref_cid = d.cid_v1 AND rds.entry_removed IS NULL
								JOIN cargo.sources rs
									ON rds.srcid = rs.srcid AND ( rs.weight >= 0 OR rs.weight IS NULL )
						)
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
		`,
	},

	{
		kind: cargoMetricGauge,
		name: "dagcargo_project_stored_bytes_oversized_deduplicated",
		help: "Amount of bytes in dags larger than a 32GiB sector",
		query: fmt.Sprintf(
			`
			WITH
				active_sources AS (
					SELECT * FROM cargo.sources WHERE weight >= 0 OR weight IS NULL
				),
				q AS (
					SELECT s.project, SUM(d.size_actual) val
						FROM active_sources s
						JOIN cargo.dag_sources ds USING ( srcid )
						JOIN cargo.dags d USING ( cid_v1 )
					WHERE
						d.size_actual > %d
							AND
						ds.entry_removed IS NULL
							AND
						-- ensure we are not a part of something else active
						NOT EXISTS (
							SELECT 42
								FROM cargo.refs r
								JOIN cargo.dag_sources rds
									ON r.cid_v1 = rds.cid_v1 AND r.ref_cid = d.cid_v1 AND rds.entry_removed IS NULL
								JOIN cargo.sources rs
									ON rds.srcid = rs.srcid AND ( rs.weight >= 0 OR rs.weight IS NULL )
						)
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
			`,
			targetMaxSize,
		),
	},
	{
		kind: cargoMetricGauge,
		name: "dagcargo_project_stored_bytes_inactive_deduplicated",
		help: "Amount of bytes in dags exclusively from inactive sources",
		query: `
			WITH
				inactive_sources AS (
					SELECT * FROM cargo.sources WHERE weight < 0 AND source != 'INTERNAL SYSTEM USER'
				),
				q AS (
					SELECT s.project, SUM(d.size_actual) val
						FROM inactive_sources s
						JOIN cargo.dag_sources ds USING ( srcid )
						JOIN cargo.dags d USING ( cid_v1 )
					WHERE
						d.size_actual IS NOT NULL
							AND
						ds.entry_removed IS NULL
							AND
						-- ensure an active source doesn't pin the same dag
						NOT EXISTS (
							SELECT 42
								FROM cargo.dag_sources actds
								JOIN cargo.sources acts USING ( srcid )
							WHERE
								actds.cid_v1 = ds.cid_v1
									AND
								( acts.weight >= 0 OR acts.weight IS NULL )
						)
							AND
						-- ensure we are not a part of something else
						NOT EXISTS ( SELECT 42 FROM cargo.refs r WHERE r.ref_cid = d.cid_v1 )
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
			`,
	},

	//
	// DAG-related counts
	{
		kind: cargoMetricCounter,
		name: "dagcargo_handled_total_dags",
		help: "How many user-named DAGs did the service handle since inception",
		query: `
			SELECT COUNT(*)
				FROM cargo.dags
			WHERE
				size_actual IS NOT NULL
		`,
	},
	{
		kind: cargoMetricCounter,
		name: "dagcargo_handled_total_bytes",
		help: "How many best-effort-deduplicated bytes did the service handle since inception",
		query: `
			SELECT SUM( size_actual )
				FROM cargo.dags d
				LEFT JOIN cargo.refs r
					ON d.cid_v1 = r.ref_cid
			WHERE
				d.size_actual IS NOT NULL
					AND
				r.ref_cid IS NULL
		`,
	},
	{
		kind: cargoMetricCounter,
		name: "dagcargo_handled_total_blocks",
		help: "How many unique-by-cid blocks did the service handle since inception",
		query: `
			SELECT
				(
					SELECT COUNT(*) FROM (
						SELECT DISTINCT( ref_cid ) FROM cargo.refs
					) d
				)
					+
				(
					SELECT COUNT(*)
						FROM cargo.dags d
						LEFT JOIN cargo.refs r
							ON d.cid_v1 = r.ref_cid
					WHERE
						d.size_actual IS NOT NULL
							AND
						r.ref_cid IS NULL
				)
		`,
	},

	//
	// deal-related metrics
	{
		kind: cargoMetricCounter,
		name: "dagcargo_filecoin_aggregates",
		help: "Count of aggregates created",
		query: `
			SELECT COUNT(*)
				FROM cargo.aggregates
		`,
	},
	{
		kind: cargoMetricCounter,
		name: "dagcargo_filecoin_aggregates_timebox_forced",
		help: "Count of aggregates created by repackaging already stored DAGs in oder to satisfy timing and size constraints",
		query: `
			SELECT COUNT(*)
				FROM cargo.aggregates
			WHERE
				(metadata->'Timeboxed')::BOOLEAN
		`,
	},
	{
		kind: cargoMetricGauge,
		name: "dagcargo_filecoin_deals",
		help: "Count of filecoin deals for aggregates packaged by the service",
		query: `
			WITH
				dealstates AS (
					SELECT status, COUNT(*) val
						FROM cargo.deals
					GROUP BY status
				)
			SELECT d.status, COALESCE( dealstates.val, 0 ) AS val
				FROM ( SELECT DISTINCT( status ) FROM cargo.deals ) d
				LEFT JOIN dealstates USING ( status )
		`,
	},
	{
		kind: cargoMetricGauge,
		name: "dagcargo_project_items_in_active_deals",
		help: "Count of aggregated items with at least one active deal per project",
		query: `
			WITH
				active_sources AS (
					SELECT * FROM cargo.sources WHERE weight >= 0 OR weight IS NULL
				),
				q AS (
					SELECT s.project, COUNT(*) val
						FROM active_sources s
						JOIN cargo.dag_sources ds USING ( srcid )
					WHERE
						EXISTS (
							SELECT 42
								FROM cargo.aggregate_entries ae
								JOIN cargo.deals de
									ON ae.cid_v1 = ds.cid_v1 AND ae.aggregate_cid = de.aggregate_cid AND de.status = 'active'
						)
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
		`,
	},
	{
		kind: cargoMetricGauge,
		name: "dagcargo_project_items_undealt_aggregates",
		help: "Count of aggregated items awaiting their first active deal per project",
		query: `
			WITH
				active_sources AS (
					SELECT * FROM cargo.sources WHERE weight >= 0 OR weight IS NULL
				),
				q AS (
					SELECT s.project, COUNT(*) val
						FROM active_sources s
						JOIN cargo.dag_sources ds USING ( srcid )
					WHERE
						EXISTS (
							SELECT 42
								FROM cargo.aggregate_entries ae
							WHERE ae.cid_v1 = ds.cid_v1
						)
							AND
						NOT EXISTS (
							SELECT 42
								FROM cargo.aggregate_entries ae
								JOIN cargo.deals de
									ON ae.cid_v1 = ds.cid_v1 AND ae.aggregate_cid = de.aggregate_cid AND de.status = 'active'
						)
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
		`,
	},
	{
		kind: cargoMetricGauge,
		name: "dagcargo_project_items_unaggregated",
		help: "Count of items pending initial aggregate inclusion per project",
		query: fmt.Sprintf(
			`
			WITH
				active_sources AS (
					SELECT * FROM cargo.sources WHERE weight >= 0 OR weight IS NULL
				),
				q AS (
					SELECT s.project, COUNT(*) val
						FROM active_sources s
						JOIN cargo.dag_sources ds USING ( srcid )
						JOIN cargo.dags d USING ( cid_v1 )
						LEFT JOIN cargo.aggregate_entries ae USING ( cid_v1 )
					WHERE
						( d.size_actual IS NOT NULL AND d.size_actual <= %[1]d )
							AND
						ds.entry_removed IS NULL
							AND
						ae.cid_v1 IS NULL
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
			`,
			targetMaxSize,
		),
	},
	{
		kind: cargoMetricGauge,
		name: "dagcargo_project_bytes_unaggregated",
		help: "Amount of per-DAG-deduplicated bytes pending initial aggregate inclusion per project",
		query: fmt.Sprintf(
			`
			WITH
				active_sources AS (
					SELECT * FROM cargo.sources WHERE weight >= 0 OR weight IS NULL
				),
				q AS (
					SELECT s.project, SUM(size_actual) val
						FROM active_sources s
						JOIN cargo.dag_sources ds USING ( srcid )
						JOIN cargo.dags d USING ( cid_v1 )
						LEFT JOIN cargo.aggregate_entries ae USING ( cid_v1 )
					WHERE
						( d.size_actual IS NOT NULL AND d.size_actual <= %[1]d )
							AND
						ds.entry_removed IS NULL
							AND
						ae.cid_v1 IS NULL
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
			`,
			targetMaxSize,
		),
	},
	{
		kind: cargoMetricGauge,
		name: "dagcargo_project_bytes_unaggregated_deduplicated_eligible",
		help: "Amount of best-effort-deduplicated bytes pending initial aggregate inclusion per project",
		query: fmt.Sprintf(
			`
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN (
					SELECT project, SUM(size_actual) AS val
						FROM ( %s ) e
					GROUP BY project
				) q USING ( project )
			`,
			eligibleForAggregationSQL(targetMaxSize, defaultAggregateSettleDelayHours),
		),
	},
}

// add some templated velocity-window metrics
func init() {
	for _, pct := range []int{50, 95} {
		for _, days := range []int{1, 7} {

			metricsList = append(metricsList, cargoMetric{
				kind: cargoMetricGauge,
				name: fmt.Sprintf("dagcargo_project_item_minutes_to_aggregate_%dday_%dpct", days, pct),
				help: fmt.Sprintf("%d percentile minutes to first aggregate inclusion for entries added in the past %d days", pct, days),
				query: fmt.Sprintf(
					`
					WITH
						active_sources AS (
							SELECT * FROM cargo.sources WHERE weight >= 0 OR weight IS NULL
						),
						q AS (
							SELECT
									s.project,
									EXTRACT(EPOCH FROM
										PERCENTILE_CONT(0.%d) WITHIN GROUP ( ORDER BY
											(
												SELECT MIN( a.entry_created )
													FROM cargo.aggregates a
													JOIN cargo.aggregate_entries ae
														ON a.aggregate_cid = ae.aggregate_cid AND ae.cid_v1 = ds.cid_v1
											) - ds.entry_created
										)
									)::INTEGER / 60 AS val
								FROM active_sources s
								JOIN cargo.dag_sources ds USING ( srcid )
							WHERE
								ds.entry_created > NOW() - '%d day'::INTERVAL
									AND
								-- if an entry came after aggregation - all bets/timings are off
								NOT EXISTS (
									SELECT 42
										FROM cargo.aggregate_entries oae, cargo.aggregates oa
									WHERE
										oae.aggregate_cid = oa.aggregate_cid
											AND
										oae.cid_v1 = ds.cid_v1
											AND
										oa.entry_created <= ds.entry_created
								)
							GROUP BY s.project
						)
					SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
						FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
						LEFT JOIN q USING ( project )
					`,
					pct,
					days,
				),
			})

			metricsList = append(metricsList, cargoMetric{
				kind: cargoMetricGauge,
				name: fmt.Sprintf("dagcargo_project_item_minutes_to_deal_published_%dday_%dpct", days, pct),
				help: fmt.Sprintf("%d percentile minutes to first published deal for entries added in the past %d days", pct, days),
				query: fmt.Sprintf(
					`
					WITH
						active_sources AS (
							SELECT * FROM cargo.sources WHERE weight >= 0 OR weight IS NULL
						),
						q AS (
							SELECT
									s.project,
									EXTRACT(EPOCH FROM
										PERCENTILE_CONT(0.%d) WITHIN GROUP ( ORDER BY
											(
												SELECT MIN( dev.entry_created )
													FROM cargo.aggregate_entries ae
													JOIN cargo.deals de
														ON ae.aggregate_cid = de.aggregate_cid AND ae.cid_v1 = ds.cid_v1
													JOIN cargo.deal_events dev
														ON de.deal_id = dev.deal_id AND dev.status = 'published'
											) - ds.entry_created
										)
									)::INTEGER / 60 AS val
								FROM active_sources s
								JOIN cargo.dag_sources ds USING ( srcid )
							WHERE
								ds.entry_created > NOW() - '%d day'::INTERVAL
									AND
								-- if an entry came after aggregation - all bets/timings are off
								NOT EXISTS (
									SELECT 42
										FROM cargo.aggregate_entries oae, cargo.aggregates oa
									WHERE
										oae.aggregate_cid = oa.aggregate_cid
											AND
										oae.cid_v1 = ds.cid_v1
											AND
										oa.entry_created <= ds.entry_created
								)
							GROUP BY s.project
						)
					SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
						FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
						LEFT JOIN q USING ( project )
					`,
					pct,
					days,
				),
			})

			metricsList = append(metricsList, cargoMetric{
				kind: cargoMetricGauge,
				name: fmt.Sprintf("dagcargo_project_item_minutes_to_deal_active_%dday_%dpct", days, pct),
				help: fmt.Sprintf("%d percentile minutes to first active deal for entries added in the past %d days", pct, days),
				query: fmt.Sprintf(
					`
					WITH
						active_sources AS (
							SELECT * FROM cargo.sources WHERE weight >= 0 OR weight IS NULL
						),
						q AS (
							SELECT
									s.project,
									EXTRACT(EPOCH FROM
										PERCENTILE_CONT(0.%d) WITHIN GROUP ( ORDER BY
											(
												SELECT MIN( dev.entry_created )
													FROM cargo.aggregate_entries ae
													JOIN cargo.deals de
														ON ae.aggregate_cid = de.aggregate_cid AND ae.cid_v1 = ds.cid_v1
													JOIN cargo.deal_events dev
														ON de.deal_id = dev.deal_id AND dev.status = 'active'
											) - ds.entry_created
										)
									)::INTEGER / 60 AS val
								FROM active_sources s
								JOIN cargo.dag_sources ds USING ( srcid )
							WHERE
								ds.entry_created > NOW() - '%d day'::INTERVAL
									AND
								-- if an entry came after aggregation - all bets/timings are off
								NOT EXISTS (
									SELECT 42
										FROM cargo.aggregate_entries oae, cargo.aggregates oa
									WHERE
										oae.aggregate_cid = oa.aggregate_cid
											AND
										oae.cid_v1 = ds.cid_v1
											AND
										oa.entry_created <= ds.entry_created
								)
							GROUP BY s.project
						)
					SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
						FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
						LEFT JOIN q USING ( project )
					`,
					pct,
					days,
				),
			})
		}
	}
}

func pushPrometheusMetrics(cctx *cli.Context) error {

	var countPromCounters, countPromGauges int
	defer func() {
		log.Infow("prometheus push completed",
			"counterMetrics", countPromCounters,
			"gaugeMetrics", countPromGauges,
			"projects", len(projects),
		)
	}()

	jobQueue := make(chan cargoMetric, len(metricsList))
	for _, m := range metricsList {
		jobQueue <- m
	}
	close(jobQueue)

	var mu sync.Mutex
	prom := prometheuspush.New(promURL, "dagcargo").BasicAuth(promUser, promPass)

	doneCh := make(chan struct{}, workerCount)
	var firstErrorSeen error

	for i := 0; i < workerCount; i++ {
		go func() {
			defer func() { doneCh <- struct{}{} }()

			for {
				m, chanOpen := <-jobQueue
				if !chanOpen {
					return
				}

				cols, err := gatherMetric(cctx.Context, m)

				mu.Lock()

				if err != nil {
					log.Errorf("failed gathering data for %s, continuing nevertheless: %s ", m.name, err)
					if firstErrorSeen == nil {
						firstErrorSeen = err
					}
				} else {
					if m.kind == cargoMetricCounter {
						countPromCounters += len(cols)
					} else if m.kind == cargoMetricGauge {
						countPromGauges += len(cols)
					}
					for _, c := range cols {
						prom.Collector(c)
					}
				}

				mu.Unlock()
			}
		}()
	}

	for workerCount > 0 {
		<-doneCh
		workerCount--
	}

	err := prom.Push()
	if err != nil {
		return err
	}

	return firstErrorSeen
}

func gatherMetric(ctx context.Context, m cargoMetric) ([]prometheus.Collector, error) {

	t0 := time.Now()
	tx, err := db.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly, IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck

	rows, err := tx.Query(ctx, m.query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fd := rows.FieldDescriptions()
	if len(fd) < 1 || len(fd) > 2 {
		return nil, xerrors.Errorf("unexpected %d columns in resultset", len(fd))
	}

	res := make(map[string]float64)

	if len(fd) == 1 {

		if !rows.Next() {
			return nil, xerrors.New("zero rows in result")
		}

		var val int64
		if err := rows.Scan(&val); err != nil {
			return nil, err
		}

		if rows.Next() {
			return nil, xerrors.New("unexpectedly received more than one result")
		}

		res[""] = float64(val)

	} else {

		var group string
		var val int64
		for rows.Next() {

			if err := rows.Scan(&group, &val); err != nil {
				return nil, err
			}

			res[group] = float64(val)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	took := time.Since(t0).Truncate(time.Millisecond).Seconds()

	cl := make([]prometheus.Collector, 0)
	for g, v := range res {
		var label prometheus.Labels
		if g != "" {
			gType := string(fd[0].Name)
			if gType == "project" && projects[g] != "" {
				g = projects[g]
			}
			label = prometheus.Labels{gType: g}
		}

		if m.kind == cargoMetricCounter {
			log.Infow("evaluatedCounter", "name", m.name, "label", label, "value", v, "tookSeconds", took)
			c := prometheus.NewCounter(prometheus.CounterOpts{Name: m.name, Help: m.help, ConstLabels: label})
			c.Add(v)
			cl = append(cl, c)
		} else if m.kind == cargoMetricGauge {
			log.Infow("evaluatedGauge", "name", m.name, "label", label, "value", v, "tookSeconds", took)
			c := prometheus.NewGauge(prometheus.GaugeOpts{Name: m.name, Help: m.help, ConstLabels: label})
			c.Set(v)
			cl = append(cl, c)
		} else {
			return nil, xerrors.Errorf("unknown metric kind '%s'", m.kind)
		}
	}

	return cl, nil
}
