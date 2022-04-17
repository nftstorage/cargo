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
	heavy bool
}

var workerCount = 24
var onlyHeavy bool
var metricDbTimeout = 30 * time.Minute
var heavyMetricDbTimeout = 70 * time.Minute

var pushMetrics = &cli.Command{
	Usage:  "Push service metrics to external collectors",
	Name:   "push-metrics",
	Flags:  []cli.Flag{},
	Action: pushPrometheusMetrics,
}

var pushHeavyMetrics = &cli.Command{
	Usage: "Push only the exceptionally heavy/infrequent metrics to external collectors",
	Name:  "push-heavy-metrics",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		onlyHeavy = true
		return pushPrometheusMetrics(cctx)
	},
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
		help: "Count of sources/users that have used the service to store some data",
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
		help: "Count of reported-pinned items pending retrieval from IPFS per project",
		query: `
			WITH
				active_sources AS (
					SELECT * FROM cargo.sources WHERE weight > 0 OR weight IS NULL
				),
				q AS (
					SELECT s.project, COUNT(*) val
						FROM active_sources s
						JOIN cargo.dag_sources ds USING ( srcid )
						JOIN cargo.dags d USING ( cid_v1 )
					WHERE
						d.size_actual IS NULL
							AND
						ds.details -> 'pin_reported_at' IS NOT NULL
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
		`,
	},
	{
		heavy: true, // not really heavy but should accompany other metrics that *are*
		kind:  cargoMetricGauge,
		name:  "dagcargo_project_stored_items_oversized",
		help:  "Count of items larger than a 32GiB sector not marked for deletion",
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
		heavy: true, // not really heavy but should accompany other metrics that *are*
		kind:  cargoMetricGauge,
		name:  "dagcargo_project_stored_items_inactive",
		help:  "Count of items exclusively from inactive sources",
		query: `
			WITH
				inactive_sources AS (
					SELECT * FROM cargo.sources WHERE weight < 0
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
			SELECT p.project::TEXT, q.val
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
		heavy: true,
		kind:  cargoMetricGauge,
		name:  "dagcargo_project_stored_bytes_active_deduplicated",
		help:  "Amount of known best-effort-deduplicated bytes stored per project",
		query: `
			WITH
				q AS (

					SELECT s.project, d.cid_v1, d.size_actual
						FROM cargo.dag_sources ds
						JOIN cargo.dags d USING ( cid_v1 )
						JOIN cargo.sources s USING ( srcid )
					WHERE
						( s.weight >= 0 OR s.weight IS NULL )
							AND
						d.size_actual IS NOT NULL
							AND
						ds.entry_removed IS NULL

				-- ensure we are not a part of something else active from *same project*
				EXCEPT

					SELECT s.project, d.cid_v1, d.size_actual
						FROM cargo.dag_sources ds
						JOIN cargo.dags d USING ( cid_v1 )
						JOIN cargo.sources s USING ( srcid )
						JOIN cargo.refs r
							ON ds.cid_v1 = r.ref_cid
						JOIN cargo.dag_sources rds
							ON r.cid_v1 = rds.cid_v1
						JOIN cargo.sources rs
							ON rds.srcid = rs.srcid
					WHERE
						d.size_actual IS NOT NULL
							AND
						ds.entry_removed IS NULL
							AND
						rds.entry_removed IS NULL
							AND
						( rs.weight >= 0 OR rs.weight IS NULL )
							AND
						rs.project = s.project
				)
			SELECT p.project::TEXT, COALESCE( SUM( q.size_actual), 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
			GROUP BY p.project
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
		heavy: true,
		kind:  cargoMetricGauge,
		name:  "dagcargo_project_stored_bytes_deleted_deduplicated",
		help:  "Amount of known best-effort-deduplicated bytes retrieved and then marked deleted per project",
		query: `
			WITH
				q AS (
					SELECT s.project, d.cid_v1, d.size_actual
						FROM cargo.sources s
						JOIN cargo.dag_sources ds USING ( srcid )
						JOIN cargo.dags d USING ( cid_v1 )
					WHERE
						( s.weight >= 0 OR s.weight IS NULL )
							AND
						d.size_actual IS NOT NULL
							AND
						ds.entry_removed IS NOT NULL
							AND
						-- ensure there isn't another active entry for the same dag
						NOT EXISTS (
							SELECT 42
								FROM cargo.dag_sources actds, cargo.sources acts
							WHERE
								actds.cid_v1 = ds.cid_v1
									AND
								actds.entry_removed IS NULL
									AND
								actds.srcid = acts.srcid
									AND
								( acts.weight >= 0 OR acts.weight IS NULL )
						)

				-- ensure we are not a part of something else active from *same project*
				EXCEPT

					SELECT s.project, d.cid_v1, d.size_actual
						FROM cargo.sources s
						JOIN cargo.dag_sources ds USING ( srcid )
						JOIN cargo.dags d USING ( cid_v1 )
						JOIN cargo.refs r
							ON d.cid_V1 = r.ref_cid
						JOIN cargo.dag_sources rds
							ON r.cid_v1 = rds.cid_v1
						JOIN cargo.sources rs
							ON rds.srcid = rs.srcid
					WHERE
						( s.weight >= 0 OR s.weight IS NULL )
							AND
						d.size_actual IS NOT NULL
							AND
						ds.entry_removed IS NOT NULL
							AND
						( rs.weight >= 0 OR rs.weight IS NULL )
							AND
						rds.entry_removed IS NULL
							AND
						rs.project = s.project
				)
			SELECT p.project::TEXT, COALESCE( SUM( q.size_actual), 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
			GROUP BY p.project
		`,
	},

	{
		heavy: true, // not really heavy but should accompany other metrics that *are*
		kind:  cargoMetricGauge,
		name:  "dagcargo_project_stored_bytes_oversized_deduplicated",
		help:  "Amount of bytes in dags larger than a 32GiB sector not marked for deletion",
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
						-- ensure we are not a part of something else active from *same project*
						NOT EXISTS (
							SELECT 42
								FROM cargo.refs r, cargo.dag_sources rds, cargo.sources rs
							WHERE
								r.ref_cid = d.cid_v1
									AND
								r.cid_v1 = rds.cid_v1
									AND
								rds.entry_removed IS NULL
									AND
								rds.srcid = rs.srcid
									AND
								( rs.weight >= 0 OR rs.weight IS NULL )
									AND
								rs.project = s.project
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
		heavy: true, // not really heavy but should accompany other metrics that *are*
		kind:  cargoMetricGauge,
		name:  "dagcargo_project_stored_bytes_inactive_deduplicated",
		help:  "Amount of bytes in dags exclusively from inactive sources",
		query: `
			WITH
				inactive_sources AS (
					SELECT * FROM cargo.sources WHERE weight < 0
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
			SELECT p.project::TEXT, q.val
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
		heavy: true,
		kind:  cargoMetricCounter,
		name:  "dagcargo_handled_total_bytes",
		help:  "How many best-effort-deduplicated bytes did the service handle since inception",
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
		heavy: true, // FIXME - use some sort of rollup... too heavy even for heavy
		kind:  cargoMetricCounter,
		name:  "dagcargo_handled_total_blocks",
		help:  "How many unique-by-cid blocks did the service handle since inception",
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
		heavy: true,
		kind:  cargoMetricGauge,
		name:  "dagcargo_project_items_in_active_deals",
		help:  "Count of aggregated items with at least one active deal per project",
		query: `
			WITH
				q AS (
					SELECT s.project, COUNT(*) val
						FROM cargo.sources s
						JOIN cargo.dag_sources ds USING ( srcid )
						JOIN cargo.dags d USING ( cid_v1 )
					WHERE
						d.size_actual IS NOT NULL
							AND
						EXISTS (
							SELECT 42
								FROM cargo.aggregate_entries ae, cargo.deals de
							WHERE
								ae.cid_v1 = ds.cid_v1
									AND
								ae.aggregate_cid = de.aggregate_cid
									AND
								de.status = 'active'
						)
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
		`,
	},
	{
		heavy: true,
		kind:  cargoMetricGauge,
		name:  "dagcargo_project_bytes_in_active_deals",
		help:  "Amount of per-DAG-deduplicated bytes with at least one active deal per project",
		query: `
			WITH
				q AS (
					SELECT s.project, SUM(d.size_actual) val
						FROM cargo.sources s
						JOIN cargo.dag_sources ds USING ( srcid )
						JOIN cargo.dags d USING ( cid_v1 )
					WHERE
						d.size_actual IS NOT NULL
							AND
						EXISTS (
							SELECT 42
								FROM cargo.aggregate_entries ae, cargo.deals de
							WHERE
								ae.cid_v1 = ds.cid_v1
									AND
								ae.aggregate_cid = de.aggregate_cid
									AND
								de.status = 'active'
						)
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
		`,
	},
	{
		heavy: true, // FIXME - rewire to per-aggregate meta, switch to lite metrics
		kind:  cargoMetricGauge,
		name:  "dagcargo_project_items_undealt_aggregates",
		help:  "Count of aggregated items awaiting their first active deal per project",
		query: `
			WITH
				q AS (
					SELECT s.project, COUNT(*) val
						FROM cargo.sources s
						JOIN cargo.dag_sources ds USING ( srcid )
						JOIN cargo.dags d USING ( cid_v1 )
					WHERE
						d.size_actual IS NOT NULL
							AND
						EXISTS (
							SELECT 42
								FROM cargo.aggregate_entries ae
							WHERE ae.cid_v1 = ds.cid_v1
						)
							AND
						NOT EXISTS (
							SELECT 42
								FROM cargo.aggregate_entries ae, cargo.deals de
							WHERE
								ae.cid_v1 = ds.cid_v1
									AND
								ae.aggregate_cid = de.aggregate_cid
									AND
								de.status = 'active'
						)
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
		`,
	},
	{
		heavy: true, // FIXME - rewire to per-aggregate meta, switch to lite metrics
		kind:  cargoMetricGauge,
		name:  "dagcargo_project_bytes_undealt_aggregates",
		help:  "Amount of per-DAG-deduplicated bytes awaiting their first active deal per project",
		query: `
			WITH
				q AS (
					SELECT s.project, SUM(d.size_actual) val
						FROM cargo.sources s
						JOIN cargo.dag_sources ds USING ( srcid )
						JOIN cargo.dags d USING ( cid_v1 )
					WHERE
						d.size_actual IS NOT NULL
							AND
						EXISTS (
							SELECT 42
								FROM cargo.aggregate_entries ae
							WHERE ae.cid_v1 = ds.cid_v1
						)
							AND
						NOT EXISTS (
							SELECT 42
								FROM cargo.aggregate_entries ae, cargo.deals de
							WHERE
								ae.cid_v1 = ds.cid_v1
									AND
								ae.aggregate_cid = de.aggregate_cid
									AND
								de.status = 'active'
						)
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
		`,
	},
	{
		heavy: true, // FIXME - rewire to per-aggregate meta, switch to lite metrics
		kind:  cargoMetricGauge,
		name:  "dagcargo_project_bytes_undealt_aggregates_deduplicated",
		help:  "Best-effort-deduplicated bytes awaiting their first active deal per project",
		query: `
			WITH
				q AS (
					SELECT s.project, SUM( d.size_actual ) AS val
						FROM cargo.sources s
						JOIN cargo.dag_sources ds USING ( srcid )
						JOIN cargo.dags d USING ( cid_v1 )
					WHERE
						d.size_actual IS NOT NULL
							AND
						EXISTS (
							SELECT 42
								FROM cargo.aggregate_entries ae
							WHERE ae.cid_v1 = ds.cid_v1
						)
							AND
						NOT EXISTS (
							SELECT 42
								FROM cargo.aggregate_entries ae, cargo.deals de
							WHERE
								ae.cid_v1 = ds.cid_v1
									AND
								ae.aggregate_cid = de.aggregate_cid
									AND
								de.status = 'active'
						)
							AND
						-- ensure we are not a part of something else aggregated
						NOT EXISTS (
							SELECT 42
								FROM cargo.refs r, cargo.aggregate_entries rae
							WHERE
								d.cid_v1 = r.ref_cid
									AND
								r.cid_v1 = rae.cid_v1
						)
					GROUP BY s.project
				)
			SELECT p.project::TEXT, COALESCE( q.val, 0 ) AS val
				FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
				LEFT JOIN q USING ( project )
			`,
	},
	{
		heavy: true, // FIXME - not heavy, has to go with undealt_aggregates above
		kind:  cargoMetricGauge,
		name:  "dagcargo_project_items_unaggregated",
		help:  "Count of items pending initial aggregate inclusion per project",
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
		heavy: true, // FIXME - not heavy, has to go with undealt_aggregates above
		kind:  cargoMetricGauge,
		name:  "dagcargo_project_bytes_unaggregated",
		help:  "Amount of per-DAG-deduplicated bytes pending initial aggregate inclusion per project",
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
					SELECT project, SUM( size_actual ) AS val
						FROM (
							SELECT DISTINCT project, cid_v1, size_actual
								FROM ( %s ) eligible
						) distinct_eligible
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
				heavy: true,
				kind:  cargoMetricGauge,
				name:  fmt.Sprintf("dagcargo_project_item_minutes_to_aggregate_%dday_%dpct", days, pct),
				help:  fmt.Sprintf("%d percentile minutes to first aggregate inclusion for entries added in the past %d days", pct, days),
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
													FROM cargo.aggregate_entries ae, cargo.aggregates a
												WHERE
													ae.cid_v1 = ds.cid_v1
														AND
													a.aggregate_cid = ae.aggregate_cid
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
					SELECT p.project::TEXT, q.val
						FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
						LEFT JOIN q USING ( project )
					`,
					pct,
					days,
				),
			})

			metricsList = append(metricsList, cargoMetric{
				heavy: true,
				kind:  cargoMetricGauge,
				name:  fmt.Sprintf("dagcargo_project_item_minutes_to_deal_published_%dday_%dpct", days, pct),
				help:  fmt.Sprintf("%d percentile minutes to first published deal for entries added in the past %d days", pct, days),
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
													FROM cargo.aggregate_entries ae, cargo.deals de, cargo.deal_events dev
												WHERE
													ae.cid_v1 = ds.cid_v1
														AND
													ae.aggregate_cid = de.aggregate_cid
														AND
													de.deal_id = dev.deal_id
														AND
													dev.status = 'published'
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
					SELECT p.project::TEXT, q.val
						FROM ( SELECT DISTINCT( project ) FROM cargo.sources ) p
						LEFT JOIN q USING ( project )
					`,
					pct,
					days,
				),
			})

			metricsList = append(metricsList, cargoMetric{
				heavy: true,
				kind:  cargoMetricGauge,
				name:  fmt.Sprintf("dagcargo_project_item_minutes_to_deal_active_%dday_%dpct", days, pct),
				help:  fmt.Sprintf("%d percentile minutes to first active deal for entries added in the past %d days", pct, days),
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
													FROM cargo.aggregate_entries ae, cargo.deals de, cargo.deal_events dev
												WHERE
													ae.cid_v1 = ds.cid_v1
														AND
													ae.aggregate_cid = de.aggregate_cid
														AND
													de.deal_id = dev.deal_id
														AND
													dev.status = 'active'
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
					SELECT p.project::TEXT, q.val
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
		if onlyHeavy == m.heavy {
			jobQueue <- m
		}
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

				cols, err := gatherMetric(cctx, m)

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

func gatherMetric(cctx *cli.Context, m cargoMetric) ([]prometheus.Collector, error) {

	ctx := cctx.Context
	t0 := time.Now()

	var statTx pgx.Tx
	defer func() {
		if statTx != nil {
			statTx.Rollback(context.Background()) //nolint:errcheck
		}
	}()

	var msecTOut int64
	if onlyHeavy {
		msecTOut = heavyMetricDbTimeout.Milliseconds()
	} else {
		msecTOut = metricDbTimeout.Milliseconds()
	}

	if statConnStr := cctx.String("cargo-pg-stats-connstring"); statConnStr != "" {
		statDb, err := pgx.Connect(ctx, statConnStr)
		if err != nil {
			return nil, err
		}
		defer statDb.Close(context.Background()) //nolint:errcheck

		// separate db - means we can have a connection-wide timeout
		if _, err = statDb.Exec(ctx, fmt.Sprintf(`SET statement_timeout = %d`, msecTOut)); err != nil {
			return nil, err
		}
		statTx, err = statDb.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		if err != nil {
			return nil, err
		}
	} else {
		var err error
		if statTx, err = cargoDb.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly}); err != nil {
			return nil, err
		}
		// using wider DB - must be tx-local timeout
		if _, err = statTx.Exec(ctx, fmt.Sprintf(`SET LOCAL statement_timeout = %d`, msecTOut)); err != nil {
			return nil, err
		}
	}

	rows, err := statTx.Query(ctx, m.query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fd := rows.FieldDescriptions()
	if len(fd) < 1 || len(fd) > 2 {
		return nil, xerrors.Errorf("unexpected %d columns in resultset", len(fd))
	}

	res := make(map[string]*int64)

	if len(fd) == 1 {

		if !rows.Next() {
			return nil, xerrors.New("zero rows in result")
		}

		var val *int64
		if err := rows.Scan(&val); err != nil {
			return nil, err
		}

		if rows.Next() {
			return nil, xerrors.New("unexpectedly received more than one result")
		}
		res[""] = val

	} else {

		for rows.Next() {
			var group string
			var val *int64
			if err := rows.Scan(&group, &val); err != nil {
				return nil, err
			}
			res[group] = val
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	took := time.Since(t0).Truncate(time.Millisecond).Seconds()

	statTx.Rollback(context.Background()) //nolint:errcheck
	statTx = nil

	cl := make([]prometheus.Collector, 0)
	for g, v := range res {

		dims := make([][2]string, 0)
		var label prometheus.Labels
		if g != "" {
			gType := string(fd[0].Name)
			if gType == "project" && projects[g] != "" {
				g = projects[g]
			}
			label = prometheus.Labels{gType: g}
			dims = append(dims, [2]string{gType, g})
		}

		if m.kind == cargoMetricCounter {
			log.Infow("evaluatedCounter", "name", m.name, "label", label, "value", v, "tookSeconds", took)
			if v != nil {
				c := prometheus.NewCounter(prometheus.CounterOpts{Name: m.name, Help: m.help, ConstLabels: label})
				c.Add(float64(*v))
				cl = append(cl, c)
			}
		} else if m.kind == cargoMetricGauge {
			log.Infow("evaluatedGauge", "name", m.name, "label", label, "value", v, "tookSeconds", took)
			if v != nil {
				c := prometheus.NewGauge(prometheus.GaugeOpts{Name: m.name, Help: m.help, ConstLabels: label})
				c.Set(float64(*v))
				cl = append(cl, c)
			}
		} else {
			return nil, xerrors.Errorf("unknown metric kind '%s'", m.kind)
		}

		_, err = cargoDb.Exec(
			ctx,
			`
			INSERT INTO metrics
					( name, dimensions, description, value, collection_took_seconds )
				VALUES
					( $1, $2, $3, $4, $5 )
				ON CONFLICT ( name, dimensions ) DO UPDATE SET
					description = EXCLUDED.description,
					value = EXCLUDED.value,
					collection_took_seconds = EXCLUDED.collection_took_seconds,
					collected_at = CLOCK_TIMESTAMP()
			`,
			m.name,
			dims,
			m.help,
			v,
			took,
		)
		if err != nil {
			return nil, err
		}
	}

	return cl, nil
}
