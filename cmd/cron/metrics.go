package main

import "fmt"

type cargoMetricType string

var cargoMetricGauge = cargoMetricType("gauge")
var cargoMetricCounter = cargoMetricType("counter")

type cargoMetric struct {
	kind  cargoMetricType
	name  string
	help  string
	query string
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
									ON r.cid_v1 = rds.cid_v1 AND r.ref_v1 = d.cid_v1 AND rds.entry_removed IS NULL
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
									ON r.cid_v1 = rds.cid_v1 AND r.ref_v1 = d.cid_v1 AND rds.entry_removed IS NULL
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
					ON d.cid_v1 = r.ref_v1
			WHERE
				d.size_actual IS NOT NULL
					AND
				r.ref_v1 IS NULL
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
						SELECT DISTINCT( ref_v1 ) FROM cargo.refs
					) d
				)
					+
				(
					SELECT COUNT(*)
						FROM cargo.dags d
						LEFT JOIN cargo.refs r
							ON d.cid_v1 = r.ref_v1
					WHERE
						d.size_actual IS NOT NULL
							AND
						r.ref_v1 IS NULL
				)
		`,
	},

	//
	// deal-related metrics
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
		help: "Count of aggregated items awaiting their first deal per project",
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
