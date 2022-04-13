#!/bin/bash

set -eu
set -o pipefail

export pgconn="service=cargo-metrics-heavy"
export wwwdir="$HOME/STATUS"
export atcat="$( dirname "${BASH_SOURCE[0]}" )/atomic_cat.bash"

###
### Only execute one version of exporter concurrently
###
[[ -z "${STAT_EXPORTER_LOCKFILE:-}" ]] \
&& export STAT_EXPORTER_LOCKFILE="/dev/shm/stat_exporter.lock" \
&& exec /usr/bin/flock -en "$STAT_EXPORTER_LOCKFILE" "$0" "$@"
###
###
###

touch $wwwdir/.run_start

ex_per_source_usage() {
  psql $pgconn -c "COPY (
    SELECT
      CASE
        WHEN source_ghkey IS NOT NULL
          THEN 'https://edent.github.io/github_id/#' || source_ghkey
        ELSE
          NULL
      END AS github_url,
      *
    FROM cargo.source_all_time_summary
    ORDER BY GiB_total DESC, most_recent_upload DESC
  ) TO STDOUT ( FORMAT CSV, QUOTE '\"', HEADER )" \
  | tee >( "$atcat" "$wwwdir/usage-summary/per-source.csv" ) \
  | column -t -s, -n \
  | "$atcat" "$wwwdir/usage-summary/per-source.txt"
}
export -f ex_per_source_usage

ex_pending_replication() {
  psql $pgconn -At -c "
    SELECT JSON_BUILD_OBJECT(
      'export_timestamp', NOW(),
      'export_type', 'pending_replication',
      'export_payload', ( SELECT JSONB_AGG(j) FROM (
        SELECT *
          FROM cargo.aggregate_summary
        WHERE tentative_replicas < 5
      ) j )
    )" \
  | jq . \
  | "$atcat" "$wwwdir/pending_replication.json"
}
export -f ex_pending_replication

ex_deal_counts() {
  psql $pgconn -At -c "
    WITH
      per_client AS (
        SELECT client k, JSONB_OBJECT_AGG(status, count) v FROM (
          SELECT client, status, COUNT(*) AS count
            FROM cargo.deals
          WHERE status IN ( 'published', 'active' )
          GROUP BY client, status
          ORDER BY client, status DESC
        ) j
        GROUP BY client
      ),
      per_aggregate AS (
        SELECT aggregate_cid k, JSONB_OBJECT_AGG( client, counts ) v FROM (
          SELECT aggregate_cid, client, JSONB_OBJECT_AGG( status, replicas ) counts FROM (
            SELECT aggregate_cid, client, status, COUNT(*) AS replicas
              FROM cargo.deals
            WHERE status IN ( 'published', 'active' )
            GROUP BY aggregate_cid, client, status
            ORDER BY client, status DESC, aggregate_cid
          ) j
          GROUP BY aggregate_cid, client
        ) j
        GROUP BY aggregate_cid
      )
    SELECT JSON_BUILD_OBJECT(
      'export_timestamp', NOW(),
      'export_type', 'deal_counts',
      'export_payload', JSONB_BUILD_OBJECT(
        'client_totals', ( SELECT JSONB_OBJECT_AGG( k, v ) FROM per_client ),
        'aggregate_totals', ( SELECT JSONB_OBJECT_AGG( k, v ) FROM per_aggregate )
      )
    )" \
  | jq . \
  | "$atcat" "$wwwdir/deal_counts.json"
}
export -f ex_deal_counts

echo ex_per_source_usage ex_pending_replication ex_deal_counts \
| xargs -d ' ' -n1 -P4 -I{} bash -c {}
