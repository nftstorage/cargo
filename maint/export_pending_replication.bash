#!/bin/bash

set -e
set -o pipefail

tmpfile=$( mktemp "$HOME/STATUS/.pending_replication.json.XXXXXX" )
trap 'rm -f -- "$tmpfile"' INT TERM HUP EXIT
chmod 644 "$tmpfile"

psql -At service=cargo -c "
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
> "$tmpfile"

mv "$tmpfile" "$HOME/STATUS/pending_replication.json"
