#!/bin/bash

set -e
set -o pipefail

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
| "$( dirname "${BASH_SOURCE[0]}" )/atomic_cat.bash" "$HOME/STATUS/pending_replication.json"
