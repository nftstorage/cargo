#!/bin/bash

set -e
set -o pipefail

atcat="$( dirname "${BASH_SOURCE[0]}" )/atomic_cat.bash"
csvout="$HOME/STATUS/usage-summary/per-source.csv"
txtout="$HOME/STATUS/usage-summary/per-source.txt"

psql service=cargo -c "COPY (
  SELECT
    CASE
      WHEN source_ghkey IS NOT NULL
        THEN 'https://edent.github.io/github_id/#' || source_ghkey
      ELSE
        NULL
    END AS github_url,
    *
  FROM cargo.dags_processed_summary
  ORDER BY GiB_total DESC, most_recent_upload DESC
) TO STDOUT ( FORMAT CSV, QUOTE '\"', HEADER )" \
| tee >( "$atcat" "$csvout" ) \
| column -t -s, -n \
| "$atcat" "$txtout"
