#!/bin/bash

set -e
set -o pipefail

atcat="$( dirname "${BASH_SOURCE[0]}" )/atomic_cat.bash"
csvout="$HOME/STATUS/usage-summary/per-source.csv"
txtout="$HOME/STATUS/usage-summary/per-source.txt"

psql service=cargo -c "COPY ( SELECT * FROM cargo.dags_processed_summary ORDER BY GiB_total DESC ) TO STDOUT ( FORMAT CSV, QUOTE '\"', HEADER )" \
| tee >( "$atcat" "$csvout" ) \
| column -t -s, -n \
| "$atcat" "$txtout"
