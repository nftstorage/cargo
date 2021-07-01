#!/bin/bash

# Runs out of band, attempting to gather anything that could have been missed
psql -AtF, -U nft postgres <<<"SELECT cid_v1 FROM cargo.dags WHERE size_actual IS NULL AND entry_created BETWEEN (NOW()-'30 days'::INTERVAL) AND (NOW()-'1 hours'::INTERVAL)" \
| tee >( xargs -n1 -P32 ipfs cid format -v0 2>/dev/null ) \
| sort -R \
| xargs -P1024 -n1 ipfs pin add 2>/dev/null \
| grep 'pinned'
