#!/bin/bash

# Get list of pending v1 cids
cids_pending="$(
  psql -AtF, service=cargo <<<"SELECT cid_v1 FROM cargo.dags WHERE size_actual IS NULL AND entry_created BETWEEN (NOW()-'30 days'::INTERVAL) AND (NOW()-'1 hours'::INTERVAL)" \
  | sort -R
)"

# Force-connect to first random 8k of anything claiming to have our stuff
echo "$cids_pending" \
| head -n 8192 \
| xargs -P 1024 -n1 timeout -k 1 3 $HOME/go-ipfs/cmd/ipfs/ipfs dht findprovs 2>/dev/null \
| sort -u \
| sed 's/^/\/p2p\//' \
| xargs -P 256 -n1 timeout 5 $HOME/go-ipfs/cmd/ipfs/ipfs swarm connect &>/dev/null

# Now try to gather anything that could have been missed
echo "$cids_pending" \
| xargs -P 1024 -n1 $HOME/go-ipfs/cmd/ipfs/ipfs pin add 2>/dev/null \
| grep 'pinned'
