#!/bin/bash

# get list of pending v1 cids, duplicate v1 as v0 where possible
cids_pending="$(
  psql -AtF, -U nft postgres <<<"SELECT cid_v1 FROM cargo.dags WHERE size_actual IS NULL AND entry_created BETWEEN (NOW()-'30 days'::INTERVAL) AND (NOW()-'1 hours'::INTERVAL)" \
  | tee >( xargs -n1 -P32 $HOME/go-ipfs/cmd/ipfs/ipfs cid format -v0 2>/dev/null ) \
  | sort -R
)"

# Connect to anything claiming to have our stuff
echo "$cids_pending" \
| xargs -P1024 -n1 timeout 30 $HOME/go-ipfs/cmd/ipfs/ipfs dht findprovs \
| sort -u \
| sed 's/^/\/p2p\//' \
| xargs -P 256 -n1 $HOME/go-ipfs/cmd/ipfs/ipfs swarm connect &>/dev/null

# try to gather anything that could have been missed
echo "$cids_pending" \
| xargs -P1024 -n1 $HOME/go-ipfs/cmd/ipfs/ipfs pin add 2>/dev/null \
| grep 'pinned'
