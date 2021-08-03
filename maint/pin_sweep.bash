#!/bin/bash

export IPFSAPI="http://127.0.0.1:5001/api/v0"

# Get list of pending cids ( if we have pinned it already - exclude )
cids_pending="$(
  comm -23 \
    <( psql -AtF, service=cargo <<<"
        SELECT cid_v1 FROM cargo.dags d WHERE
          size_actual IS NULL
            AND
          entry_created BETWEEN (NOW()-'30 days'::INTERVAL) AND (NOW()-'1 hours'::INTERVAL)
            AND
          EXISTS ( SELECT 42 FROM cargo.dag_sources ds WHERE d.cid_v1 = ds.cid_v1 AND ds.entry_removed IS NULL )
         --   AND
         -- EXISTS (
         --   SELECT 42
         --     FROM cargo.dag_sources ds JOIN cargo.sources s USING ( srcid )
         --   WHERE
         --     d.cid_v1 = ds.cid_v1
         --       AND
         --     ( s.weight IS NULL OR s.weight >= 0 )
         -- )
      " | sort
    ) \
    <( curl -sXPOST "$IPFSAPI/pin/ls?type=recursive" | jq -r '.Keys | to_entries | .[] | .key' | sort ) \
  | sort -R
)"

# Force-connect to first random 8k of anything claiming to have our stuff
echo "$cids_pending" \
| head -n 8192 \
| xargs -P 1024 -n1 -I{} bash -c \
  'curl -m7 -sXPOST "$IPFSAPI/dht/findprovs?numproviders=7&verbose=false&arg={}" | jq -r "select(.Type == 4) | .Responses | .[] | .ID"' \
| sort -u \
| sed 's/^/\/p2p\//' \
| xargs -P 256 -n1 -I{} curl -m35 -sXPOST "$IPFSAPI/swarm/connect?arg={}" >/dev/null

# Now try to gather anything that could have been missed, without a timeout ( timeout comes from caller )
echo "$cids_pending" \
| xargs -P 1024 -n1 -I{} bash -c \
  'curl -sXPOST "$IPFSAPI/pin/add?arg={}" | jq -r "try .Pins[]"'
