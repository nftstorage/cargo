#!/bin/bash

set -eu
set -o pipefail

export SWEEP_MOST_AGE="${SWEEP_MOST_AGE:-30 days}"
export SWEEP_LEAST_AGE="${SWEEP_LEAST_AGE:-15 minutes}"
export SWEEP_EXTRA_COND="${SWEEP_EXTRA_COND:-}"
export SWEEP_TIMEOUT_SEC="${SWEEP_TIMEOUT_SEC:-300}"
export SWEEP_CONCURRENCY="${SWEEP_CONCURRENCY:-1024}"

export SWEEP_IPFSAPI="${SWEEP_IPFSAPI:-$( grep "ipfs-api" "$HOME/dagcargo.toml" | cut -d '=' -f2- | sed 's/[ "]*//g' )}"
SWEEP_IPFSAPI="${SWEEP_IPFSAPI:-http://localhost:5001}"

###
### Only execute one version of sweeper with specific options
###
[[ -z "${SWEEP_LOCKFILE:-}" ]] \
&& export SWEEP_LOCKFILE="/dev/shm/pin_sweep_$(
  md5sum <<<"$SWEEP_IPFSAPI $SWEEP_MOST_AGE $SWEEP_LEAST_AGE $SWEEP_EXTRA_COND $SWEEP_TIMEOUT_SEC $SWEEP_CONCURRENCY" \
  | cut -d' ' -f1
).lock" \
&& exec /usr/bin/flock -en "$SWEEP_LOCKFILE" "$0" "$@"
###
###
###

# Get list of pending cids, excluding anything repoted already pinned by daemon
cids_pending="$(
  comm -13 \
    <( curl -sXPOST "$SWEEP_IPFSAPI/api/v0/pin/ls?type=recursive" | jq -r '.Keys | to_entries | .[] | .key' | sort -u ) \
    <( psql -AtF, service=cargo <<<"
        SELECT cid_v1
          FROM cargo.dags_missing_list
        WHERE
          entry_created BETWEEN
            (NOW()-'$SWEEP_MOST_AGE'::INTERVAL)
              AND
            (NOW()-'$SWEEP_LEAST_AGE'::INTERVAL)
          $SWEEP_EXTRA_COND
      " | sort -u
    ) \
  | sort -R
)"


echo "$(date -u): Attempting sweep of $( wc -w <<<"$cids_pending" ) DAGs [[ SWEEP_MOST_AGE:'$SWEEP_MOST_AGE' SWEEP_LEAST_AGE:'$SWEEP_LEAST_AGE' SWEEP_TIMEOUT_SEC:'$SWEEP_TIMEOUT_SEC' SWEEP_CONCURRENCY:'$SWEEP_CONCURRENCY' SWEEP_EXTRA_COND:'$SWEEP_EXTRA_COND' ]]"

exec 3>&1
pin_count="$(
 <<<"$cids_pending" xargs -P $SWEEP_CONCURRENCY -n1 -I{} bash -c "curl -m$SWEEP_TIMEOUT_SEC -sXPOST '$SWEEP_IPFSAPI/api/v0/pin/add?arg={}' | jq -r 'try .Pins[]'" \
 | tee >( perl -pe '$|=1; s/.*/./s' >&3 ) \
 | wc -w
)"

echo -e "\n$(date -u): Attempt finished, resulted in $pin_count new pins"
