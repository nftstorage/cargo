#!/bin/bash

set -eu
set -o pipefail

export SWEEP_MOST_AGE="${SWEEP_MOST_AGE:-30 days}"
export SWEEP_LEAST_AGE="${SWEEP_LEAST_AGE:-0 days}"
export SWEEP_EXTRA_COND="${SWEEP_EXTRA_COND:-}"
export SWEEP_TIMEOUT_SEC="${SWEEP_TIMEOUT_SEC:-900}"
export SWEEP_CONCURRENCY="${SWEEP_CONCURRENCY:-256}"

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

say() { echo -e "\n$(date -u): $@"; }

current_pins="$( curl -sXPOST "$SWEEP_IPFSAPI/api/v0/pin/ls?type=recursive" | jq -r '.Keys | to_entries | .[] | .key' | sort -u )"
[[ -z "$current_pins" ]] && say "IPFS pin ls returned an empty set" && exit 0

# Get a shuffled list of pending cids, excluding anything reported already pinned by daemon
cids_pending="$(
  comm -13 \
    <( echo "$current_pins" ) \
    <( psql -AtF, service=cargo <<<"
        SELECT DISTINCT( cid_v1 )
          FROM cargo.dags_missing_list
        WHERE
          entry_created BETWEEN (NOW()-'$SWEEP_MOST_AGE'::INTERVAL) AND (NOW()-'$SWEEP_LEAST_AGE'::INTERVAL)
            AND
          ( ${SWEEP_EXTRA_COND:-TRUE} )
        ORDER BY cid_v1
      " ) \
  | sort -R
)"

pending_count="$( wc -w <<<"$cids_pending" )"

rundesc="[[ SWEEP_MOST_AGE:'$SWEEP_MOST_AGE' SWEEP_LEAST_AGE:'$SWEEP_LEAST_AGE' SWEEP_TIMEOUT_SEC:'$SWEEP_TIMEOUT_SEC' SWEEP_CONCURRENCY:'$SWEEP_CONCURRENCY' SWEEP_EXTRA_COND:\"$SWEEP_EXTRA_COND\" ]]"

[[ "$pending_count" == 0 ]] && say "Nothing to do $rundesc" && exit 0

say "Attempting sweep of $pending_count DAGs $rundesc"

exec 3>&1
pin_count="$(
 <<<"$cids_pending" xargs -P $SWEEP_CONCURRENCY -n1 -I{} bash -c "curl -m$(( $SWEEP_TIMEOUT_SEC + 5 )) -sXPOST '$SWEEP_IPFSAPI/api/v0/pin/add?progress=false&timeout=${SWEEP_TIMEOUT_SEC}s&arg={}' | jq -r 'try .Pins[]'" \
 | tee >( perl -pe '$|=1; s/.*/./s' >&3 ) \
 | wc -w
)"

say "Attempt finished, pinned $pin_count out of $pending_count dags $rundesc"
