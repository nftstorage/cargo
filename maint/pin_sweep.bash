#!/bin/bash

set -eu
set -o pipefail

export LANG=C

export SWEEP_MOST_AGE="${SWEEP_MOST_AGE:-90 days}"
export SWEEP_LEAST_AGE="${SWEEP_LEAST_AGE:-0 days}"
export SWEEP_EXTRA_COND="${SWEEP_EXTRA_COND:-}"
export SWEEP_TIMEOUT_SEC="${SWEEP_TIMEOUT_SEC:-900}"
export SWEEP_CONCURRENCY="${SWEEP_CONCURRENCY:-256}"

export SWEEP_IPFSAPI="${SWEEP_IPFSAPI:-$( grep "ipfs-api" "$HOME/dagcargo.toml" | cut -d '=' -f2- | sed 's/[ "]*//g' )}"
SWEEP_IPFSAPI="${SWEEP_IPFSAPI:-http://localhost:5001}"

say() { echo -e "\n$(date -u): $@"; }

if [[ ! -t 0 ]] && current_requested="$( cat | perl -p -e 's/\s+/\n/g' | sort -u )" && [[ -n "$current_requested" ]]; then

  SWEEP_MOST_AGE="N/A"
  SWEEP_LEAST_AGE="N/A"
  SWEEP_EXTRA_COND="N/A"

  say "Got $( wc -w <<<"$current_requested" ) items on STDIN"

  if [[ -z "${SWEEP_ANY:-}" ]]; then
    current_requested="$( comm -12 \
      <( echo "$current_requested" ) \
      <( psql -AtF, service=cargo <<<"SELECT DISTINCT( cid_v1 ) FROM cargo.dags_missing_list ORDER BY cid_v1" ) \
    )"
  fi

else

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

  current_requested="$( psql -AtF, service=cargo <<<"
    SELECT DISTINCT( cid_v1 )
      FROM cargo.dags_missing_list
    WHERE
      entry_created BETWEEN (NOW()-'$SWEEP_MOST_AGE'::INTERVAL) AND (NOW()-'$SWEEP_LEAST_AGE'::INTERVAL)
        AND
      ( ${SWEEP_EXTRA_COND:-TRUE} )
    ORDER BY cid_v1
  " )"

  say "Got $( wc -w <<<"$current_requested" ) items from RDBMS"

fi

rundesc="[[ SWEEP_TIMEOUT_SEC:'$SWEEP_TIMEOUT_SEC' SWEEP_CONCURRENCY:'$SWEEP_CONCURRENCY' SWEEP_MOST_AGE:'$SWEEP_MOST_AGE' SWEEP_LEAST_AGE:'$SWEEP_LEAST_AGE' SWEEP_EXTRA_COND:\"$SWEEP_EXTRA_COND\" ]]"
[[ "$( wc -w <<<"$current_requested" )" == 0 ]] && say "Nothing to do $rundesc" && exit 0

current_pins="$( curl -sXPOST "$SWEEP_IPFSAPI/api/v0/pin/ls?type=recursive" | jq -r '.Keys | to_entries | .[] | .key' | sort -u )"
[[ -z "$current_pins" ]] && say "IPFS pin ls returned an empty set" && exit 0

# Get a shuffled list of pending cids, excluding anything reported already pinned by daemon
cids_pending="$(
  comm -23 \
    <( echo "$current_requested" ) \
    <( echo "$current_pins" ) \
  | sort -R
)"

pending_count="$( wc -w <<<"$cids_pending" )"
[[ "$pending_count" == 0 ]] && say "Nothing to do $rundesc" && exit 0

pinsdonefn="$(mktemp /dev/shm/.pin_sweep-run.XXXXXX)"
dunnn() { say "Attempt finished, pinned $( <"$pinsdonefn" tr -cd '.' | wc -c ) out of $pending_count dags $rundesc\n"; rm -f "$pinsdonefn"; }
trap 'dunnn' EXIT
say "Attempting sweep of $pending_count DAGs $rundesc"

[[ -t 1 ]] && show_progress="true" || show_progress="false"
<<<"$cids_pending" xargs -P $SWEEP_CONCURRENCY -n1 -I{} bash -c "curl -m$(( $SWEEP_TIMEOUT_SEC + 5 )) -sNXPOST '$SWEEP_IPFSAPI/api/v0/pin/add?progress=${show_progress}&timeout=${SWEEP_TIMEOUT_SEC}s&arg={}' | perl -pe '$|++; s/\$/ \$\$/'" \
 | perl -pe '
   BEGIN { ($|,@anim)=(1,qw(- \ | /)) }
   s/^\{\}.*//s
    or
   s/^\{"Pins".*/".$anim[($ac++)%($#anim+1)]\x08"/se
    or
   s/^\{"Progress"(.*)/ $seen{$1}++ ? "" : "$anim[($ac++)%($#anim+1)]\x08" /se
    or
   s/^.*"Type":"error".*/"x$anim[($ac++)%($#anim+1)]\x08"/se
    or
   s/^.*/"?$anim[($ac++)%($#anim+1)]\x08"/se
 ' \
 | ( [[ -t 1 ]] && tee "$pinsdonefn" || cat > "$pinsdonefn" )
