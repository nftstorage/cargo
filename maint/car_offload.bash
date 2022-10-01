#!/bin/bash

# installation:
#
#   sudo ln -s /srv/cargo/dagcargo/maint/car_offload.bash  /etc/cron.hourly/car_offload
#

set -e
set -o pipefail

# let someone else make this for perm reasons
LOGDIR="/srv/cargo/LOGS/$(date -u '+%Y-%m-%d')"
while ! [[ -d "$LOGDIR" ]]; do sleep 1; done

exec 1>>"$LOGDIR/car_offload.log"
exec 2>&1

export RCLONE_EXEC="sudo -u rclone /srv/rclone/rclone/rclone"
export LOCAL_SRC="/srv/cargo/CAR_DATA"
export REMOTE_DST="cargo_r2:/dagcargo"

CARGO_NGINX_REDIRECTS_INCLUDE="/srv/cargo/ETC/car_redirects"

[[ -z "$(ls -A "$LOCAL_SRC" )" ]] && exit 0

[[ $EUID -ne 0 ]] && echo "You must be root to run this" && exit 1

###
### Only execute one version of offloader concurrently
###
[[ -z "${OFFLOAD_LOCKFILE:-}" ]] \
&& export OFFLOAD_LOCKFILE="/dev/shm/car_offload.lock" \
&& exec /usr/bin/flock -en "$OFFLOAD_LOCKFILE" "$0" "$@"
###
###
###

# copy stuff we have locally
  ls "$LOCAL_SRC" \
| xargs -n1 -P3 -I{} -- $RCLONE_EXEC --checksum --s3-chunk-size=128M --s3-upload-concurrency=3 copy "$LOCAL_SRC/{}" "$REMOTE_DST"

# regenerate the 301 list
  $RCLONE_EXEC lsf "$REMOTE_DST" \
| grep -Eo '\S+\.car$' \
| perl -ne 'chomp; print "rewrite ^/deal-cars/$_\$  https://cargo.dag.haus/$_ permanent;\n"' \
> "$CARGO_NGINX_REDIRECTS_INCLUDE"

# tell nginx to reread the 301 list
/etc/init.d/nginx reload >/dev/null

# delete everything local that matches its md5
  comm -12 <( ls "$LOCAL_SRC" | sort ) <( $RCLONE_EXEC lsf "$REMOTE_DST" | sort ) \
| xargs -n1 -I{} bash -c "$RCLONE_EXEC md5sum $REMOTE_DST/{} | perl -e 'my (\$md5, \$md5fn) = <> =~ /(\S+)\s+([^_]+)/; exit 1 if \$md5 ne \$md5fn' && rm $LOCAL_SRC/{}"
