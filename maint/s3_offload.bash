#!/bin/bash

# installation:
#
#   sudo ln -s /srv/cargo/dagcargo/maint/s3_offload.bash  /etc/cron.hourly/car_s3_offload
#

set -e
set -o pipefail

# let someone else make this for perm reasons
LOGDIR="/srv/cargo/LOGS/$(date -u '+%Y-%m-%d')"
while ! [[ -d "$LOGDIR" ]]; do sleep 1; done

exec 1>>"$LOGDIR/s3_offload.log"
exec 2>&1

CARGO_DATADIR="/srv/cargo/CAR_DATA"
CARGO_NGINX_REDIRECTS_INCLUDE="/srv/cargo/ETC/car_s3_redirects"
export CARGO_S3_BUCKET="web3-storage-carstage"

[[ -z "$(ls -A "$CARGO_DATADIR" )" ]] && exit 0

[[ $EUID -ne 0 ]] && echo "You must be root to run this" && exit 1

aws s3 sync --no-progress --acl public-read "$CARGO_DATADIR" "s3://$CARGO_S3_BUCKET/"

aws s3 ls "s3://$CARGO_S3_BUCKET/" \
| grep -Eo '\S+\.car$' \
| perl -ne 'chomp; ($root) = m/([^_]+)/; $full = $_; print "rewrite ^/deal-cars/$_\$  https://$ENV{CARGO_S3_BUCKET}.s3.amazonaws.com/$full permanent;\n" for $full, "$root.car"' \
> "$CARGO_NGINX_REDIRECTS_INCLUDE"

/etc/init.d/nginx reload >/dev/null

aws s3 ls "s3://$CARGO_S3_BUCKET/" \
| grep -Eo '\S+\.car$' \
| xargs -I{} -n1 rm -f "$CARGO_DATADIR/{}"
