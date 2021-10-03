#!/bin/bash

set -eu

LOGDIR="$HOME/LOGS/$(date -u '+%Y-%m-%d')"
mkdir -p "$LOGDIR"

LOGFILE="$LOGDIR/$1"
"${@:2}" >>"$LOGFILE" 2>&1
