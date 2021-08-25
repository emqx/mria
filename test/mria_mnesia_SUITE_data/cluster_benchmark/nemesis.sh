#!/bin/bash
# This script runs as root, receives commands from the common test
# suite over FIFO, and forwards them to slowdown.sh
set -uo pipefail

FIFO=/tmp/nemesis

if [ -p $FIFO ]; then
    echo "Nemesis is already running"
    exit 0
fi

trap "rm -f $FIFO" EXIT

mkfifo $FIFO
chmod 666 $FIFO

while true; do
    if read line < $FIFO; then
        echo "Received command ${line}"
        $(dirname $0)/slowdown.sh -d $line -j 1 epmd
    fi
done
