#!/bin/bash
# Run full cluster benchmark
set -eu pipefail

# Perform OS check (we need `netem' and `iptables' features to inject
# faults/delays into the system, those are Linux-only):
[ $(uname) = Linux ] || {
    echo "Sorry, this script relies on some Linux IP stack features, and only works on Linux";
    exit 1;
}

export SCRIPT_DIR=$(dirname $0)

# Start nemesis process:
echo "Root permissions are needed to start nemesis process"
sudo -b ${SCRIPT_DIR}/nemesis.sh

# Run benchmark:
rebar3 do ct --name ct@127.0.0.1 --suite mria_mnesia_SUITE --case cluster_benchmark --readable=true

# Collect stats:
${SCRIPT_DIR}/latency_graph.gp
