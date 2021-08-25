#!/bin/bash
set -euo pipefail

[ $(uname) = Linux ] || {
    echo "Sorry, this script only works on Linux";
    exit 1;
}

[ -z ${1+1} ] && {
    echo "Emulate network latency on the localhost.

Usage:

    $(basename $0) DELAY JITTER [PORT1 PORT2 ...]

It is possible to specify PORT as 'empd' to apply delay to the
distribution ports of all running BEAM VMs (excluding the CT
master).

Both DELAY and JITTER should be more than 0

Example:

    $(basename $0) 500ms 1 8001 8002 empd"
    exit 1;
}

DELAY="10ms"
JITTER=1
INTERFACE=lo
RATE=1000Mbps

while getopts "r:i:d:j:" flag; do
    case "$flag" in
        i) INTERFACE="$OPTARG";;
        d) DELAY="$OPTARG";;
        j) JITTER="$OPTARG";;
        r) RATE="$OPTARG";;
    esac
done
shift $((OPTIND-1))

CHAIN="OUTPUT"
# Clean up:
iptables -t mangle -F "$CHAIN" || true
tc qdisc del dev "$INTERFACE" root || true

echo "Delay=${DELAY} jitter=${JITTER} interface=${INTERFACE}"

# Shape packets marked as 12
MARK=12
ID=$MARK
tc qdisc add dev "$INTERFACE" root handle 1: htb
tc class add dev "$INTERFACE" parent 1: classid 1:$ID htb rate "$RATE"
# tc qdisc add dev "$INTERFACE" root netem rate "$RATE" delay "$DELAY" "$JITTER"
tc qdisc add dev "$INTERFACE" parent 1:$ID handle $MARK netem delay $DELAY $JITTER distribution normal
tc filter add dev "$INTERFACE" parent 1: prio 1 protocol ip handle $MARK fw flowid 1:$ID

# Create firewall rules to mark the packets:
mark_port() {
    PORT=$1
    echo "Adding latency on tcp port $PORT"
    iptables -A "${CHAIN}" -p tcp --dport $PORT -t mangle -j MARK --set-mark $MARK
}

while [ ! -z ${1+1} ]; do
    PORT=$1
    shift
    if [ $PORT = epmd ]; then
        for i in $(epmd -names | grep -v 'ct@' | awk '/at port/{print $5}'); do
            mark_port $i
        done
    else
        mark_port $PORT
    fi
done
