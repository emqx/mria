#!/bin/bash
set -euo pipefail

[ $(uname) = Linux ] || {
    echo "Sorry, this script only works on Linux";
    exit 1;
}

[ -z ${1+1} ] && {
    echo "Emulate network latency on the localhost.

USAGE:

    $(basename $0) [ -d DELAY ] [ -j JITTER ] [ -r RATE ] [PORT1 PORT2 ...]

It is possible to specify PORT as 'empd' to apply delay to the
distribution ports of all running BEAM VMs (excluding the CT
master).

Both DELAY and JITTER should be more than 0

Port can be:

   1. A number: it will apply netem on the messages that are sent to
      and from the port

   2. A number with 'd' prefix (e.g. d1883): it will apply netem on
      the messages sent to the port

   3. A number with 's' prefix (e.g. s1883): it will apply netem on
      the messages sent from the port

   4. 'epmd': It will apply delay to all ports registered in epmd and
      apply netem to all erlang distribution protocol connections.

EXAMPLE:

    $(basename $0) -d 500ms -j 1 -r 10kbps 8001 s8002 empd
"
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

echo "Delay=${DELAY} jitter=${JITTER} rate=${RATE} interface=${INTERFACE}"

# Shape packets marked as 12
MARK=12
ID=$MARK
tc qdisc add dev "$INTERFACE" root handle 1: htb
tc class add dev "$INTERFACE" parent 1: classid 1:$ID htb rate "$RATE"
# tc qdisc add dev "$INTERFACE" root netem rate "$RATE" delay "$DELAY" "$JITTER"
tc qdisc add dev "$INTERFACE" parent 1:$ID handle $MARK netem delay $DELAY $JITTER distribution normal
tc filter add dev "$INTERFACE" parent 1: prio 1 protocol ip handle $MARK fw flowid 1:$ID

mark() {
    echo "Applying netem on $1 $2"
    iptables -A "${CHAIN}" -p tcp -t mangle -j MARK --set-mark $MARK $1 $2
}

# Create firewall rules to mark the packets:
mark_port() {
    local PORT=$1
    if [[ $PORT =~ ^([0-9]+)$ ]]; then
        mark --sport $PORT
        mark --dport $PORT
    elif [[ $PORT =~ ^s([0-9]+)$ ]]; then
        PORT=${BASH_REMATCH[1]}
        mark --sport $PORT
    elif [[ $PORT =~ ^d([0-9]+)$ ]]; then
        PORT=${BASH_REMATCH[1]}
        mark --dport $PORT
    fi
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
