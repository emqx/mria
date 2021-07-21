#!/bin/bash

set -euo pipefail

TAG="$1"
DRYRUN=0
if [ "${2:-}" = "--dryrun" ]; then
    DRYRUN=1
fi

VSN="$(erl -noshell -eval '{ok, [{application, ekka, App}]}=file:consult("src/ekka.app.src"), io:format(proplists:get_value(vsn,App)), halt(0)')"
UPGRADE_BASE="$(erl -noshell -eval '{ok, [Appup]}=file:consult("src/ekka.appup.src"), io:format(element(1,Appup)), halt(0)')"

if [ "$VSN" != "$UPGRADE_BASE" ]; then
    echo "ERROR: vsn and upgrade base mismatch"
    echo "vsn in app.src: $VSN"
    echo "upgrade base in appup.src: $UPGRADE_BASE"
    exit 1
fi

if [ "$DRYRUN" -eq 0 ]; then
    if [ "$VSN" != "$TAG" ]; then
        echo "ERROR: vsn and tag mismatch"
        echo "vsn in app.src: $VSN"
        echo "new tag to add: $TAG"
        exit 2
    fi
    git tag "$1"
fi
