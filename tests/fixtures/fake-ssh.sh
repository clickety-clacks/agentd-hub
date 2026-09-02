#!/bin/sh
set -eu

machine=${9}
[ "$#" -eq 10 ] || exit 96
command=${10-}
if [ "$command" != 'PATH="$HOME/.local/bin:$PATH" agentd watch --json' ]; then
  exit 97
fi
mode=$(cat "$machine/mode")

case "$mode" in
  timeout)
    echo $$ >"$machine/pid"
    exec sleep 30
    ;;
  valid)
    echo started >"$machine/started"
    printf '%s\n' '{"type":"snapshot","schema":"agentd.snapshot.v1","instanceId":"i","revision":1,"observedAtUnixMs":1,"scan":{},"agents":[]}'
    echo exited >"$machine/exited"
    ;;
  *)
    exit 2
    ;;
esac
