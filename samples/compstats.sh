#!/bin/bash

function space2csv {
  sed 's/^ *//g' | sed 's/ *$//g' | sed 's/ \+/,/g'
}

function genStats {
  local DATE="`date +%s%3N`"
  local STATS="`vmstat`"
  echo hostname,timestamp,`echo "$STATS" | tail -n 2 | head -n 1 | space2csv`
  while true; do
    STATS="`vmstat`"
    echo $HOSTNAME,$DATE,`echo "$STATS" | tail -n 1 | space2csv`
    sleep 1
  done
}

genStats
