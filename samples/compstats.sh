#!/bin/bash

function space2csv {
  sed 's/^ *//g' | sed 's/ *$//g' | sed 's/ \+/,/g'
}

function genStats {
  STATS="`vmstat`"
  echo hostname,timestamp,`echo "$STATS" | tail -n 2 | head -n 1 | space2csv`
  while true; do
    echo `hostname`,`date +%s%3N`,`echo "$STATS" | tail -n 1 | space2csv`
    sleep 1
  done
}

genStats
