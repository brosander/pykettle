#!/bin/bash

function space2csv {
  sed 's/^ *//g' | sed 's/ *$//g' | sed 's/ \+/,/g'
}

function genStats {
  STAT="`vmstat`"
  echo hostname,timestamp,`echo "$STAT" | tail -n 2 | head -n 1 | space2csv`
  while true; do
    STAT="`vmstat`"
    echo `hostname`,`date +%s%3N`,`echo "$STAT" | tail -n 1 | space2csv`
    sleep 1
  done
}

genStats
