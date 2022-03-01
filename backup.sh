#!/bin/sh

find main -type f                     \
  \( -name "*.tar.gz" -o              \
     -name "crdb-latency.csv" -o      \
     -name "crdb.csv"                 \
  \)                                  \
  -not -path "./.ipynb_checkpoints/*" \
  > backup-files.txt

tar -czvf detock.tar.gz -T backup-files.txt
