#!/bin/sh

find main -type f                     \
  \( -name "*.tar.gz" -o              \
     -name "*.parquet" -o             \
     -name "*.pickle" -o              \
     -name "crdb-latency.csv" -o      \
     -name "crdb.csv"                 \
  \)                                  \
  -not -path "./.ipynb_checkpoints/*" \
  > backup-files.txt

# tar -czvf detock.tar.gz -T backup-files.txt