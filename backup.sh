#!/bin/sh

find main -type f                     \
  \( -name "*.ipynb" -o               \
     -name "*.tar.gz" -o              \
     -name "crdb-latency.csv" -o      \
     -name "crdb.csv" -o              \
     -name "*.sh" -o                  \
     -name "*.parquet" \)             \
  -not -path "./archived/*"           \
  -not -path "./quick/*"              \
  -not -path "./.ipynb_checkpoints/*" \
  > backup-files.txt

tar -czvf detock.tar.gz -T backup-files.txt