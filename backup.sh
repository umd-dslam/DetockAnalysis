#!/bin/sh

find . -type f                                                                                              \
  \( -name "*.ipynb" -o -name "*.tar.gz" -o -name "crdb-latency.csv" -o -name "crdb.csv" -o -name "*.sh" \) \
  -not -path "./archived/*"                                                                                 \
  -not -path "./quick/*"                                                                                    \
  -not -path "./.ipynb_checkpoints/*"                                                                       \
  > backup-files.txt

tar -czvf detock.tar -T backup-files.txt