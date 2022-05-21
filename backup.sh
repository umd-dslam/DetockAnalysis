#!/bin/sh
experiments="cockroach \
             cockroach-latency \
             tpcc \
             ycsb \
             ycsb-asym \
             ycsb-jitter \
             ycsb-jitter-overshoot10 \
             ycsb-latency"

for exp in $experiments
do
  find main/$exp -type f -name "*.tar.gz" > $exp-files.txt
  tar -czvf $exp.tar.gz -T $exp-files.txt
done
