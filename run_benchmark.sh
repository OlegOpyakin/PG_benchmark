#!/bin/bash
set -e

NUM_RUNS=3
SCALE=5

mkdir -p results
rm -f results/pgbench_results.csv

mvn clean package -DskipTests

for run in $(seq 1 $NUM_RUNS); do
    java -jar target/benchmarks.jar results/pgbench_results.csv $run $SCALE
done

python3 scripts/plot_histogram.py results/pgbench_results.csv results
