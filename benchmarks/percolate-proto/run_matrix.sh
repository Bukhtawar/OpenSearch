#!/bin/bash
# Runs the ingest-time (Java) and query-time (DataFusion) legs across rule counts.
# Environment:
#   WORK   scratch dir holding data/ (10M-row Parquet+TSV), data2/ (2M-row subset), rules_*/ and target/
#   CODE   this directory (defaults to the script's own location)
#   JAVA   java binary (default: java on PATH)
#   RULES  rule set directory name under $WORK (default rules_nore = regex leaves disabled)
# Results appended to $WORK/results_$RULES.txt as one line per run.
set -u
P=${WORK:?set WORK to the scratch dir}
CODE=${CODE:-$(cd "$(dirname "$0")" && pwd)}
JAVA=${JAVA:-java}
RULES=${RULES:-rules_nore}
OUT=$P/results_$RULES.txt
: > $OUT
mkdir -p $P/data2 && ln -sf $P/data/part-0000.parquet $P/data/part-0001.parquet $P/data/part-0000.tsv $P/data/part-0001.tsv $P/data2/
echo "# start $(date -u +%FT%TZ) host=$(uname -m) nproc=$(nproc)" >> $OUT
for N in 50 200 782; do
  for MODE in where project; do
    $P/target/release/dfscan $P/data $P/$RULES/rules.json $P/$RULES/rules.sql $N $MODE 16 2 2>&1 | grep '^leg=' | sed "s/^/dataset=10M /" >> $OUT
  done
  $P/target/release/dfscan $P/data2 $P/$RULES/rules.json $P/$RULES/rules.sql $N where 1 1 2>&1 | grep '^leg=' | sed "s/^/dataset=2M /" >> $OUT
  for MODE in a1 a1cse a2 a3; do
    $JAVA -Xmx12g -XX:+UseParallelGC -cp $CODE/java Harness $P/data2 $P/$RULES/rules.json $N $MODE 250000 2>&1 | grep '^leg=' | sed "s/^/dataset=2M /" >> $OUT
  done
  AC_KIND=dfa $P/target/release/rsmatch $P/data2 $P/$RULES/rules.json $N 2>/dev/null | grep '^leg=' | sed "s/^/dataset=2M /" >> $OUT
  echo "# N=$N done $(date -u +%FT%TZ)" >> $OUT
done
echo "# end $(date -u +%FT%TZ)" >> $OUT
