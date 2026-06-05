#!/bin/bash
# ClickBench benchmark script with per-query log isolation.
# Clears LC cache before each query, captures logs per query into separate files.
ITERS=3
QUERY_DIR="/home/ec2-user/OpenSearch-bench/sandbox/plugins/analytics-engine/src/test/resources/clickbench/queries"
OS="http://localhost:9200"
LOG_FILE="/home/ec2-user/opensearch/logs/opensearch.log"
PER_QUERY_LOG_DIR="/home/ec2-user/bench_query_logs"

mkdir -p "$PER_QUERY_LOG_DIR"

curl -s -X PUT "$OS/_cluster/settings" -H "Content-Type: application/json" \
  -d '{"transient":{"datafusion.liquid_cache.enabled":"true"}}' > /dev/null
sleep 2

echo "=== ClickBench LC ON - ${ITERS} iters (cache cleared per query) ==="
echo "Query | Min(ms) | Times(ms)"
echo "------|---------|----------"

for i in $(seq 1 43); do
  QFILE="$QUERY_DIR/q${i}.ppl"
  if [ ! -f "$QFILE" ]; then continue; fi

  python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    lines = f.readlines()
ppl_lines = []
capture = False
for line in lines:
    stripped = line.strip()
    if stripped.startswith('source='):
        capture = True
    if capture and stripped and not stripped.startswith('/*') and not stripped.startswith('*/') and not stripped.startswith('*'):
        ppl_lines.append(stripped)
ppl = ' '.join(ppl_lines).replace('source=hits', 'source=clickbench')
json.dump({'query': ppl}, open('/tmp/ppl_query.json', 'w'))
" "$QFILE"

  if [ ! -s /tmp/ppl_query.json ]; then continue; fi

  # Clear LC cache before each query
  curl -s -X POST "$OS/_plugins/analytics_backend_datafusion/liquid_cache/clear" > /dev/null 2>&1

  # Record log line count before query
  BEFORE=$(wc -l < "$LOG_FILE" 2>/dev/null || echo 0)

  TIMES=""
  MIN=999999
  for iter in $(seq 1 $ITERS); do
    START=$(date +%s%N)
    RESP=$(curl -s --max-time 120 -X POST "$OS/_plugins/_ppl" \
      -H "Content-Type: application/json" -d @/tmp/ppl_query.json 2>&1)
    END=$(date +%s%N)
    MS=$(( (END - START) / 1000000 ))
    if echo "$RESP" | grep -q '"status":4\|"status":5'; then MS=-1; fi
    if [ $MS -lt $MIN ] && [ $MS -ge 0 ]; then MIN=$MS; fi
    TIMES="$TIMES $MS"
  done

  # Capture per-query logs
  AFTER=$(wc -l < "$LOG_FILE" 2>/dev/null || echo 0)
  if [ "$AFTER" -gt "$BEFORE" ]; then
    sed -n "$((BEFORE+1)),${AFTER}p" "$LOG_FILE" | grep "LC-" > "$PER_QUERY_LOG_DIR/q${i}.log"
  fi

  if [ $MIN -eq 999999 ]; then MIN="ERR"; fi
  printf "Q%-2d   | %7s |%s\n" "$i" "$MIN" "$TIMES"
done
echo "=== Done ==="

# Print per-query LC summary
echo
echo "=== Per-Query LC Summary ==="
for i in $(seq 1 43); do
  LOGF="$PER_QUERY_LOG_DIR/q${i}.log"
  if [ -f "$LOGF" ] && [ -s "$LOGF" ]; then
    OPT_WRAP=$(grep -c "WRAP" "$LOGF" 2>/dev/null)
    OPT_SKIP=$(grep -c "SKIP" "$LOGF" 2>/dev/null)
    STREAM=$(grep -c "LC STREAM" "$LOGF" 2>/dev/null)
    DELEGATE=$(grep -c "DELEGATE" "$LOGF" 2>/dev/null)
    EMPTY=$(grep -c "EMPTY" "$LOGF" 2>/dev/null)
    HITS=$(grep "LC-Reader" "$LOGF" 2>/dev/null | grep -c "misses=0")
    MISSES=$(grep "LC-Reader" "$LOGF" 2>/dev/null | grep -c "misses=[1-9]")
    printf "Q%-2d: wrap=%s skip=%s stream=%s delegate=%s empty=%s hits=%s misses=%s\n" \
      "$i" "$OPT_WRAP" "$OPT_SKIP" "$STREAM" "$DELEGATE" "$EMPTY" "$HITS" "$MISSES"
  else
    printf "Q%-2d: NO LC LOGS (skipped by optimizer)\n" "$i"
  fi
done
