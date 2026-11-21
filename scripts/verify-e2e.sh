#!/usr/bin/env sh
set -eu

EDGE_URL="${EDGE_URL:-http://localhost:8083}"
FOG_URL="${FOG_URL:-http://localhost:8082}"
CLOUD_URL="${CLOUD_URL:-http://localhost:8081}"

json_get() {
    python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get(sys.argv[1], ""))' "$1"
}

echo "Waiting for Edge, Fog, and Cloud APIs..."
attempt=1
while [ "$attempt" -le 60 ]; do
    if curl -fsS --max-time 5 "$EDGE_URL/api/edge/stats" >/tmp/streamforge-edge.json 2>/dev/null &&
       curl -fsS --max-time 5 "$FOG_URL/api/fog/stats" >/tmp/streamforge-fog.json 2>/dev/null &&
       curl -fsS --max-time 5 "$CLOUD_URL/api/cloud/stats" >/tmp/streamforge-cloud.json 2>/dev/null; then
        echo "All tiers healthy."
        break
    fi
    attempt=$((attempt + 1))
    sleep 2
done
if [ "$attempt" -gt 60 ]; then
    echo "ERROR: Timed out waiting for the StreamForge APIs." >&2
    exit 1
fi

curl -fsS --max-time 5 -H 'Content-Type: application/json' \
    -d '{"anomalyBurst":true,"anomalyBurstCount":50}' \
    "$EDGE_URL/api/edge/control" >/tmp/streamforge-edge-after.json
sleep 3

curl -fsS --max-time 5 "$FOG_URL/api/fog/stats" >/tmp/streamforge-fog.json
edge_reduction="$(json_get reductionPercent </tmp/streamforge-edge-after.json)"
anomalies="$(json_get anomaliesDetected </tmp/streamforge-edge-after.json)"
fog_ratio="$(json_get cumulativeCompressionRatio </tmp/streamforge-fog.json)"
codec="$(json_get activeEncodingCompression </tmp/streamforge-fog.json)"
echo "Edge reduction: ${edge_reduction}% | Anomaly overrides: ${anomalies}"
echo "Fog compression ratio: ${fog_ratio}x | Codec: ${codec}"

curl -fsS --max-time 5 -H 'Content-Type: application/json' \
    -d '{"failMode":true,"latencyMs":0}' \
    "$CLOUD_URL/api/cloud/chaos" >/tmp/streamforge-chaos-outage.json
enabled="$(json_get failMode </tmp/streamforge-chaos-outage.json)"
if [ "$enabled" != "True" ] && [ "$enabled" != "true" ]; then
    echo "ERROR: Cloud outage mode did not enable." >&2
    exit 1
fi

echo "Cloud outage enabled. Waiting 8 seconds for circuit and WAL failover..."
sleep 8
curl -fsS --max-time 5 "$FOG_URL/api/fog/stats" >/tmp/streamforge-fog-outage.json
state="$(json_get circuitBreakerState </tmp/streamforge-fog-outage.json)"
wal_segments="$(json_get pendingWalSegmentCount </tmp/streamforge-fog-outage.json)"
wal_bytes="$(json_get pendingWalBytes </tmp/streamforge-fog-outage.json)"
echo "Circuit breaker: ${state} | WAL: ${wal_segments} segments / ${wal_bytes} bytes"

if [ "$state" != "OPEN" ]; then
    echo "ERROR: Resilience4j circuit breaker did not open; observed '${state}'." >&2
    exit 1
fi
if [ "$wal_segments" -le 0 ] || [ "$wal_bytes" -le 0 ]; then
    echo "ERROR: Disk WAL did not accumulate buffered data." >&2
    exit 1
fi

curl -fsS --max-time 5 "$CLOUD_URL/api/cloud/stats" >/tmp/streamforge-cloud-before-recovery.json
batches_before="$(json_get batchesIngested </tmp/streamforge-cloud-before-recovery.json)"

curl -fsS --max-time 5 -H 'Content-Type: application/json' \
    -d '{"failMode":false,"latencyMs":0}' \
    "$CLOUD_URL/api/cloud/chaos" >/dev/null

echo "Cloud outage disabled. Waiting 8 seconds for WAL replay..."
sleep 8
curl -fsS --max-time 5 "$FOG_URL/api/fog/stats" >/tmp/streamforge-fog-recovered.json
curl -fsS --max-time 5 "$CLOUD_URL/api/cloud/stats" >/tmp/streamforge-cloud-recovered.json
recovered_segments="$(json_get pendingWalSegmentCount </tmp/streamforge-fog-recovered.json)"
recovered_bytes="$(json_get pendingWalBytes </tmp/streamforge-fog-recovered.json)"
batches_recovered="$(json_get batchesIngested </tmp/streamforge-cloud-recovered.json)"
checksum_errors="$(json_get checksumErrors </tmp/streamforge-cloud-recovered.json)"
recovered_state="$(json_get circuitBreakerState </tmp/streamforge-fog-recovered.json)"

echo "Circuit breaker: ${recovered_state} | WAL: ${recovered_segments} segments / ${recovered_bytes} bytes"
echo "Persisted batches: ${batches_recovered} | CRC32 errors: ${checksum_errors}"

if [ "$recovered_segments" -ne 0 ] || [ "$recovered_bytes" -ne 0 ]; then
    echo "ERROR: Disk WAL did not drain after cloud recovery." >&2
    exit 1
fi
if [ "$batches_recovered" -lt "$batches_before" ]; then
    echo "ERROR: Cloud persisted batch count decreased during recovery." >&2
    exit 1
fi
if [ "$checksum_errors" -ne 0 ]; then
    echo "ERROR: CRC32 integrity verification reported checksum failures." >&2
    exit 1
fi

echo "E2E chaos verification PASSED."
