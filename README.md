# StreamForge

StreamForge is a three-tier distributed telemetry pipeline demonstrating adaptive edge filtering, bounded fog aggregation, lossless compression, disk-backed failover, and idempotent cloud persistence.

## System Architecture

```text
+------------------+      MQTT 5       +----------------------+      gRPC       +----------------------+
| Edge Node        | ----------------> | Fog Gateway          | --------------> | Cloud Ingest         |
| :8083            | sensors/+/{node}  | :8082                | cloud-ingest:9090| :8081 HTTP / :9090  |
| Virtual sensors  |                   | Micro-batcher        |                 | Decompress + verify  |
| Welford Z-score  |                   | Columnar Delta       |                 | Redis idempotency    |
| Adaptive deadband|                   | Delta/XOR encoding   |                 | MySQL persistence    |
+------------------+                   | LZ4 / Zstd / GZip    |                 | SSE metrics stream   |
                                       | Resilience4j + WAL   |                 +----------+-----------+
                                       +----------+-----------+                            |
                                                  |                                        |
                                                  +------------ fog_wal:/app/wal --------+

Mosquitto :1883       Redis :6379       MySQL :3306       Dashboard :5173
```

### Three-Tier Design Rationale

- **Edge:** Virtual-thread sensor workers generate telemetry. A rolling Welford Z-score filter detects anomalies, the sanity/range filter rejects invalid readings, and adaptive deadband suppresses steady-state data. Anomalies bypass filtering.
- **Fog:** The gateway uses a bounded micro-batcher, Columnar Delta encoding, and adaptive LZ4/Zstd/GZip compression. Resilience4j retry/circuit logic plus a binary Disk WAL protect batches during cloud outages.
- **Cloud:** gRPC receives batches, Redis provides 1-hour batch idempotency, CRC32 verifies payload integrity, and MySQL persists telemetry batches and readings. SSE publishes live pipeline snapshots.

## Port & Service Matrix

| Service | Host port | Container role |
|---|---:|---|
| Mosquitto | 1883 | MQTT broker for Edge-to-Fog transport |
| MySQL | 3306 | Persistent batch and sensor storage |
| Redis | 6379 | Batch idempotency and live metrics |
| Cloud Ingest | 8081, 9090 | REST/SSE API and gRPC ingestion |
| Fog Gateway | 8082 | MQTT batching, compression, WAL, gRPC client |
| Edge Node | 8083 | Sensor simulation and REST control API |
| Telemetry Dashboard | 5173 | React observability and chaos console |

## Docker Quickstart

```powershell
docker compose up --build -d
docker compose ps
```

Open [http://localhost:5173](http://localhost:5173). Stop the stack with:

```powershell
docker compose down
```

Remove persisted Fog WAL data and other named volumes:

```powershell
docker compose down -v
```

## Local Maven + Vite Development

Start infrastructure only:

```powershell
docker compose up -d mosquitto mysql redis
```

Run the Maven applications in separate terminals:

```powershell
mvn -pl cloud-ingest -am spring-boot:run
mvn -pl fog-gateway -am spring-boot:run
mvn -pl edge-node -am spring-boot:run
```

Run the dashboard:

```powershell
cd telemetry-dashboard
npm install
npm run dev -- --host 0.0.0.0
```

Local IDE defaults remain `localhost` for MQTT and API endpoints.

## Live Fault-Tolerance Demonstration

1. Open `http://localhost:5173` and confirm all three tier indicators are live.
2. Raise the Edge node count and lower the emit interval to increase traffic.
3. Click **Trigger Anomaly Spike**. Watch Edge reduction/anomalies and the anomaly stream.
4. Switch among `AUTO`, `LZ4`, `ZSTD`, and `GZIP`. Compare raw versus compressed bytes.
5. Enable **Simulate Cloud Outage** for at least 8 seconds.
6. Observe the Fog circuit breaker become `OPEN` and WAL segment/byte counts increase.
7. Disable the outage and verify the circuit recovers and WAL drains as batches replay to MySQL.

## Automated E2E and Chaos Verification

PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File ./scripts/verify-e2e.ps1
```

Linux or macOS:

```bash
./scripts/verify-e2e.sh
```

The script polls all tier APIs, triggers an Edge anomaly burst, enables a cloud outage, verifies circuit/WAL failover, restores cloud connectivity, and asserts WAL drain, batch persistence, and CRC32 integrity.

## Verification Commands

```powershell
mvn clean test
docker compose config
cd telemetry-dashboard
npm install
npm run build
```
