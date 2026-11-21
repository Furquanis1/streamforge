# StreamForge — System Architecture & Engineering Boundaries

## 1. Project Identity & Purpose
- **System Name:** StreamForge
- **Academic Framing:** Adaptive Edge-Fog-Cloud Telemetry Compression & Fault-Tolerant Ingestion Pipeline
- **Engineering Reality:** High-throughput, fault-tolerant distributed telemetry ingestion engine built on Java 21, Spring Boot, MQTT 5, gRPC/Protobuf, Redis, MySQL, and React.
- **Deployment Mode:** 100% software-based containerized emulation via Docker Compose (zero physical hardware).

---

## 2. Strict Scope & Non-Goals (Guardrails for AI Agents)
- **DO:**
  - Read this file (`ARCHITECTURE.md`) before generating or modifying any code.
  - Adhere strictly to the Protobuf contracts in `pipeline-proto`.
  - Keep core business logic (filtering, encoding, compression, WAL) decoupled from transport (MQTT/gRPC) and persistence (JPA/Redis).
  - Write deterministic JUnit 5 unit tests for all encoding, filtering, compression, and WAL recovery logic.
  - Enforce bounded queues and explicit resource caps so backpressure and failover are demonstrable.
- **DO NOT:**
  - Do NOT add Spring Security, JWT auth, user login/signup, or multi-tenancy (out of scope; distracts from core distributed systems focus).
  - Do NOT use iFogSim, CloudSim, or external academic simulators.
  - Do NOT invent new RPCs, REST endpoints, or database tables outside this specification without explicit approval.
  - Do NOT replace gRPC between Fog and Cloud with REST/JSON.

---

## 3. Repository & Module Layout
```text
streamforge/
├── ARCHITECTURE.md                  # Single source of truth (this file)
├── docker-compose.yml               # Infrastructure + multi-node service orchestration
├── mosquitto/config/mosquitto.conf  # Local MQTT broker config
├── pipeline-proto/                  # Module 1: Shared Protobuf models & gRPC service contracts
├── pipeline-common/                 # Module 2: Compression strategies, Columnar Delta encoder, CRC32
├── edge-node/                       # Module 3: Virtual-thread sensor simulator + Filter chain + RingBuffer
├── fog-gateway/                     # Module 4: MQTT consumer, Batcher, Adaptive Compressor, Resilience4j + Disk WAL, gRPC client
├── cloud-ingest/                    # Module 5: gRPC server, Redis deduplication, MySQL storage, SSE Metrics & Chaos API
└── telemetry-dashboard/             # Module 6: React + Vite + Tailwind + Recharts live console
```

---

## 4. Network Topology, Ports & Docker Services

| Service Name | Container / Image | Internal Port | Host Port | Role |
| :--- | :--- | :--- | :--- | :--- |
| `mosquitto` | `eclipse-mosquitto:2.0` | `1883` | `1883` | MQTT 5 broker connecting Edge $\rightarrow$ Fog |
| `mysql` | `mysql:8.0` | `3306` | `3306` | Persistent storage (`streamforge` DB) |
| `redis` | `redis:7-alpine` | `6379` | `6379` | Batch idempotency cache & live sliding-window metrics |
| `toxiproxy` | `ghcr.io/shopify/toxiproxy:2.9.0` | `8474`, `9090` | `8474`, `9090` | Network fault/bandwidth proxy (`9090` $\rightarrow$ `cloud-ingest:9091`) |
| `cloud-ingest` | Spring Boot App | `8080` (HTTP), `9091` (gRPC) | `8080`, `9091` | Cloud receiver, decompressor, metrics & chaos control plane |
| `fog-gateway-1` | Spring Boot App | `8081` | `8081` | Fog Aggregator Replica 1 (`mem_limit: 384m`, `cpus: '0.5'`) |
| `fog-gateway-2` | Spring Boot App | `8082` | `8082` | Fog Aggregator Replica 2 (`mem_limit: 384m`, `cpus: '0.5'`) |
| `edge-node` | Java 21 App | `8083` (Control HTTP) | `8083` | Virtual-thread telemetry generator & edge filter |
| `telemetry-dashboard` | React + Vite / Nginx | `80` | `3000` | Live visualization & chaos control UI |

---

## 5. Locked Data Contracts (`pipeline-proto/src/main/proto/telemetry.proto`)
All inter-tier communication MUST use these exact schemas:

- **MQTT Topic (Edge $\rightarrow$ Fog):** `sensors/{nodeId}/telemetry`
  - **Fog Subscription (Load-Balanced):** `$share/fog-workers/sensors/+/telemetry`
  - **MQTT Payload:** Serialized `SensorEvent` bytes (or raw UTF-8 JSON bytes when `RAW_JSON` baseline mode is active).
- **gRPC Service (Fog $\rightarrow$ Toxiproxy `9090` $\rightarrow$ Cloud `9091`):**
  - `TelemetryIngestService.IngestBatch(CompressedBatchRequest) returns (BatchAckResponse)`
- **Proto Definitions (`package com.streamforge.proto`):**
  - `enum CompressionType { RAW_NONE = 0; GZIP = 1; LZ4 = 2; ZSTD = 3; }`
  - `enum EncodingType { PLAIN_ROW = 0; COLUMNAR_DELTA = 1; }`
  - `message SensorEvent { string node_id = 1; string sensor_type = 2; int64 timestamp_ms = 3; double value = 4; bool anomaly = 5; }`
  - `message CompressedBatchRequest`:
    - `string batch_id = 1` (UUID for idempotency)
    - `string fog_node_id = 2` (e.g., `fog-1`, `fog-2`)
    - `int64 created_at_ms = 3` (epoch millis)
    - `int32 record_count = 4`
    - `int32 raw_bytes_size = 5` (uncompressed equivalent byte size)
    - `int32 compressed_bytes_size = 6` (actual wire payload byte size)
    - `int32 edge_dropped_count = 7` (events filtered out at Edge since last batch)
    - `CompressionType compression = 8`
    - `EncodingType encoding = 9`
    - `int64 crc32_checksum = 10` (CRC32 of uncompressed payload for integrity verification)
    - `bytes payload = 11`
  - `message BatchAckResponse { string batch_id = 1; bool accepted = 2; bool duplicate = 3; int64 server_received_at_ms = 4; string message = 5; }`

---

## 6. Core Algorithms & Engineering Patterns by Module

### Module 2: `pipeline-common`
1. **Columnar Delta Encoder (`ColumnarDeltaCodec`):**
   - Converts `List<SensorEvent>` into column arrays before compression:
     - `timestamps[]`: Base timestamp (`int64`) + Delta-of-Delta (`int32`) offsets.
     - `values[]`: Base double (`int64` bits) + XOR difference from previous value (Gorilla-style bit packing) or scaled fixed-point delta.
     - `nodeIds[]` & `sensorTypes[]`: Dictionary-encoded index lookup table + index array.
2. **Compression Strategy (`CompressionStrategy` interface):**
   - Implementations: `NoOpCompression`, `GzipCompression`, `Lz4Compression`, `ZstdCompression`.
   - Factory: `CompressionFactory.get(CompressionType type)`.
3. **Integrity (`ChecksumUtil`):**
   - Standard `java.util.zip.CRC32` computed prior to compression and verified post-decompression on Cloud.

### Module 3: `edge-node`
1. **Concurrency:** Java 21 `Executors.newVirtualThreadPerTaskExecutor()` running `N` configurable sensor workers (default: `100` nodes @ `10 Hz` = `1,000 events/sec`, dynamically scalable from `500` to `10,000 events/sec`).
2. **Chain-of-Responsibility Filter Pipeline:**
   - `OutlierSpikeFilter`: Computes rolling mean and standard deviation over the last 20 readings; flags `anomaly = true` if $|x - \mu| > 3.5\sigma$ (anomalies bypass deadband suppression).
   - `AdaptiveDeadbandFilter`: Suppresses non-anomalous readings if $|v_{\text{curr}} - v_{\text{lastSent}}| < \Delta_{\text{adaptive}}$, where $\Delta_{\text{adaptive}}$ widens during low-variance steady state and tightens during high variance.
3. **Disconnect Resilience:** Bounded lock-free/synchronized `RingBuffer` (capacity: `50,000` events) that buffers events if Mosquitto is unreachable and replays them upon reconnect.

### Module 4: `fog-gateway`
1. **Micro-Batcher:** Bounded `LinkedBlockingQueue<SensorEvent>` (capacity `10,000`). Flushes when batch reaches `250 records` OR `1,000 ms` elapsed (whichever comes first).
2. **Adaptive Compressor:**
   - Modes: `RAW_JSON` (baseline), `LZ4_FAST`, `ZSTD_HIGH`, `ADAPTIVE_AUTO`.
   - `ADAPTIVE_AUTO` rule: Selects `LZ4` when Fog queue occupancy $> 60\%$ (prioritizing CPU speed/throughput) and `ZSTD` when queue occupancy $\le 60\%$ (prioritizing maximum bandwidth compression).
3. **Resilience4j + Disk Write-Ahead Log (WAL):**
   - `gRPC` client wrapped in Resilience4j `CircuitBreaker` + `Retry` (exponential backoff).
   - When Circuit Breaker is `OPEN` or gRPC fails after retries, serialized `CompressedBatchRequest` frames are appended to a local binary WAL file (`./wal/fog-wal.bin`).
   - Background recovery worker drains WAL frames in FIFO order once Cloud health recovers.

### Module 5: `cloud-ingest`
1. **Idempotency Guard:** Checks Redis `SET batch:{batchId} "1" NX EX 3600`. If key exists, increments duplicate counter and returns `BatchAckResponse(accepted=true, duplicate=true)`.
2. **Decompression & Verification:** Reverses `CompressionStrategy` $\rightarrow$ verifies `CRC32` $\rightarrow$ decodes `ColumnarDeltaCodec` $\rightarrow$ persists batch summary to MySQL (`batch_metrics` table) and updates live sliding-window metrics in Redis/memory.
3. **Control & Observability REST/SSE API (`http://localhost:8080/api/v1`):**
   - `GET /metrics/stream` — Server-Sent Events (SSE) pushing live stats every `1,000 ms` (raw bytes/sec, compressed bytes/sec, bandwidth saved %, edge drop %, p50/p95/p99 latency ms, active fog nodes, circuit breaker & WAL depth).
   - `POST /control/config` — Broadcasts runtime config updates (compression mode, edge deadband toggle, edge event rate) to Edge and Fog nodes.
   - `POST /chaos/network` — Configures Toxiproxy bandwidth limit (kbps) and latency (ms) between Fog and Cloud.
   - `POST /chaos/cloud-outage` — Toggles simulated Cloud gRPC failure (`503 UNAVAILABLE`) to demonstrate Fog Circuit Breaker + WAL failover on command.
   - `POST /benchmarks/run` — Runs an automated 30-second comparative benchmark across `RAW_JSON`, `LZ4_FAST`, and `ZSTD_HIGH` and returns JSON/CSV summary results.

### Module 6: `telemetry-dashboard`
- Single-page React 18 + Vite + Tailwind CSS + Recharts observability console:
  1. **KPI Header Cards:** Bandwidth Saved (%), Compression Ratio, End-to-End p95 Latency (ms), Edge Filter Drop Rate (%), Active Fog Nodes, WAL Queue Depth.
  2. **Real-Time Charts:**
     - Area/Line Chart: *Raw Generated vs. Edge Filtered vs. Cloud Received (KB/s)*.
     - Bar Chart: *Latency Breakdown (Edge+Fog Processing vs. Network Transit)*.
  3. **Chaos & Pipeline Control Panel:**
     - Mode Switcher (`RAW_JSON`, `LZ4_FAST`, `ZSTD_HIGH`, `ADAPTIVE_AUTO`).
     - Edge Filter Toggle (ON / OFF) & Load Slider (`500` to `10,000` events/sec).
     - Network Throttle Preset Buttons (`Unthrottled 100 Mbps`, `4G LTE 2 Mbps`, `Constrained IoT 256 Kbps`).
     - `Simulate Cloud Outage` Toggle Button (live demo of Circuit Breaker tripping + WAL draining).