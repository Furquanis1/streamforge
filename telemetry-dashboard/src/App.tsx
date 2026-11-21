import { useEffect, useMemo, useRef, useState } from 'react'
import {
  Area,
  AreaChart,
  CartesianGrid,
  Line,
  LineChart,
  ReferenceDot,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import {
  Activity,
  ArrowRight,
  Gauge,
  Radio,
  Server,
  ShieldCheck,
  Waves,
  Zap,
} from 'lucide-react'
import { API, getJson, postJson } from './api'

type EdgeStats = {
  generated: number
  forwarded: number
  dropped: number
  anomaliesDetected: number
  reductionPercent: number
  activeNodeCount: number
  deadbandEpsilon: number
  emitIntervalMs: number
}

type FogStats = {
  ingressRateEventsPerSec: number
  batchesForwarded: number
  rawBytes: number
  compressedBytes: number
  bytesSaved: number
  activeEncodingCompression: string
  circuitBreakerState: string
  pendingWalSegmentCount: number
  pendingWalBytes: number
  ingesionQueueSize?: number
}

type CloudStats = {
  batchesIngested: number
  sensorEventsPersisted: number
  duplicateBatchesReplayed: number
  checksumErrors: number
  rawBytes: number
  compressedBytes: number
  recentBatches?: Batch[]
}

type Reading = {
  id?: number
  batchId?: string
  nodeId: string
  sensorType: string
  readingValue: number
  anomaly: boolean
  timestampMs: number
}

type Batch = {
  batchId: string
  gatewayId?: string
  encodingType: string
  encoding?: string
  compressionType: string
  compression?: string
  eventCount: number
  events?: number
  uncompressedBytes?: number
  rawBytes?: number
  compressedBytes: number
  crc32Checksum?: number
}

type TierStatus = 'LIVE' | 'OFFLINE'

const number = (value?: number, digits = 0) =>
  (value ?? 0).toLocaleString(undefined, { maximumFractionDigits: digits })

function StatusBadge({ label, status }: { label: string; status: TierStatus }) {
  return (
    <div className="status-badge">
      <span className={status === 'LIVE' ? 'live-dot' : 'offline-dot'} />
      <b>{label}</b>
      <span>{status === 'LIVE' ? 'LIVE' : 'Service Offline — Waiting for backend...'}</span>
    </div>
  )
}

function KpiCard({
  title,
  value,
  detail,
  icon,
  accent = 'teal',
}: {
  title: string
  value: string
  detail: string
  icon: React.ReactNode
  accent?: 'teal' | 'amber' | 'rose'
}) {
  return (
    <article className={`kpi-card ${accent}`}>
      <div className="kpi-head">
        <span>{title}</span>
        <div className="kpi-icon">{icon}</div>
      </div>
      <strong>{value}</strong>
      <p>{detail}</p>
    </article>
  )
}

function App() {
  const [edge, setEdge] = useState<EdgeStats | null>(null)
  const [fog, setFog] = useState<FogStats | null>(null)
  const [cloud, setCloud] = useState<CloudStats | null>(null)
  const [readings, setReadings] = useState<Reading[]>([])
  const [statuses, setStatuses] = useState<Record<'edge' | 'fog' | 'cloud', TierStatus>>({
    edge: 'OFFLINE',
    fog: 'OFFLINE',
    cloud: 'OFFLINE',
  })
  const [bandwidthHistory, setBandwidthHistory] = useState<Array<{ time: string; raw: number; compressed: number }>>([])
  const [nodeCount, setNodeCount] = useState(10)
  const [emitInterval, setEmitInterval] = useState(500)
  const [epsilon, setEpsilon] = useState(0.15)
  const [codec, setCodec] = useState('AUTO')
  const [failMode, setFailMode] = useState(false)
  const [latency, setLatency] = useState(0)
  const lastBandRef = useRef({ raw: 0, compressed: 0 })

  useEffect(() => {
    let active = true

    const poll = async () => {
      const edgeTask = getJson<EdgeStats>(API.edge, '/api/edge/stats')
        .then((value) => { if (active) { setEdge(value); setStatuses((s) => ({ ...s, edge: 'LIVE' })) } })
        .catch(() => { if (active) setStatuses((s) => ({ ...s, edge: 'OFFLINE' })) })
      const fogTask = getJson<FogStats>(API.fog, '/api/fog/stats')
        .then((value) => { if (active) { setFog(value); setStatuses((s) => ({ ...s, fog: 'LIVE' })) } })
        .catch(() => { if (active) setStatuses((s) => ({ ...s, fog: 'OFFLINE' })) })
      const cloudTask = getJson<CloudStats>(API.cloud, '/api/cloud/stats')
        .then((value) => { if (active) { setCloud(value); setStatuses((s) => ({ ...s, cloud: 'LIVE' })) } })
        .catch(() => { if (active) setStatuses((s) => ({ ...s, cloud: 'OFFLINE' })) })
      const readingsTask = getJson<Reading[]>(API.cloud, '/api/cloud/readings/recent?limit=120')
        .then((value) => { if (active) setReadings(Array.isArray(value) ? value : []) })
        .catch(() => undefined)
      await Promise.all([edgeTask, fogTask, cloudTask, readingsTask])
    }

    poll()
    const timer = window.setInterval(poll, 1500)
    return () => { active = false; window.clearInterval(timer) }
  }, [])

  useEffect(() => {
    let source: EventSource | null = null
    let reconnect: number | null = null
    const connect = () => {
      source = new EventSource(`${API.cloud}/api/cloud/stream`)
      source.addEventListener('pipeline', (event) => {
        const snapshot = JSON.parse((event as MessageEvent).data) as CloudStats & { recentBatches?: Batch[] }
        setCloud((previous) => ({ ...previous, ...snapshot }))
      })
      source.onerror = () => {
        source?.close()
        reconnect = window.setTimeout(connect, 3000)
      }
    }
    connect()
    return () => { source?.close(); if (reconnect) window.clearTimeout(reconnect) }
  }, [])

  useEffect(() => {
    const raw = fog?.rawBytes ?? cloud?.rawBytes ?? 0
    const compressed = fog?.compressedBytes ?? cloud?.compressedBytes ?? 0
    if (raw === lastBandRef.current.raw && compressed === lastBandRef.current.compressed) return
    lastBandRef.current = { raw, compressed }
    const point = {
      time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
      raw: Math.round(raw / 1024),
      compressed: Math.round(compressed / 1024),
    }
    setBandwidthHistory((history) => [...history, point].slice(-24))
  }, [fog, cloud])

  const telemetry = useMemo(() => {
    const latest = [...readings].reverse().slice(-80)
    const points = new Map<number, Record<string, number | string | boolean>>()
    latest.forEach((reading) => {
      const key = reading.timestampMs
      const point = points.get(key) ?? { time: new Date(key).toLocaleTimeString([], { hour12: false }) }
      point[reading.sensorType] = reading.readingValue
      if (reading.anomaly) point[`${reading.sensorType}_anomaly`] = reading.readingValue
      points.set(key, point)
    })
    return [...points.values()]
  }, [readings])

  const batches = cloud?.recentBatches ?? []
  const anomalies = readings.filter((reading) => reading.anomaly).slice(-8).reverse()
  const edgeReduction = edge?.reductionPercent ?? 0
  const compressionRatio = fog?.compressedBytes
    ? (fog.rawBytes ?? 0) / fog.compressedBytes
    : 1
  const circuitState = fog?.circuitBreakerState ?? 'UNKNOWN'
  const crcHealthy = (cloud?.checksumErrors ?? 0) === 0

  const updateEdge = async (partial: Record<string, number | boolean>) =>
    postJson<EdgeStats>(API.edge, '/api/edge/control', partial)

  const edgeControl = async (partial: Record<string, number | boolean>) => {
    try {
      const value = await updateEdge(partial)
      setEdge(value)
    } catch { /* status polling exposes service availability */ }
  }

  const setCompression = async (mode: string) => {
    setCodec(mode)
    try { await postJson(API.fog, '/api/fog/compression-override', { mode }) }
    catch { /* status polling exposes service availability */ }
  }

  const updateChaos = async (partial: { failMode?: boolean; latencyMs?: number }) => {
    try { await postJson(API.cloud, '/api/cloud/chaos', partial) }
    catch { /* status polling exposes service availability */ }
  }

  return (
    <main className="console">
      <header className="topbar">
        <div className="brand">
          <div className="brand-mark"><Waves size={20} /></div>
          <div><b>STREAMFORGE</b><span>Telemetry Operations</span></div>
        </div>
        <div className="status-list">
          <StatusBadge label="EDGE :8083" status={statuses.edge} />
          <StatusBadge label="FOG :8082" status={statuses.fog} />
          <StatusBadge label="CLOUD :8081" status={statuses.cloud} />
        </div>
      </header>

      <section className="pipeline-strip">
        <div className="pipeline-node"><Radio size={16} /><span>Edge</span><b>{number(edge?.forwarded)} events</b></div>
        <ArrowRight className="flow-arrow" size={18} />
        <div className="pipeline-node"><Server size={16} /><span>Fog WAL / Batcher</span><b>{number(fog?.bytesSaved)} bytes saved</b></div>
        <ArrowRight className="flow-arrow" size={18} />
        <div className="pipeline-node"><ShieldCheck size={16} /><span>Cloud Ingest</span><b>{number(cloud?.sensorEventsPersisted)} persisted</b></div>
      </section>

      <section className="kpi-grid">
        <KpiCard title="Edge Data Reduction" value={`${edgeReduction.toFixed(1)}%`} detail={`${number(edge?.anomaliesDetected)} anomalies bypassed`} icon={<Gauge size={17} />} />
        <KpiCard title="Fog Compression Ratio" value={`${compressionRatio.toFixed(2)}x`} detail={fog?.activeEncodingCompression ?? 'Waiting for codec'} icon={<Activity size={17} />} accent="amber" />
        <KpiCard title="Resilience & WAL" value={circuitState} detail={`${number(fog?.pendingWalSegmentCount)} segments · ${number(fog?.pendingWalBytes)} bytes`} icon={<Zap size={17} />} accent={circuitState === 'OPEN' ? 'rose' : 'teal'} />
        <KpiCard title="Cloud Persisted" value={number(cloud?.batchesIngested)} detail={`${number(cloud?.sensorEventsPersisted)} events · CRC32 ${crcHealthy ? 'VALID' : 'ERROR'}`} icon={<Server size={17} />} accent="amber" />
      </section>

      <section className="chart-grid">
        <article className="panel">
          <div className="panel-head"><div><span>Bandwidth Efficiency</span><b>Raw vs. compressed bytes</b></div><em>LIVE</em></div>
          <div className="chart">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={bandwidthHistory} margin={{ top: 12, right: 8, bottom: 0, left: -16 }}>
                <defs>
                  <linearGradient id="raw" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor="#14B8A6" stopOpacity={0.35}/><stop offset="95%" stopColor="#14B8A6" stopOpacity={0}/></linearGradient>
                </defs>
                <CartesianGrid stroke="#1E293B" vertical={false} />
                <XAxis dataKey="time" tick={{ fill: '#64748B', fontSize: 10 }} tickLine={false} axisLine={false} />
                <YAxis tick={{ fill: '#64748B', fontSize: 10 }} tickLine={false} axisLine={false} />
                <Tooltip contentStyle={{ background: '#111827', border: '1px solid #1E293B', borderRadius: 6 }} />
                <Area type="monotone" dataKey="raw" name="Raw KB" stroke="#14B8A6" fill="url(#raw)" strokeWidth={2} />
                <Area type="monotone" dataKey="compressed" name="Compressed KB" stroke="#F59E0B" fill="transparent" strokeWidth={2} />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </article>

        <article className="panel">
          <div className="panel-head"><div><span>Sensor Telemetry</span><b>Temperature · vibration · pressure</b></div><em>ANOMALY STREAM</em></div>
          <div className="chart">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={telemetry} margin={{ top: 12, right: 8, bottom: 0, left: -16 }}>
                <CartesianGrid stroke="#1E293B" vertical={false} />
                <XAxis dataKey="time" tick={{ fill: '#64748B', fontSize: 10 }} tickLine={false} axisLine={false} />
                <YAxis tick={{ fill: '#64748B', fontSize: 10 }} tickLine={false} axisLine={false} />
                <Tooltip contentStyle={{ background: '#111827', border: '1px solid #1E293B', borderRadius: 6 }} />
                <Line type="monotone" dataKey="temperature" stroke="#14B8A6" dot={false} strokeWidth={2} />
                <Line type="monotone" dataKey="vibration" stroke="#F59E0B" dot={false} strokeWidth={2} />
                <Line type="monotone" dataKey="pressure" stroke="#8B5CF6" dot={false} strokeWidth={2} />
                {telemetry.filter((p) => p.temperature_anomaly !== undefined).map((p, i) => (
                  <ReferenceDot key={`temp-${i}`} x={p.time as string} y={p.temperature_anomaly as number} r={4} fill="#F43F5E" stroke="#0B1120" />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
        </article>
      </section>

      <section className="control-grid">
        <article className="panel control-panel">
          <div className="panel-head"><div><span>Edge Simulator</span><b>Virtual sensor workers</b></div></div>
          <label>Node count <b>{nodeCount}</b><input type="range" min="1" max="50" value={nodeCount} onChange={(e) => setNodeCount(Number(e.target.value))} onMouseUp={() => edgeControl({ nodeCount })} /></label>
          <label>Emit interval <b>{emitInterval} ms</b><input type="range" min="100" max="2000" step="50" value={emitInterval} onChange={(e) => setEmitInterval(Number(e.target.value))} onMouseUp={() => edgeControl({ emitIntervalMs: emitInterval })} /></label>
          <label>Deadband epsilon <b>{epsilon.toFixed(2)}</b><input type="range" min="0" max="1" step="0.01" value={epsilon} onChange={(e) => setEpsilon(Number(e.target.value))} onMouseUp={() => edgeControl({ deadbandEpsilon: epsilon })} /></label>
          <button className="danger-button" onClick={() => edgeControl({ anomalyBurst: true, anomalyBurstCount: 20 })}>Trigger Anomaly Spike</button>
        </article>

        <article className="panel control-panel">
          <div className="panel-head"><div><span>Fog Codec A/B</span><b>Live compression override</b></div></div>
          <div className="codec-buttons">
            {['AUTO', 'RAW_NONE', 'LZ4', 'ZSTD', 'GZIP'].map((mode) => (
              <button key={mode} className={codec === mode ? 'selected' : ''} onClick={() => setCompression(mode)}>{mode}</button>
            ))}
          </div>
        </article>

        <article className="panel control-panel">
          <div className="panel-head"><div><span>Cloud Chaos</span><b>Fault-tolerance simulator</b></div></div>
          <label className="toggle-row">Simulate Cloud Outage
            <button className={`switch ${failMode ? 'on' : ''}`} aria-label="Toggle cloud outage" onClick={() => { const next = !failMode; setFailMode(next); updateChaos({ failMode: next }) }}><span /></button>
          </label>
          <label>Latency <b>{latency} ms</b><input type="range" min="0" max="3000" step="50" value={latency} onChange={(e) => setLatency(Number(e.target.value))} onMouseUp={() => updateChaos({ latencyMs: latency })} /></label>
        </article>
      </section>

      <section className="panel table-panel">
        <div className="panel-head"><div><span>Recent Batches & Audit</span><b>Latest persistence and integrity records</b></div></div>
        <div className="table-wrap">
          <table>
            <thead><tr><th>Batch ID</th><th>Encoding</th><th>Compression</th><th>Events</th><th>Raw → Wire</th><th>Ratio</th><th>CRC32</th></tr></thead>
            <tbody>
              {batches.slice(0, 8).map((batch) => {
                const raw = batch.uncompressedBytes ?? batch.rawBytes ?? 0
                return (
                  <tr key={batch.batchId}>
                    <td>{batch.batchId?.slice(0, 8) ?? '—'}</td>
                    <td>{batch.encodingType ?? batch.encoding ?? '—'}</td>
                    <td>{batch.compressionType ?? batch.compression ?? '—'}</td>
                    <td>{number(batch.eventCount ?? batch.events)}</td>
                    <td>{number(raw)} → {number(batch.compressedBytes)} B</td>
                    <td>{raw && batch.compressedBytes ? (raw / batch.compressedBytes).toFixed(2) + 'x' : '—'}</td>
                    <td><span className={crcHealthy ? 'valid' : 'invalid'}>{crcHealthy ? 'VALID' : 'CHECK'}</span></td>
                  </tr>
                )
              })}
              {batches.length === 0 && <tr><td colSpan={7} className="empty">Waiting for cloud batches…</td></tr>}
            </tbody>
          </table>
        </div>
        <div className="anomaly-list">
          <b>Recent Anomalies</b>
          {anomalies.length ? anomalies.map((reading, index) => (
            <span key={`${reading.timestampMs}-${index}`}><i />{reading.sensorType} · {reading.readingValue.toFixed(2)} · {new Date(reading.timestampMs).toLocaleTimeString()}</span>
          )) : <span className="muted">No flagged anomalies</span>}
        </div>
      </section>
    </main>
  )
}

export default App
