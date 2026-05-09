'use client';

import { useState, useEffect, useRef, useCallback } from 'react';
import dynamic from 'next/dynamic';
import * as echarts from 'echarts';
import type { DistrictStat, Hotspot } from './components/DistrictMap';

const DistrictMap = dynamic(() => import('./components/DistrictMap'), {
  ssr: false,
  loading: () => <MapPlaceholder />,
});

// ── Types ──────────────────────────────────────────────────────────────────

interface CrimeEvent {
  event_id: string;
  victim_id?: string;
  incident_date: string;
  incident_time?: string;
  location?: string;
  district?: string;
  injury_type?: string;
  severity?: number;
  processed_timestamp: number;
  processed_timestamp_api?: number;
}

interface Stats {
  connected_clients: number;
  total_events: number;
  timestamp: number;
}

interface Trend {
  hour: string;
  event_count: number;
  avg_severity: number;
}

interface ArrestRate {
  primary_type: string;
  total_crimes: number;
  arrest_rate: number;
}

interface CrimeTrendMonthly {
  month: number;
  crime_count: number;
}

interface Correlation {
  group_key: string | null;
  metric_a: number | null;
  metric_b: number | null;
}

// ── Constants ──────────────────────────────────────────────────────────────

const API = 'http://localhost:8081';
const WS = 'ws://localhost:8081/ws';

const SEV: Record<number, { label: string; color: string; dim: string }> = {
  5: { label: 'CRITICAL', color: '#ff4e42', dim: 'rgba(255,78,66,0.15)' },
  4: { label: 'HIGH', color: '#ff7b39', dim: 'rgba(255,123,57,0.12)' },
  3: { label: 'MEDIUM', color: '#ffd93d', dim: 'rgba(255,217,61,0.10)' },
  2: { label: 'LOW', color: '#3fb950', dim: 'rgba(63,185,80,0.10)' },
  1: { label: 'INFO', color: '#4d96ff', dim: 'rgba(77,150,255,0.10)' },
};

const sev = (n?: number) => SEV[n ?? 1] ?? SEV[1];

// ── Reusable ECharts hook ─────────────────────────────────────────────────

function useEChart(
  ref: React.RefObject<HTMLDivElement | null>,
  getOption: () => echarts.EChartsCoreOption,
  deps: unknown[]
) {
  useEffect(() => {
    const el = ref.current;
    if (!el) return;

    let chart = echarts.getInstanceByDom(el) as echarts.ECharts | undefined;
    if (!chart) chart = echarts.init(el, undefined, { renderer: 'canvas' });

    chart.setOption(getOption(), { notMerge: true });

    const ro = new ResizeObserver(() => chart?.resize());
    ro.observe(el);
    return () => ro.disconnect();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);
}

// ── Sub-components ─────────────────────────────────────────────────────────

function MapPlaceholder() {
  return (
    <div
      className="w-full h-full flex items-center justify-center text-xs"
      style={{ color: 'var(--text-dim)' }}
    >
      Loading map…
    </div>
  );
}

interface MetricCardProps {
  label: string;
  value: number | string;
  sub?: string;
  color: string;
  pulse?: boolean;
}
function MetricCard({ label, value, sub, color, pulse }: MetricCardProps) {
  const prevRef = useRef(value);
  const [popKey, setPopKey] = useState(0);

  useEffect(() => {
    if (prevRef.current !== value) {
      prevRef.current = value;
      setPopKey((k) => k + 1);
    }
  }, [value]);

  return (
    <div
      className={`rounded border p-3 flex flex-col gap-1 ${pulse ? 'glow-pulse' : 'glow-cyan'}`}
      style={{ background: 'var(--bg-card)', borderColor: 'var(--border)' }}
    >
      <span
        className="text-[10px] uppercase tracking-widest"
        style={{ color: 'var(--text-muted)' }}
      >
        {label}
      </span>
      <span
        key={popKey}
        className="text-2xl font-bold animate-pop tabular-nums"
        style={{ color }}
      >
        {value}
      </span>
      {sub && (
        <span className="text-[10px]" style={{ color: 'var(--text-dim)' }}>
          {sub}
        </span>
      )}
    </div>
  );
}

function SeverityBadge({ severity }: { severity?: number }) {
  const s = sev(severity);
  return (
    <span
      className="text-[9px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
      style={{
        color: s.color,
        background: s.dim,
        border: `1px solid ${s.color}40`,
      }}
    >
      {s.label}
    </span>
  );
}

function AlertCard({ event }: { event: CrimeEvent }) {
  const s = sev(event.severity);
  const ts = new Date(
    event.processed_timestamp_api ?? event.processed_timestamp
  ).toLocaleTimeString('en-US', { hour12: false });

  return (
    <div
      className="slide-in rounded border-l-2 px-3 py-2 text-xs flex flex-col gap-1.5"
      style={{
        background: s.dim,
        borderLeftColor: s.color,
        borderTop: '1px solid rgba(255,255,255,0.04)',
        borderRight: '1px solid rgba(255,255,255,0.04)',
        borderBottom: '1px solid rgba(255,255,255,0.04)',
      }}
    >
      <div className="flex items-center justify-between gap-2">
        <code className="text-[10px] opacity-50">
          {event.event_id.slice(0, 12)}…
        </code>
        <div className="flex items-center gap-2">
          <SeverityBadge severity={event.severity} />
          <span className="opacity-40">{ts}</span>
        </div>
      </div>
      <div
        className="grid grid-cols-2 gap-x-4 gap-y-0.5"
        style={{ color: 'var(--text-muted)' }}
      >
        {event.district && (
          <div>
            <span className="opacity-50">DIST </span>
            <span style={{ color: 'var(--text)' }}>{event.district}</span>
          </div>
        )}
        {event.incident_date && (
          <div>
            <span className="opacity-50">DATE </span>
            <span style={{ color: 'var(--text)' }}>{event.incident_date}</span>
          </div>
        )}
        {event.injury_type && (
          <div className="col-span-2">
            <span className="opacity-50">TYPE </span>
            <span style={{ color: s.color }}>{event.injury_type}</span>
          </div>
        )}
      </div>
    </div>
  );
}

// ── Charts ─────────────────────────────────────────────────────────────────

const CHART_BASE: echarts.EChartsCoreOption = {
  backgroundColor: 'transparent',
  textStyle: { color: '#8b949e', fontFamily: 'monospace', fontSize: 10 },
  animation: false,
};

function TrendsChart({ trends }: { trends: Trend[] }) {
  const ref = useRef<HTMLDivElement>(null);

  useEChart(
    ref,
    () => ({
      ...CHART_BASE,
      grid: { left: 36, right: 8, top: 8, bottom: 28 },
      xAxis: {
        type: 'category',
        data: trends.map((t) => {
          const h = new Date(t.hour);
          return `${h.getHours().toString().padStart(2, '0')}:00`;
        }),
        axisLine: { lineStyle: { color: '#1e2d3d' } },
        axisTick: { show: false },
        axisLabel: { color: '#8b949e', fontSize: 9, interval: 'auto' },
      },
      yAxis: {
        type: 'value',
        axisLine: { show: false },
        axisTick: { show: false },
        splitLine: { lineStyle: { color: '#1e2d3d', type: 'dashed' } },
        axisLabel: { color: '#8b949e', fontSize: 9 },
        minInterval: 1,
      },
      series: [
        {
          type: 'line',
          data: trends.map((t) => t.event_count),
          smooth: true,
          symbol: 'none',
          lineStyle: { color: '#00d4ff', width: 2 },
          areaStyle: {
            color: {
              type: 'linear',
              x: 0,
              y: 0,
              x2: 0,
              y2: 1,
              colorStops: [
                { offset: 0, color: 'rgba(0,212,255,0.35)' },
                { offset: 1, color: 'rgba(0,212,255,0.0)' },
              ],
            },
          },
        },
      ],
      tooltip: {
        trigger: 'axis',
        backgroundColor: '#0d1117',
        borderColor: '#1e2d3d',
        textStyle: { color: '#c9d1d9', fontFamily: 'monospace', fontSize: 11 },
      },
    }),
    [trends]
  );

  return <div ref={ref} className="w-full h-full" />;
}

function SeverityChart({ events }: { events: CrimeEvent[] }) {
  const ref = useRef<HTMLDivElement>(null);
  const counts = [1, 2, 3, 4, 5].map(
    (s) => events.filter((e) => e.severity === s).length
  );
  const total = counts.reduce((a, b) => a + b, 0);

  useEChart(
    ref,
    () => ({
      ...CHART_BASE,
      series: [
        {
          type: 'pie',
          radius: ['52%', '78%'],
          center: ['50%', '52%'],
          data: [1, 2, 3, 4, 5]
            .map((s) => ({
              value: counts[s - 1],
              name: SEV[s].label,
              itemStyle: { color: SEV[s].color },
            }))
            .filter((d) => d.value > 0),
          label: { show: false },
          emphasis: {
            label: {
              show: true,
              color: '#c9d1d9',
              fontSize: 11,
              fontWeight: 'bold',
              fontFamily: 'monospace',
            },
          },
        },
      ],
      graphic: [
        {
          type: 'text',
          left: 'center',
          top: 'middle',
          style: {
            text: total > 0 ? String(total) : '—',
            fill: '#c9d1d9',
            fontSize: total >= 1000 ? 14 : 18,
            fontWeight: 'bold',
            fontFamily: 'monospace',
          },
        },
      ],
      tooltip: {
        trigger: 'item',
        backgroundColor: '#0d1117',
        borderColor: '#1e2d3d',
        textStyle: { color: '#c9d1d9', fontFamily: 'monospace', fontSize: 11 },
        formatter: '{b}: {c} ({d}%)',
      },
    }),
    [events.length]
  );

  return <div ref={ref} className="w-full h-full" />;
}

function DistrictChart({ districts }: { districts: DistrictStat[] }) {
  const ref = useRef<HTMLDivElement>(null);
  const top = [...districts].slice(0, 8).reverse();

  useEChart(
    ref,
    () => ({
      ...CHART_BASE,
      grid: { left: 44, right: 28, top: 4, bottom: 20 },
      xAxis: {
        type: 'value',
        axisLabel: { color: '#8b949e', fontSize: 9 },
        splitLine: { lineStyle: { color: '#1e2d3d', type: 'dashed' } },
        minInterval: 1,
      },
      yAxis: {
        type: 'category',
        data: top.map((d) => d.district ?? '?'),
        axisLine: { lineStyle: { color: '#1e2d3d' } },
        axisTick: { show: false },
        axisLabel: { color: '#8b949e', fontSize: 9 },
      },
      series: [
        {
          type: 'bar',
          data: top.map((d) => ({
            value: d.total_events,
            itemStyle: {
              color:
                d.avg_severity >= 4
                  ? '#ff4e42'
                  : d.avg_severity >= 3
                    ? '#ffd93d'
                    : '#00d4ff',
              borderRadius: [0, 2, 2, 0],
            },
          })),
          barMaxWidth: 14,
          label: {
            show: true,
            position: 'right',
            color: '#8b949e',
            fontSize: 9,
            fontFamily: 'monospace',
          },
        },
      ],
      tooltip: {
        trigger: 'axis',
        backgroundColor: '#0d1117',
        borderColor: '#1e2d3d',
        textStyle: { color: '#c9d1d9', fontFamily: 'monospace', fontSize: 11 },
      },
    }),
    [districts]
  );

  return <div ref={ref} className="w-full h-full" />;
}

function ArrestRatesChart({ data }: { data: ArrestRate[] }) {
  const ref = useRef<HTMLDivElement>(null);
  const top = [...data].slice(0, 8).reverse();
  useEChart(
    ref,
    () => ({
      ...CHART_BASE,
      grid: { left: 130, right: 40, top: 4, bottom: 20 },
      xAxis: {
        type: 'value',
        max: 1,
        axisLabel: {
          color: '#8b949e',
          fontSize: 9,
          formatter: (v: number) => `${(v * 100).toFixed(0)}%`,
        },
        splitLine: { lineStyle: { color: '#1e2d3d', type: 'dashed' } },
      },
      yAxis: {
        type: 'category',
        data: top.map((d) => d.primary_type),
        axisLine: { lineStyle: { color: '#1e2d3d' } },
        axisTick: { show: false },
        axisLabel: { color: '#8b949e', fontSize: 8 },
      },
      series: [
        {
          type: 'bar',
          data: top.map((d) => ({
            value: d.arrest_rate,
            itemStyle: {
              color:
                d.arrest_rate > 0.5
                  ? '#ff4e42'
                  : d.arrest_rate > 0.25
                    ? '#ffd93d'
                    : '#3fb950',
              borderRadius: [0, 2, 2, 0],
            },
          })),
          barMaxWidth: 12,
          label: {
            show: true,
            position: 'right',
            color: '#8b949e',
            fontSize: 8,
            fontFamily: 'monospace',
            formatter: (p: { value: number }) =>
              `${(p.value * 100).toFixed(1)}%`,
          },
        },
      ],
      tooltip: {
        trigger: 'axis',
        backgroundColor: '#0d1117',
        borderColor: '#1e2d3d',
        textStyle: { color: '#c9d1d9', fontFamily: 'monospace', fontSize: 11 },
      },
    }),
    [data]
  );
  return <div ref={ref} className="w-full h-full" />;
}

function CrimeTrendsMonthlyChart({ data }: { data: CrimeTrendMonthly[] }) {
  const ref = useRef<HTMLDivElement>(null);
  const MONTHS = [
    'Jan',
    'Feb',
    'Mar',
    'Apr',
    'May',
    'Jun',
    'Jul',
    'Aug',
    'Sep',
    'Oct',
    'Nov',
    'Dec',
  ];
  const byMonth = Array.from({ length: 12 }, (_, i) => {
    const rows = data.filter((d) => d.month === i + 1);
    return rows.reduce((s, r) => s + r.crime_count, 0);
  });
  useEChart(
    ref,
    () => ({
      ...CHART_BASE,
      grid: { left: 36, right: 8, top: 8, bottom: 28 },
      xAxis: {
        type: 'category',
        data: MONTHS,
        axisLine: { lineStyle: { color: '#1e2d3d' } },
        axisTick: { show: false },
        axisLabel: { color: '#8b949e', fontSize: 9 },
      },
      yAxis: {
        type: 'value',
        axisLabel: { color: '#8b949e', fontSize: 9 },
        splitLine: { lineStyle: { color: '#1e2d3d', type: 'dashed' } },
        minInterval: 1,
      },
      series: [
        {
          type: 'bar',
          data: byMonth.map((v) => ({
            value: v,
            itemStyle: { color: '#7b5ea7', borderRadius: [2, 2, 0, 0] },
          })),
          barMaxWidth: 20,
        },
      ],
      tooltip: {
        trigger: 'axis',
        backgroundColor: '#0d1117',
        borderColor: '#1e2d3d',
        textStyle: { color: '#c9d1d9', fontFamily: 'monospace', fontSize: 11 },
      },
    }),
    [data]
  );
  return <div ref={ref} className="w-full h-full" />;
}

function CorrelationChart({ data }: { data: Correlation[] }) {
  const ref = useRef<HTMLDivElement>(null);
  const top = data
    .filter((d) => d.group_key && d.metric_a != null && d.metric_b != null)
    .slice(0, 10)
    .reverse();
  useEChart(
    ref,
    () => ({
      ...CHART_BASE,
      grid: { left: 40, right: 8, top: 8, bottom: 28 },
      legend: {
        data: ['Arrest Rate', 'Violence Rate'],
        textStyle: { color: '#8b949e', fontSize: 8 },
        top: 0,
      },
      xAxis: {
        type: 'category',
        data: top.map((d) => `D${d.group_key}`),
        axisLine: { lineStyle: { color: '#1e2d3d' } },
        axisTick: { show: false },
        axisLabel: { color: '#8b949e', fontSize: 8 },
      },
      yAxis: {
        type: 'value',
        axisLabel: { color: '#8b949e', fontSize: 8 },
        splitLine: { lineStyle: { color: '#1e2d3d', type: 'dashed' } },
      },
      series: [
        {
          name: 'Arrest Rate',
          type: 'bar',
          data: top.map((d) => d.metric_a),
          itemStyle: { color: '#4d96ff' },
          barMaxWidth: 10,
        },
        {
          name: 'Violence Rate',
          type: 'bar',
          data: top.map((d) => d.metric_b),
          itemStyle: { color: '#ff4e42' },
          barMaxWidth: 10,
        },
      ],
      tooltip: {
        trigger: 'axis',
        backgroundColor: '#0d1117',
        borderColor: '#1e2d3d',
        textStyle: { color: '#c9d1d9', fontFamily: 'monospace', fontSize: 11 },
      },
    }),
    [data]
  );
  return <div ref={ref} className="w-full h-full" />;
}

function EmptyChart({ label }: { label: string }) {
  return (
    <div
      className="w-full h-full flex items-center justify-center text-[10px]"
      style={{ color: 'var(--text-dim)' }}
    >
      {label}
    </div>
  );
}

// ── Time slider (historical replay) ───────────────────────────────────────

function TimeSlider({
  onReplay,
  onLive,
  isReplay,
}: {
  onReplay: (offset: number, limit: number) => void;
  onLive: () => void;
  isReplay: boolean;
}) {
  const [sliderValue, setSliderValue] = useState(0); // 0 = oldest, 100 = live

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const v = Number(e.target.value);
    setSliderValue(v);
    // Map 0-99 → offset from newest: slider at 0 shows events from oldest, 100 = live
    const offset = Math.round((100 - v) * 4.9); // up to ~490 offset
    onReplay(offset, 50);
  };

  return (
    <div
      className="flex-none px-5 py-2 flex items-center gap-4 border-t"
      style={{ borderColor: 'var(--border)', background: 'var(--bg-card)' }}
    >
      <span
        className="text-[10px] uppercase tracking-widest flex-none"
        style={{ color: 'var(--text-muted)' }}
      >
        Time Replay
      </span>

      <input
        type="range"
        min={0}
        max={100}
        value={sliderValue}
        onChange={handleChange}
        disabled={!isReplay && sliderValue === 100}
        className="flex-1"
        style={{ accentColor: 'var(--cyan)', cursor: 'pointer' }}
      />

      <span
        className="text-[10px] flex-none tabular-nums"
        style={{ color: isReplay ? 'var(--yellow)' : 'var(--text-dim)' }}
      >
        {sliderValue === 100
          ? 'LIVE'
          : `T-${Math.round((100 - sliderValue) * 4.9)} events`}
      </span>

      <button
        onClick={() => {
          setSliderValue(100);
          onLive();
        }}
        className="text-[10px] px-2 py-0.5 rounded border uppercase tracking-wider"
        style={{
          color: isReplay ? 'var(--cyan)' : 'var(--text-dim)',
          borderColor: isReplay ? 'var(--cyan)' : 'var(--border)',
          background: isReplay ? 'var(--cyan-dim)' : 'transparent',
          cursor: 'pointer',
        }}
      >
        ↩ Live
      </button>
    </div>
  );
}

// ── Main page ──────────────────────────────────────────────────────────────

export default function Home() {
  const [events, setEvents] = useState<CrimeEvent[]>([]);
  const [replayEvents, setReplayEvents] = useState<CrimeEvent[]>([]);
  const [isReplay, setIsReplay] = useState(false);
  const [stats, setStats] = useState<Stats>({
    connected_clients: 0,
    total_events: 0,
    timestamp: 0,
  });
  const [wsStatus, setWsStatus] = useState<
    'connecting' | 'connected' | 'disconnected'
  >('disconnected');
  const [districts, setDistricts] = useState<DistrictStat[]>([]);
  const [trends, setTrends] = useState<Trend[]>([]);
  const [hotspots, setHotspots] = useState<Hotspot[]>([]);
  const [arrestRates, setArrestRates] = useState<ArrestRate[]>([]);
  const [crimeTrendsMonthly, setCrimeTrendsMonthly] = useState<
    CrimeTrendMonthly[]
  >([]);
  const [correlations, setCorrelations] = useState<Correlation[]>([]);
  const [clock, setClock] = useState('');

  const tsBuffer = useRef<number[]>([]);

  // Historical replay handlers
  const handleReplay = useCallback(async (offset: number, limit: number) => {
    try {
      const r = await fetch(
        `${API}/events/history?limit=${limit}&offset=${offset}`
      );
      if (r.ok) {
        const data = await r.json();
        setReplayEvents(data.events ?? []);
        setIsReplay(true);
      }
    } catch {}
  }, []);

  const handleLive = useCallback(() => {
    setIsReplay(false);
    setReplayEvents([]);
  }, []);

  // Clock
  useEffect(() => {
    const tick = () =>
      setClock(new Date().toLocaleTimeString('en-US', { hour12: false }));
    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, []);

  // WebSocket
  const connectWS = useCallback(() => {
    setWsStatus('connecting');
    const ws = new WebSocket(WS);

    ws.onopen = () => setWsStatus('connected');

    ws.onmessage = (e) => {
      const msg = JSON.parse(e.data);
      if (msg.type === 'event') {
        const ev = msg.data as CrimeEvent;
        tsBuffer.current.push(Date.now());
        setEvents((prev) => [ev, ...prev.slice(0, 149)]);
      }
    };

    ws.onclose = () => {
      setWsStatus('disconnected');
      setTimeout(connectWS, 3000);
    };

    ws.onerror = () => ws.close();
  }, []);

  useEffect(() => {
    connectWS();
  }, [connectWS]);

  // Stats poll (2 s)
  useEffect(() => {
    const poll = async () => {
      try {
        const r = await fetch(`${API}/stats`);
        if (r.ok) setStats(await r.json());
      } catch {}
    };
    poll();
    const id = setInterval(poll, 2000);
    return () => clearInterval(id);
  }, []);

  // Districts poll (15 s)
  useEffect(() => {
    const poll = async () => {
      try {
        const r = await fetch(`${API}/districts/summary`);
        if (r.ok) {
          const data = await r.json();
          setDistricts(data.districts ?? []);
        }
      } catch {}
    };
    poll();
    const id = setInterval(poll, 15_000);
    return () => clearInterval(id);
  }, []);

  // Analytics poll (5 min — batch data changes infrequently)
  useEffect(() => {
    const poll = async () => {
      try {
        const [h, a, ct, co] = await Promise.all([
          fetch(`${API}/analytics/hotspots`).then((r) => r.json()),
          fetch(`${API}/analytics/arrest-rates`).then((r) => r.json()),
          fetch(`${API}/analytics/crime-trends`).then((r) => r.json()),
          fetch(`${API}/analytics/correlations?type=district`).then((r) =>
            r.json()
          ),
        ]);
        setHotspots(h.data ?? []);
        setArrestRates(a.data ?? []);
        setCrimeTrendsMonthly(ct.data ?? []);
        setCorrelations(co.data ?? []);
      } catch {}
    };
    poll();
    const id = setInterval(poll, 300_000);
    return () => clearInterval(id);
  }, []);

  // Trends poll (30 s)
  useEffect(() => {
    const poll = async () => {
      try {
        const r = await fetch(`${API}/analytics/trends?hours=24`);
        if (r.ok) {
          const data = await r.json();
          setTrends(data.trends ?? []);
        }
      } catch {}
    };
    poll();
    const id = setInterval(poll, 30_000);
    return () => clearInterval(id);
  }, []);

  // Derived metrics
  const eventsPerMin = (() => {
    const cutoff = Date.now() - 60_000;
    tsBuffer.current = tsBuffer.current.filter((t) => t > cutoff);
    return tsBuffer.current.length;
  })();

  const activeDistricts = new Set(
    events
      .filter(
        (e) =>
          Date.now() - (e.processed_timestamp_api ?? e.processed_timestamp) <
          300_000
      )
      .map((e) => e.district)
      .filter(Boolean)
  ).size;

  const criticalCount = events.filter((e) => (e.severity ?? 0) >= 4).length;

  const displayEvents = isReplay ? replayEvents : events;

  const statusColor =
    wsStatus === 'connected'
      ? '#00d4ff'
      : wsStatus === 'connecting'
        ? '#ffd93d'
        : '#ff4e42';

  return (
    <div
      className="h-screen flex flex-col overflow-hidden scan-line"
      style={{ background: 'var(--bg-base)', color: 'var(--text)' }}
    >
      {/* Header */}
      <header
        className="flex-none flex items-center justify-between px-5 py-2.5 border-b"
        style={{ borderColor: 'var(--border)', background: 'var(--bg-card)' }}
      >
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2">
            <span style={{ color: 'var(--cyan)', fontSize: 18 }}>⬡</span>
            <span className="font-bold tracking-widest text-sm uppercase">
              WhiteChristmas
            </span>
          </div>
          <span style={{ color: 'var(--border-bright)' }}>│</span>
          <span
            className="text-[10px] uppercase tracking-wider"
            style={{ color: 'var(--text-muted)' }}
          >
            Real-Time Crime Analytics &amp; Intelligent Alert System
          </span>
        </div>

        <div className="flex items-center gap-5">
          <div className="flex items-center gap-2">
            <div
              className="w-2 h-2 rounded-full"
              style={{
                background: statusColor,
                boxShadow: `0 0 6px ${statusColor}`,
                animation:
                  wsStatus === 'connected'
                    ? 'glow-pulse 2s ease-in-out infinite'
                    : 'none',
              }}
            />
            <span
              className="text-[10px] uppercase tracking-wider"
              style={{ color: statusColor }}
            >
              {wsStatus}
            </span>
          </div>
          <span
            className="text-[11px] cursor"
            style={{ color: 'var(--text-muted)' }}
          >
            {clock}
          </span>
        </div>
      </header>

      {/* Metrics */}
      <div
        className="flex-none grid grid-cols-4 gap-3 px-5 py-3 border-b"
        style={{ borderColor: 'var(--border)' }}
      >
        <MetricCard
          label="Total Events"
          value={stats.total_events.toLocaleString()}
          sub={`${stats.connected_clients} client${stats.connected_clients !== 1 ? 's' : ''} connected`}
          color="var(--cyan)"
        />
        <MetricCard
          label="Events / Min"
          value={eventsPerMin}
          sub="last 60 seconds"
          color="#7b5ea7"
        />
        <MetricCard
          label="Active Districts"
          value={activeDistricts}
          sub="last 5 minutes"
          color="#4d96ff"
        />
        <MetricCard
          label="Critical Alerts"
          value={criticalCount}
          sub="severity ≥ 4"
          color="var(--red)"
          pulse={criticalCount > 0}
        />
      </div>

      {/* Map + Feed */}
      <div className="flex-1 flex min-h-0">
        {/* Map */}
        <div
          className="flex-none w-[44%] flex flex-col border-r"
          style={{ borderColor: 'var(--border)' }}
        >
          <div
            className="flex-none px-3 py-1.5 flex items-center justify-between border-b"
            style={{
              borderColor: 'var(--border)',
              background: 'var(--bg-card)',
            }}
          >
            <span
              className="text-[10px] uppercase tracking-wider"
              style={{ color: 'var(--text-muted)' }}
            >
              Chicago District Map
            </span>
            <span className="text-[10px]" style={{ color: 'var(--text-dim)' }}>
              {districts.length > 0
                ? `${districts.length} districts reporting`
                : 'awaiting data…'}
            </span>
          </div>
          <div className="flex-1 min-h-0">
            <DistrictMap districts={districts} hotspots={hotspots} />
          </div>
        </div>

        {/* Feed */}
        <div className="flex-1 flex flex-col min-w-0">
          <div
            className="flex-none px-3 py-1.5 flex items-center justify-between border-b"
            style={{
              borderColor: 'var(--border)',
              background: 'var(--bg-card)',
            }}
          >
            <div className="flex items-center gap-2">
              <span
                className="text-[10px] uppercase tracking-wider"
                style={{
                  color: isReplay ? 'var(--yellow)' : 'var(--text-muted)',
                }}
              >
                {isReplay ? '⏮ Historical Replay' : 'Live Alert Feed'}
              </span>
              {isReplay && (
                <span
                  className="text-[9px] px-1 py-0.5 rounded border"
                  style={{
                    color: 'var(--yellow)',
                    borderColor: 'rgba(255,217,61,0.3)',
                    background: 'rgba(255,217,61,0.08)',
                  }}
                >
                  PAUSED
                </span>
              )}
            </div>
            <span className="text-[10px]" style={{ color: 'var(--text-dim)' }}>
              {displayEvents.length > 0
                ? `${displayEvents.length} events`
                : 'waiting for events…'}
            </span>
          </div>
          <div className="flex-1 overflow-y-auto px-3 py-2 space-y-1.5 custom-scroll">
            {displayEvents.length === 0 ? (
              <div
                className="h-full flex items-center justify-center text-xs"
                style={{ color: 'var(--text-dim)' }}
              >
                {isReplay
                  ? 'No events in this time window'
                  : wsStatus === 'connected'
                    ? 'Waiting for events…'
                    : 'Connecting to stream…'}
              </div>
            ) : (
              displayEvents.map((ev, i) => (
                <AlertCard key={`${ev.event_id}-${i}`} event={ev} />
              ))
            )}
          </div>
        </div>
      </div>

      {/* Time Slider */}
      <TimeSlider
        onReplay={handleReplay}
        onLive={handleLive}
        isReplay={isReplay}
      />

      {/* Charts — row 1: live streaming data */}
      <div
        className="flex-none grid grid-cols-3 border-t"
        style={{ height: 185, borderColor: 'var(--border)' }}
      >
        <div
          className="flex flex-col border-r"
          style={{ borderColor: 'var(--border)' }}
        >
          <div
            className="flex-none px-3 pt-2 pb-0.5 text-[10px] uppercase tracking-wider"
            style={{ color: 'var(--text-muted)' }}
          >
            Events / Hour&nbsp;
            <span style={{ color: 'var(--text-dim)' }}>— last 24 h</span>
          </div>
          <div className="flex-1 min-h-0 pb-1 px-1">
            {trends.length > 0 ? (
              <TrendsChart trends={trends} />
            ) : (
              <EmptyChart label="No trend data yet" />
            )}
          </div>
        </div>

        <div
          className="flex flex-col border-r"
          style={{ borderColor: 'var(--border)' }}
        >
          <div
            className="flex-none px-3 pt-2 pb-0.5 text-[10px] uppercase tracking-wider"
            style={{ color: 'var(--text-muted)' }}
          >
            Severity Profile&nbsp;
            <span style={{ color: 'var(--text-dim)' }}>— live feed</span>
          </div>
          <div className="flex-1 min-h-0 pb-1">
            {events.length > 0 ? (
              <SeverityChart events={events} />
            ) : (
              <EmptyChart label="No events yet" />
            )}
          </div>
        </div>

        <div className="flex flex-col">
          <div
            className="flex-none px-3 pt-2 pb-0.5 text-[10px] uppercase tracking-wider"
            style={{ color: 'var(--text-muted)' }}
          >
            Top Districts&nbsp;
            <span style={{ color: 'var(--text-dim)' }}>— all time</span>
          </div>
          <div className="flex-1 min-h-0 pb-1">
            {districts.length > 0 ? (
              <DistrictChart districts={districts} />
            ) : (
              <EmptyChart label="No district data yet" />
            )}
          </div>
        </div>
      </div>

      {/* Charts — row 2: batch analytics */}
      <div
        className="flex-none grid grid-cols-3 border-t"
        style={{ height: 185, borderColor: 'var(--border)' }}
      >
        <div
          className="flex flex-col border-r"
          style={{ borderColor: 'var(--border)' }}
        >
          <div
            className="flex-none px-3 pt-2 pb-0.5 text-[10px] uppercase tracking-wider"
            style={{ color: 'var(--text-muted)' }}
          >
            Crime Trends&nbsp;
            <span style={{ color: 'var(--text-dim)' }}>— by month</span>
          </div>
          <div className="flex-1 min-h-0 pb-1 px-1">
            {crimeTrendsMonthly.length > 0 ? (
              <CrimeTrendsMonthlyChart data={crimeTrendsMonthly} />
            ) : (
              <EmptyChart label="Run BatchAnalytics to populate" />
            )}
          </div>
        </div>
        <div
          className="flex flex-col border-r"
          style={{ borderColor: 'var(--border)' }}
        >
          <div
            className="flex-none px-3 pt-2 pb-0.5 text-[10px] uppercase tracking-wider"
            style={{ color: 'var(--text-muted)' }}
          >
            Arrest Rates&nbsp;
            <span style={{ color: 'var(--text-dim)' }}>
              — top 10 crime types
            </span>
          </div>
          <div className="flex-1 min-h-0 pb-1">
            {arrestRates.length > 0 ? (
              <ArrestRatesChart data={arrestRates} />
            ) : (
              <EmptyChart label="Run BatchAnalytics to populate" />
            )}
          </div>
        </div>
        <div className="flex flex-col">
          <div
            className="flex-none px-3 pt-2 pb-0.5 text-[10px] uppercase tracking-wider"
            style={{ color: 'var(--text-muted)' }}
          >
            Correlations&nbsp;
            <span style={{ color: 'var(--text-dim)' }}>
              — arrest vs violence rate
            </span>
          </div>
          <div className="flex-1 min-h-0 pb-1 px-1">
            {correlations.length > 0 ? (
              <CorrelationChart data={correlations} />
            ) : (
              <EmptyChart label="Run BatchAnalytics to populate" />
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
