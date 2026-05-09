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
  source_dataset?: string;
  victim_id?: string;
  incident_date: string;
  incident_time?: string;
  location?: string;
  district?: string;
  crime_type?: string;
  injury_type?: string;
  event_label?: string;
  severity?: number;
  processed_timestamp: number;
  processed_timestamp_api?: number;
}

interface DatasetStat {
  dataset: string;
  total_events: number;
  avg_severity: number;
  critical_count: number;
  last_seen: string;
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

interface SeverityStat {
  severity: number;
  count: number;
}

interface ViolenceRow {
  month: number;
  district: string;
  homicides: number;
  non_fatal_shootings: number;
  gunshot_incidents: number;
  total_incidents: number;
  gunshot_proportion_overall: number;
}

interface SexOffenderSummary {
  total_offenders: number;
  minor_victim_count: number;
  priority_count: number;
  standard_count: number;
  by_race: { race: string; count: number }[];
}

interface CommunityCrimeRow {
  community_area: string | null;
  total_crimes: number | null;
}

// ── Constants ──────────────────────────────────────────────────────────────

const API = 'http://localhost:8081';
const WS = 'ws://localhost:8081/ws';

// ── Debug logger ───────────────────────────────────────────────────────────

const L = {
  group: (label: string) =>
    console.group(`%c[WC] ${label}`, 'color:#00d4ff;font-weight:bold'),
  groupEnd: () => console.groupEnd(),
  info: (msg: string, ...a: unknown[]) =>
    console.log(`%c[WC] ${msg}`, 'color:#00d4ff', ...a),
  ok: (msg: string, ...a: unknown[]) =>
    console.log(`%c[WC] ✓ ${msg}`, 'color:#3fb950', ...a),
  warn: (msg: string, ...a: unknown[]) =>
    console.warn(`%c[WC] ⚠ ${msg}`, 'color:#ffd93d', ...a),
  err: (msg: string, ...a: unknown[]) =>
    console.error(`%c[WC] ✗ ${msg}`, 'color:#ff4e42', ...a),
};

async function apiFetch(
  url: string
): Promise<{ ok: boolean; status: number; body: unknown }> {
  L.info(`fetch → ${url}`);
  try {
    const r = await fetch(url);
    let body: unknown;
    const text = await r.text();
    try {
      body = JSON.parse(text);
    } catch {
      body = text;
    }
    if (r.ok) {
      L.ok(`${r.status} ← ${url}`, body);
    } else {
      L.err(`${r.status} ← ${url}`, body);
    }
    return { ok: r.ok, status: r.status, body };
  } catch (e) {
    L.err(`NETWORK ERROR ← ${url}`, e);
    return { ok: false, status: 0, body: null };
  }
}

const DATASET_COLORS: Record<
  string,
  { color: string; dim: string; label: string }
> = {
  crimes: { color: '#ff4e42', dim: 'rgba(255,78,66,0.15)', label: 'CRIMES' },
  violence: {
    color: '#ff7b39',
    dim: 'rgba(255,123,57,0.12)',
    label: 'VIOLENCE',
  },
  arrests: { color: '#4d96ff', dim: 'rgba(77,150,255,0.12)', label: 'ARRESTS' },
  'sex-offenders': {
    color: '#7b5ea7',
    dim: 'rgba(123,94,167,0.15)',
    label: 'SEX OFFENDERS',
  },
};
const datasetStyle = (ds?: string) =>
  DATASET_COLORS[ds ?? ''] ?? {
    color: '#8b949e',
    dim: 'rgba(139,148,158,0.1)',
    label: ds?.toUpperCase() ?? 'UNKNOWN',
  };

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

function DatasetBadge({ dataset }: { dataset?: string }) {
  const ds = datasetStyle(dataset);
  return (
    <span
      className="text-[9px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
      style={{
        color: ds.color,
        background: ds.dim,
        border: `1px solid ${ds.color}40`,
      }}
    >
      {ds.label}
    </span>
  );
}

function AlertCard({ event }: { event: CrimeEvent }) {
  const s = sev(event.severity);
  const ts = new Date(
    event.processed_timestamp_api ?? event.processed_timestamp
  ).toLocaleTimeString('en-US', { hour12: false });
  const label = event.event_label ?? event.crime_type ?? event.injury_type;

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
        <div className="flex items-center gap-1.5">
          <DatasetBadge dataset={event.source_dataset} />
          <code className="text-[10px] opacity-40">
            {event.event_id.slice(0, 10)}…
          </code>
        </div>
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
        {label && (
          <div className="col-span-2">
            <span className="opacity-50">TYPE </span>
            <span style={{ color: s.color }}>{label}</span>
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

function SeverityChart({
  events,
  dbStats,
}: {
  events: CrimeEvent[];
  dbStats: SeverityStat[];
}) {
  const ref = useRef<HTMLDivElement>(null);

  // Prefer cumulative DB counts; fall back to live buffer when DB unavailable
  const counts =
    dbStats.length > 0
      ? [1, 2, 3, 4, 5].map(
          (s) => dbStats.find((d) => d.severity === s)?.count ?? 0
        )
      : [1, 2, 3, 4, 5].map(
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
            text:
              total > 0
                ? total >= 1000
                  ? `${(total / 1000).toFixed(1)}k`
                  : String(total)
                : '—',
            fill: '#c9d1d9',
            fontSize: total >= 10000 ? 13 : 18,
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

    [...counts]
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

function DatasetBreakdownChart({ data }: { data: DatasetStat[] }) {
  const ref = useRef<HTMLDivElement>(null);
  const sorted = [...data].sort((a, b) => b.total_events - a.total_events);

  useEChart(
    ref,
    () => ({
      ...CHART_BASE,
      grid: { left: 80, right: 50, top: 4, bottom: 20 },
      xAxis: {
        type: 'value',
        axisLabel: { color: '#8b949e', fontSize: 9 },
        splitLine: { lineStyle: { color: '#1e2d3d', type: 'dashed' } },
        minInterval: 1,
      },
      yAxis: {
        type: 'category',
        data: sorted.map((d) => d.dataset),
        axisLine: { lineStyle: { color: '#1e2d3d' } },
        axisTick: { show: false },
        axisLabel: { color: '#8b949e', fontSize: 9 },
      },
      series: [
        {
          type: 'bar',
          data: sorted.map((d) => ({
            value: d.total_events,
            itemStyle: {
              color: DATASET_COLORS[d.dataset]?.color ?? '#8b949e',
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
            formatter: (p: { value: number }) =>
              p.value > 0 ? p.value.toLocaleString() : '',
          },
        },
      ],
      tooltip: {
        trigger: 'axis',
        backgroundColor: '#0d1117',
        borderColor: '#1e2d3d',
        textStyle: { color: '#c9d1d9', fontFamily: 'monospace', fontSize: 11 },
        formatter: (params: { name: string; value: number }[]) => {
          const d = data.find((x) => x.dataset === params[0]?.name);
          if (!d) return '';
          return `${d.dataset}<br/>Events: ${d.total_events.toLocaleString()}<br/>Avg severity: ${d.avg_severity.toFixed(2)}<br/>Critical: ${d.critical_count}`;
        },
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

function ViolenceMonthlyChart({ data }: { data: ViolenceRow[] }) {
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
    const rows = data.filter((r) => r.month === i + 1);
    return {
      homicides: rows.reduce((s, r) => s + r.homicides, 0),
      non_fatal: rows.reduce((s, r) => s + r.non_fatal_shootings, 0),
    };
  });
  const gsProportion = data.length > 0 ? data[0].gunshot_proportion_overall : 0;

  useEChart(
    ref,
    () => ({
      ...CHART_BASE,
      grid: { left: 36, right: 8, top: 18, bottom: 28 },
      legend: {
        data: ['Homicides', 'Non-Fatal Shootings'],
        textStyle: { color: '#8b949e', fontSize: 8 },
        top: 0,
        right: 0,
      },
      xAxis: {
        type: 'category',
        data: MONTHS,
        axisLine: { lineStyle: { color: '#1e2d3d' } },
        axisTick: { show: false },
        axisLabel: { color: '#8b949e', fontSize: 8 },
      },
      yAxis: {
        type: 'value',
        axisLabel: { color: '#8b949e', fontSize: 9 },
        splitLine: { lineStyle: { color: '#1e2d3d', type: 'dashed' } },
        minInterval: 1,
      },
      series: [
        {
          name: 'Homicides',
          type: 'bar',
          stack: 'violence',
          data: byMonth.map((m) => m.homicides),
          itemStyle: { color: '#ff4e42' },
          barMaxWidth: 18,
        },
        {
          name: 'Non-Fatal Shootings',
          type: 'bar',
          stack: 'violence',
          data: byMonth.map((m) => m.non_fatal),
          itemStyle: { color: '#ff7b39' },
          barMaxWidth: 18,
        },
      ],
      graphic:
        gsProportion > 0
          ? [
              {
                type: 'text',
                right: 8,
                bottom: 32,
                style: {
                  text: `Gunshot: ${(gsProportion * 100).toFixed(1)}%`,
                  fill: '#ffd93d',
                  fontSize: 9,
                  fontFamily: 'monospace',
                },
              },
            ]
          : [],
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

function SexOffenderChart({ data }: { data: SexOffenderSummary }) {
  const ref = useRef<HTMLDivElement>(null);
  const top = [...data.by_race].slice(0, 8).reverse();

  useEChart(
    ref,
    () => ({
      ...CHART_BASE,
      grid: { left: 70, right: 70, top: 4, bottom: 20 },
      xAxis: {
        type: 'value',
        axisLabel: { color: '#8b949e', fontSize: 9 },
        splitLine: { lineStyle: { color: '#1e2d3d', type: 'dashed' } },
        minInterval: 1,
      },
      yAxis: {
        type: 'category',
        data: top.map((r) => r.race),
        axisLine: { lineStyle: { color: '#1e2d3d' } },
        axisTick: { show: false },
        axisLabel: { color: '#8b949e', fontSize: 8 },
      },
      series: [
        {
          type: 'bar',
          data: top.map((r) => ({
            value: r.count,
            itemStyle: { color: '#7b5ea7', borderRadius: [0, 2, 2, 0] },
          })),
          barMaxWidth: 12,
          label: {
            show: true,
            position: 'right',
            color: '#8b949e',
            fontSize: 8,
            fontFamily: 'monospace',
          },
        },
      ],
      graphic: [
        {
          type: 'text',
          right: 8,
          top: 4,
          style: {
            text: `PRIORITY: ${data.priority_count.toLocaleString()}`,
            fill: '#ff4e42',
            fontSize: 9,
            fontFamily: 'monospace',
          },
        },
        {
          type: 'text',
          right: 8,
          top: 18,
          style: {
            text: `TOTAL: ${data.total_offenders.toLocaleString()}`,
            fill: '#8b949e',
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
    [data]
  );
  return <div ref={ref} className="w-full h-full" />;
}

function CommunityCrimeChart({ data }: { data: CommunityCrimeRow[] }) {
  const ref = useRef<HTMLDivElement>(null);
  const top = [...data]
    .sort((a, b) => (b.total_crimes ?? 0) - (a.total_crimes ?? 0))
    .slice(0, 10)
    .reverse();

  useEChart(
    ref,
    () => ({
      ...CHART_BASE,
      grid: { left: 28, right: 50, top: 4, bottom: 20 },
      xAxis: {
        type: 'value',
        axisLabel: { color: '#8b949e', fontSize: 9 },
        splitLine: { lineStyle: { color: '#1e2d3d', type: 'dashed' } },
        minInterval: 1,
      },
      yAxis: {
        type: 'category',
        data: top.map((r) => `CA ${r.community_area}`),
        axisLine: { lineStyle: { color: '#1e2d3d' } },
        axisTick: { show: false },
        axisLabel: { color: '#8b949e', fontSize: 8 },
      },
      series: [
        {
          type: 'bar',
          data: top.map((r) => ({
            value: r.total_crimes ?? 0,
            itemStyle: { color: '#4d96ff', borderRadius: [0, 2, 2, 0] },
          })),
          barMaxWidth: 12,
          label: {
            show: true,
            position: 'right',
            color: '#8b949e',
            fontSize: 8,
            fontFamily: 'monospace',
            formatter: (p: { value: number }) => p.value.toLocaleString(),
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

// ── Time slider (historical replay) ───────────────────────────────────────

function TimeSlider({
  onReplay,
  onLive,
  isReplay,
  totalEvents,
}: {
  onReplay: (offset: number, limit: number) => void;
  onLive: () => void;
  isReplay: boolean;
  totalEvents: number;
}) {
  // Default to rightmost position (live). 0 = oldest end, 100 = live end.
  const [sliderValue, setSliderValue] = useState(100);

  const calcOffset = (v: number) => {
    const maxOffset = Math.max(totalEvents - 50, 0);
    return Math.round(((100 - v) / 100) * maxOffset);
  };

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const v = Number(e.target.value);
    setSliderValue(v);
    if (v === 100) {
      onLive();
    } else {
      onReplay(calcOffset(v), 50);
    }
  };

  const positionLabel = (() => {
    if (sliderValue === 100) return 'LIVE';
    if (totalEvents > 0) {
      const offset = calcOffset(sliderValue);
      return `${offset.toLocaleString()} events back`;
    }
    return `${100 - sliderValue}% back`;
  })();

  return (
    <div
      className="flex-none px-5 py-2 flex items-center gap-3 border-t"
      style={{ borderColor: 'var(--border)', background: 'var(--bg-card)' }}
    >
      <span
        className="text-[10px] uppercase tracking-widest flex-none"
        style={{ color: 'var(--text-muted)' }}
      >
        History
      </span>

      <span
        className="text-[9px] flex-none"
        style={{ color: 'var(--text-dim)' }}
      >
        oldest
      </span>

      <input
        type="range"
        min={0}
        max={100}
        value={sliderValue}
        onChange={handleChange}
        className="flex-1"
        style={{ accentColor: 'var(--cyan)', cursor: 'pointer' }}
      />

      <span className="text-[9px] flex-none" style={{ color: 'var(--cyan)' }}>
        live
      </span>

      <span
        className="text-[10px] flex-none tabular-nums w-36 text-right"
        style={{ color: isReplay ? 'var(--yellow)' : 'var(--text-dim)' }}
      >
        {positionLabel}
      </span>

      {isReplay && (
        <button
          onClick={() => {
            setSliderValue(100);
            onLive();
          }}
          className="text-[10px] px-2 py-0.5 rounded border uppercase tracking-wider flex-none"
          style={{
            color: 'var(--cyan)',
            borderColor: 'var(--cyan)',
            background: 'var(--cyan-dim)',
            cursor: 'pointer',
          }}
        >
          ↩ Live
        </button>
      )}
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
  const [datasetStats, setDatasetStats] = useState<DatasetStat[]>([]);
  const [severityStats, setSeverityStats] = useState<SeverityStat[]>([]);
  const [violence, setViolence] = useState<ViolenceRow[]>([]);
  const [sexOffenders, setSexOffenders] = useState<SexOffenderSummary | null>(
    null
  );
  const [communityCorrelations, setCommunityCorrelations] = useState<
    CommunityCrimeRow[]
  >([]);
  const [clock, setClock] = useState('');

  const tsBuffer = useRef<number[]>([]);

  // Historical replay handlers
  const handleReplay = useCallback(async (offset: number, limit: number) => {
    const { ok, body } = await apiFetch(
      `${API}/events/history?limit=${limit}&offset=${offset}`
    );
    if (ok) {
      const data = body as { events?: CrimeEvent[] };
      const evs = data.events ?? [];
      L.info(`replay loaded ${evs.length} events (offset=${offset})`);
      setReplayEvents(evs);
      setIsReplay(true);
    }
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
  const wsAttemptRef = useRef(0);
  const connectWS = useCallback(() => {
    const attempt = ++wsAttemptRef.current;
    L.group(`WebSocket attempt #${attempt}`);
    L.info(`connecting to ${WS}`);
    L.info(`readyState before new WebSocket: N/A (creating)`);
    L.groupEnd();

    setWsStatus('connecting');
    let ws: WebSocket;
    try {
      ws = new WebSocket(WS);
    } catch (e) {
      L.err(`WebSocket constructor threw`, e);
      setTimeout(connectWS, 3000);
      return;
    }

    ws.onopen = () => {
      L.ok(`WebSocket OPEN (attempt #${attempt}) readyState=${ws.readyState}`);
      setWsStatus('connected');
    };

    ws.onmessage = (e) => {
      let msg: { type: string; data: unknown };
      try {
        msg = JSON.parse(e.data);
      } catch (err) {
        L.err(`WebSocket message parse failed`, e.data, err);
        return;
      }
      L.info(`WebSocket message type="${msg.type}"`, msg.data);
      if (msg.type === 'event') {
        const ev = msg.data as CrimeEvent;
        tsBuffer.current.push(Date.now());
        setEvents((prev) => [ev, ...prev.slice(0, 149)]);
      }
    };

    ws.onclose = (e) => {
      L.warn(
        `WebSocket CLOSED (attempt #${attempt}) code=${e.code} reason="${e.reason}" wasClean=${e.wasClean}`
      );
      setWsStatus('disconnected');
      setTimeout(connectWS, 3000);
    };

    ws.onerror = (e) => {
      L.err(
        `WebSocket ERROR (attempt #${attempt}) readyState=${ws.readyState}`,
        e
      );
      ws.close();
    };
  }, []);

  useEffect(() => {
    connectWS();
  }, [connectWS]);

  // Log mount
  useEffect(() => {
    L.group('Dashboard mounted');
    L.info(`API base: ${API}`);
    L.info(`WS url:   ${WS}`);
    L.groupEnd();
  }, []);

  // Stats poll (2 s)
  useEffect(() => {
    const poll = async () => {
      const { ok, body } = await apiFetch(`${API}/stats`);
      if (ok) {
        const s = body as Stats;
        L.info(
          `stats updated — total_events=${s.total_events} connected_clients=${s.connected_clients}`
        );
        setStats(s);
      }
    };
    poll();
    const id = setInterval(poll, 2000);
    return () => clearInterval(id);
  }, []);

  // Districts poll (15 s)
  useEffect(() => {
    const poll = async () => {
      const { ok, body } = await apiFetch(`${API}/districts/summary`);
      if (ok) {
        const data = body as { districts?: DistrictStat[] };
        const districts = data.districts ?? [];
        L.info(`districts updated — ${districts.length} districts`);
        setDistricts(districts);
      }
    };
    poll();
    const id = setInterval(poll, 15_000);
    return () => clearInterval(id);
  }, []);

  // Dataset summary poll (15 s)
  useEffect(() => {
    const poll = async () => {
      const { ok, body } = await apiFetch(`${API}/datasets/summary`);
      if (ok) {
        const data = body as { datasets?: DatasetStat[] };
        const datasets = data.datasets ?? [];
        L.info(
          `dataset stats updated — ${datasets.length} datasets`,
          datasets.map((d) => `${d.dataset}:${d.total_events}`)
        );
        setDatasetStats(datasets);
      }
    };
    poll();
    const id = setInterval(poll, 15_000);
    return () => clearInterval(id);
  }, []);

  // Analytics poll (5 min — batch data changes infrequently)
  useEffect(() => {
    const poll = async () => {
      L.group('Analytics batch poll');
      const [h, a, ct, co, v, so, cc] = await Promise.all([
        apiFetch(`${API}/analytics/hotspots`),
        apiFetch(`${API}/analytics/arrest-rates`),
        apiFetch(`${API}/analytics/crime-trends`),
        apiFetch(`${API}/analytics/correlations?type=district`),
        apiFetch(`${API}/analytics/violence`),
        apiFetch(`${API}/analytics/sex-offenders`),
        apiFetch(`${API}/analytics/correlations?type=community`),
      ]);
      L.info(`hotspots ok=${h.ok}`, h.body);
      L.info(`arrest-rates ok=${a.ok}`, a.body);
      L.info(`crime-trends ok=${ct.ok}`, ct.body);
      L.info(`correlations ok=${co.ok}`, co.body);
      L.info(`violence ok=${v.ok}`, v.body);
      L.info(`sex-offenders ok=${so.ok}`, so.body);
      L.info(`community-correlations ok=${cc.ok}`, cc.body);
      L.groupEnd();
      if (h.ok) setHotspots((h.body as { data?: Hotspot[] }).data ?? []);
      if (a.ok) setArrestRates((a.body as { data?: ArrestRate[] }).data ?? []);
      if (ct.ok)
        setCrimeTrendsMonthly(
          (ct.body as { data?: CrimeTrendMonthly[] }).data ?? []
        );
      if (co.ok)
        setCorrelations((co.body as { data?: Correlation[] }).data ?? []);
      if (v.ok) setViolence((v.body as { data?: ViolenceRow[] }).data ?? []);
      if (so.ok) {
        const d = (so.body as { data?: SexOffenderSummary }).data;
        if (d && d.total_offenders !== undefined) setSexOffenders(d);
      }
      if (cc.ok) {
        const rows =
          (
            cc.body as {
              data?: {
                community_area: string | null;
                metric_a: number | null;
              }[];
            }
          ).data ?? [];
        setCommunityCorrelations(
          rows
            .filter((r) => r.community_area && r.metric_a != null)
            .map((r) => ({
              community_area: r.community_area,
              total_crimes: r.metric_a,
            }))
        );
      }
    };
    poll();
    const id = setInterval(poll, 300_000);
    return () => clearInterval(id);
  }, []);

  // Trends poll (30 s)
  useEffect(() => {
    const poll = async () => {
      const { ok, body } = await apiFetch(`${API}/analytics/trends?hours=24`);
      if (ok) {
        const data = body as { trends?: Trend[] };
        const t = data.trends ?? [];
        L.info(`trends updated — ${t.length} hourly buckets`);
        setTrends(t);
      }
    };
    poll();
    const id = setInterval(poll, 30_000);
    return () => clearInterval(id);
  }, []);

  // Severity distribution poll (30 s) — cumulative from DB
  useEffect(() => {
    const poll = async () => {
      const { ok, body } = await apiFetch(`${API}/analytics/severity`);
      if (ok) {
        const data = body as { data?: SeverityStat[] };
        setSeverityStats(data.data ?? []);
      }
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
        totalEvents={stats.total_events}
      />

      {/* Charts — row 1: live streaming data */}
      <div
        className="flex-none grid grid-cols-4 border-t"
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
            <span style={{ color: 'var(--text-dim)' }}>— all time</span>
          </div>
          <div className="flex-1 min-h-0 pb-1">
            {severityStats.length > 0 || events.length > 0 ? (
              <SeverityChart events={events} dbStats={severityStats} />
            ) : (
              <EmptyChart label="No events yet" />
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

        <div className="flex flex-col">
          <div
            className="flex-none px-3 pt-2 pb-0.5 text-[10px] uppercase tracking-wider"
            style={{ color: 'var(--text-muted)' }}
          >
            By Dataset&nbsp;
            <span style={{ color: 'var(--text-dim)' }}>— all time</span>
          </div>
          <div className="flex-1 min-h-0 pb-1">
            {datasetStats.length > 0 ? (
              <DatasetBreakdownChart data={datasetStats} />
            ) : (
              <EmptyChart label="No dataset data yet" />
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
              — arrest vs violence rate by district
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

      {/* Charts — row 3: violence, sex offender proximity, community crime (7.3 / 7.4 / 7.6) */}
      <div
        className="flex-none grid grid-cols-3 border-t"
        style={{ height: 160, borderColor: 'var(--border)' }}
      >
        <div
          className="flex flex-col border-r"
          style={{ borderColor: 'var(--border)' }}
        >
          <div
            className="flex-none px-3 pt-2 pb-0.5 text-[10px] uppercase tracking-wider"
            style={{ color: 'var(--text-muted)' }}
          >
            Violence Analysis&nbsp;
            <span style={{ color: 'var(--text-dim)' }}>
              — homicides &amp; shootings by month
            </span>
          </div>
          <div className="flex-1 min-h-0 pb-1 px-1">
            {violence.length > 0 ? (
              <ViolenceMonthlyChart data={violence} />
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
            Sex Offender Proximity&nbsp;
            <span style={{ color: 'var(--text-dim)' }}>
              — by race · priority flagged
            </span>
          </div>
          <div className="flex-1 min-h-0 pb-1 px-1">
            {sexOffenders && sexOffenders.total_offenders > 0 ? (
              <SexOffenderChart data={sexOffenders} />
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
            Crime by Community Area&nbsp;
            <span style={{ color: 'var(--text-dim)' }}>— top 10</span>
          </div>
          <div className="flex-1 min-h-0 pb-1 px-1">
            {communityCorrelations.length > 0 ? (
              <CommunityCrimeChart data={communityCorrelations} />
            ) : (
              <EmptyChart label="Run BatchAnalytics to populate" />
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
