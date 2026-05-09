'use client';

import { useEffect, useRef } from 'react';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';

export interface DistrictStat {
  district: string;
  total_events: number;
  avg_severity: number;
}

export interface Hotspot {
  cluster_id: number;
  crime_count: number;
  centroid_lat: number;
  centroid_lon: number;
  primary_types: string;
}

// Approximate CPD district centroids (Chicago Police Department numbered districts).
// Keys are zero-padded 3-digit strings to match the Violence Reduction dataset format.
const DISTRICT_COORDS: Record<string, [number, number]> = {
  '001': [41.8827, -87.6278],
  '002': [41.68, -87.6195],
  '003': [41.7726, -87.5822],
  '004': [41.7067, -87.6099],
  '005': [41.7395, -87.5565],
  '006': [41.7598, -87.6487],
  '007': [41.766, -87.706],
  '008': [41.8001, -87.7388],
  '009': [41.8218, -87.6981],
  '010': [41.8554, -87.7272],
  '011': [41.8669, -87.7177],
  '012': [41.8699, -87.668],
  '014': [41.9124, -87.673],
  '015': [41.8749, -87.7494],
  '016': [41.9519, -87.7629],
  '017': [41.994, -87.6952],
  '018': [41.9036, -87.634],
  '019': [41.9477, -87.6528],
  '020': [41.9755, -87.674],
  '022': [41.7358, -87.6816],
  '024': [41.996, -87.736],
  '025': [41.9055, -87.7567],
};

function severityColor(avg: number): [number, number, number] {
  if (avg >= 4.5) return [255, 78, 66]; // red
  if (avg >= 4.0) return [255, 123, 57]; // orange
  if (avg >= 3.0) return [255, 217, 61]; // yellow
  return [0, 212, 255]; // cyan
}

function normalizeDistrict(d: string): string {
  return d?.trim().replace(/^0+/, '').padStart(3, '0') ?? '';
}

function severityHex(avg: number): string {
  const [r, g, b] = severityColor(avg);
  return `#${r.toString(16).padStart(2, '0')}${g.toString(16).padStart(2, '0')}${b.toString(16).padStart(2, '0')}`;
}

interface Props {
  districts: DistrictStat[];
  hotspots?: Hotspot[];
}

function drawHeatmap(
  map: L.Map,
  canvas: HTMLCanvasElement,
  districts: DistrictStat[]
) {
  const ctx = canvas.getContext('2d');
  if (!ctx) return;
  ctx.clearRect(0, 0, canvas.width, canvas.height);

  if (districts.length === 0) {
    // Ghost blobs at all known district coords when no data
    ctx.globalCompositeOperation = 'source-over';
    Object.values(DISTRICT_COORDS).forEach((coords) => {
      const pt = map.latLngToContainerPoint(L.latLng(coords[0], coords[1]));
      const grad = ctx.createRadialGradient(pt.x, pt.y, 0, pt.x, pt.y, 40);
      grad.addColorStop(0, 'rgba(30,45,61,0.5)');
      grad.addColorStop(1, 'rgba(30,45,61,0)');
      ctx.beginPath();
      ctx.arc(pt.x, pt.y, 40, 0, Math.PI * 2);
      ctx.fillStyle = grad;
      ctx.fill();
    });
    return;
  }

  const maxEvents = Math.max(...districts.map((d) => d.total_events), 1);

  // First pass: draw additive blobs to create heat accumulation
  ctx.globalCompositeOperation = 'lighter';

  districts.forEach((d) => {
    const key = normalizeDistrict(d.district);
    const coords = DISTRICT_COORDS[key];
    if (!coords) return;

    const pt = map.latLngToContainerPoint(L.latLng(coords[0], coords[1]));
    const intensity = d.total_events / maxEvents; // 0..1
    const [r, g, b] = severityColor(d.avg_severity);

    // Outer diffuse glow
    const outerR = 80 + intensity * 70;
    const outerGrad = ctx.createRadialGradient(
      pt.x,
      pt.y,
      0,
      pt.x,
      pt.y,
      outerR
    );
    outerGrad.addColorStop(
      0,
      `rgba(${r},${g},${b},${0.18 + intensity * 0.22})`
    );
    outerGrad.addColorStop(0.5, `rgba(${r},${g},${b},${0.07 * intensity})`);
    outerGrad.addColorStop(1, 'rgba(0,0,0,0)');
    ctx.beginPath();
    ctx.arc(pt.x, pt.y, outerR, 0, Math.PI * 2);
    ctx.fillStyle = outerGrad;
    ctx.fill();

    // Inner bright core
    const coreR = 18 + intensity * 24;
    const coreGrad = ctx.createRadialGradient(pt.x, pt.y, 0, pt.x, pt.y, coreR);
    coreGrad.addColorStop(0, `rgba(${r},${g},${b},${0.7 + intensity * 0.3})`);
    coreGrad.addColorStop(0.6, `rgba(${r},${g},${b},${0.3 * intensity})`);
    coreGrad.addColorStop(1, 'rgba(0,0,0,0)');
    ctx.beginPath();
    ctx.arc(pt.x, pt.y, coreR, 0, Math.PI * 2);
    ctx.fillStyle = coreGrad;
    ctx.fill();
  });

  // Restore normal compositing for subsequent draws
  ctx.globalCompositeOperation = 'source-over';

  // Second pass: district label dots + district number text
  districts.forEach((d) => {
    const key = normalizeDistrict(d.district);
    const coords = DISTRICT_COORDS[key];
    if (!coords) return;

    const pt = map.latLngToContainerPoint(L.latLng(coords[0], coords[1]));
    const hex = severityHex(d.avg_severity);
    const intensity = d.total_events / maxEvents;

    // Center dot
    ctx.beginPath();
    ctx.arc(pt.x, pt.y, 3 + intensity * 3, 0, Math.PI * 2);
    ctx.fillStyle = hex;
    ctx.fill();

    // District label
    ctx.font = 'bold 9px monospace';
    ctx.textAlign = 'center';
    ctx.fillStyle = 'rgba(201,209,217,0.75)';
    ctx.fillText(`D${parseInt(d.district, 10)}`, pt.x, pt.y - 10);
  });
}

export default function DistrictMap({ districts, hotspots = [] }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<L.Map | null>(null);
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const clickLayerRef = useRef<L.LayerGroup | null>(null);
  const hotspotLayerRef = useRef<L.LayerGroup | null>(null);
  // Store latest districts in a ref so map event handlers always see fresh data
  const districtsRef = useRef<DistrictStat[]>(districts);
  districtsRef.current = districts;

  // Init map once
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    const container = containerRef.current;
    container.style.position = 'relative';

    const map = L.map(container, {
      center: [41.85, -87.65],
      zoom: 11,
      zoomControl: false,
    });

    L.tileLayer(
      'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
      {
        attribution:
          '&copy; <a href="https://www.openstreetmap.org/">OSM</a> &copy; <a href="https://carto.com/">CARTO</a>',
        subdomains: 'abcd',
        maxZoom: 19,
      }
    ).addTo(map);

    L.control.zoom({ position: 'bottomright' }).addTo(map);

    // Canvas heatmap overlay — pointer-events:none so clicks reach the map
    const canvas = document.createElement('canvas');
    canvas.style.cssText =
      'position:absolute;top:0;left:0;pointer-events:none;z-index:450;';
    container.appendChild(canvas);
    canvasRef.current = canvas;

    const syncCanvas = () => {
      canvas.width = container.clientWidth;
      canvas.height = container.clientHeight;
      drawHeatmap(map, canvas, districtsRef.current);
    };
    syncCanvas();

    const ro = new ResizeObserver(syncCanvas);
    ro.observe(container);

    map.on('moveend zoomend viewreset', () => {
      if (canvasRef.current)
        drawHeatmap(map, canvasRef.current, districtsRef.current);
    });

    clickLayerRef.current = L.layerGroup().addTo(map);
    hotspotLayerRef.current = L.layerGroup().addTo(map);
    mapRef.current = map;

    return () => {
      ro.disconnect();
      map.remove();
      mapRef.current = null;
      canvasRef.current = null;
      clickLayerRef.current = null;
      hotspotLayerRef.current = null;
    };
  }, []);

  // Redraw heatmap + refresh invisible popup markers whenever district data changes
  useEffect(() => {
    const map = mapRef.current;
    const canvas = canvasRef.current;
    if (!map || !canvas || !clickLayerRef.current) return;

    drawHeatmap(map, canvas, districts);
    clickLayerRef.current.clearLayers();

    districts.forEach((d) => {
      const key = normalizeDistrict(d.district);
      const coords = DISTRICT_COORDS[key];
      if (!coords) return;

      const hex = severityHex(d.avg_severity);
      const intensity =
        d.total_events / Math.max(...districts.map((x) => x.total_events), 1);
      const hitRadius = Math.round(20 + intensity * 20);

      // Invisible hit area — only purpose is to bind the popup
      L.circleMarker(coords, {
        radius: hitRadius,
        opacity: 0,
        fillOpacity: 0,
      })
        .bindPopup(
          `<div style="font-family:monospace;font-size:12px;line-height:1.7;min-width:140px">
            <div style="color:${hex};font-weight:bold;margin-bottom:4px;letter-spacing:.05em">
              DISTRICT ${parseInt(d.district, 10)}
            </div>
            <div style="color:#8b949e">Events&nbsp;&nbsp;&nbsp;&nbsp; <span style="color:#c9d1d9">${d.total_events.toLocaleString()}</span></div>
            <div style="color:#8b949e">Avg severity <span style="color:${hex}">${d.avg_severity.toFixed(2)}</span></div>
          </div>`,
          { className: 'wc-popup' }
        )
        .addTo(clickLayerRef.current!);
    });
  }, [districts]);

  // Hotspot markers — styled as labelled crosshair pins
  useEffect(() => {
    if (!hotspotLayerRef.current) return;
    hotspotLayerRef.current.clearLayers();
    if (hotspots.length === 0) return;

    const maxCount = Math.max(...hotspots.map((h) => h.crime_count), 1);

    hotspots.forEach((h) => {
      if (!h.centroid_lat || !h.centroid_lon) return;
      const intensity = h.crime_count / maxCount;
      const radius = 6 + intensity * 12;

      // Outer ring
      L.circleMarker([h.centroid_lat, h.centroid_lon], {
        radius: radius + 5,
        color: '#ff7b39',
        fillColor: 'transparent',
        fillOpacity: 0,
        weight: 1,
        opacity: 0.4,
        dashArray: '4 3',
      }).addTo(hotspotLayerRef.current!);

      // Core marker
      L.circleMarker([h.centroid_lat, h.centroid_lon], {
        radius,
        color: '#ff7b39',
        fillColor: '#ff7b39',
        fillOpacity: 0.25,
        weight: 1.5,
        opacity: 0.9,
      })
        .bindPopup(
          `<div style="font-family:monospace;font-size:12px;line-height:1.7;min-width:160px">
            <div style="color:#ff7b39;font-weight:bold;margin-bottom:4px;letter-spacing:.05em">
              HOTSPOT CLUSTER ${h.cluster_id}
            </div>
            <div style="color:#8b949e">Crimes&nbsp;&nbsp; <span style="color:#c9d1d9">${h.crime_count.toLocaleString()}</span></div>
            <div style="color:#8b949e">Types&nbsp;&nbsp;&nbsp; <span style="color:#c9d1d9">${h.primary_types.split(', ').slice(0, 3).join(', ')}</span></div>
          </div>`,
          { className: 'wc-popup' }
        )
        .addTo(hotspotLayerRef.current!);
    });
  }, [hotspots]);

  return (
    <div
      ref={containerRef}
      className="w-full h-full"
      style={{ minHeight: 0 }}
    />
  );
}
