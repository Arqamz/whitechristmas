'use client';

import { useEffect, useRef } from 'react';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';

export interface DistrictStat {
  district: string;
  total_events: number;
  avg_severity: number;
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

function severityColor(avg: number): string {
  if (avg >= 4.5) return '#ff4e42';
  if (avg >= 4.0) return '#ff7b39';
  if (avg >= 3.0) return '#ffd93d';
  return '#00d4ff';
}

function normalizeDistrict(d: string): string {
  // Accept "11", "011", "011 " etc.
  return d?.trim().replace(/^0+/, '').padStart(3, '0') ?? '';
}

interface Props {
  districts: DistrictStat[];
}

export default function DistrictMap({ districts }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<L.Map | null>(null);
  const layerRef = useRef<L.LayerGroup | null>(null);

  // Init map once
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    const map = L.map(containerRef.current, {
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

    layerRef.current = L.layerGroup().addTo(map);
    mapRef.current = map;

    return () => {
      map.remove();
      mapRef.current = null;
      layerRef.current = null;
    };
  }, []);

  // Update markers whenever district data changes
  useEffect(() => {
    if (!layerRef.current) return;

    layerRef.current.clearLayers();

    if (districts.length === 0) {
      // Show placeholder markers while no live data
      Object.entries(DISTRICT_COORDS).forEach(([key, coords]) => {
        L.circleMarker(coords, {
          radius: 6,
          color: '#1e2d3d',
          fillColor: '#1e2d3d',
          fillOpacity: 0.5,
          weight: 1,
        }).addTo(layerRef.current!);
      });
      return;
    }

    const maxEvents = Math.max(...districts.map((d) => d.total_events), 1);

    districts.forEach((d) => {
      const key = normalizeDistrict(d.district);
      const coords = DISTRICT_COORDS[key];
      if (!coords) return;

      const color = severityColor(d.avg_severity);
      const radius = 8 + (d.total_events / maxEvents) * 22;

      const marker = L.circleMarker(coords, {
        radius,
        color,
        fillColor: color,
        fillOpacity: 0.25,
        weight: 2,
        opacity: 0.9,
      });

      marker.bindPopup(`
        <div style="font-family:monospace;font-size:12px;line-height:1.6">
          <div style="color:#00d4ff;font-weight:bold;margin-bottom:4px">
            DISTRICT ${d.district}
          </div>
          <div style="color:#8b949e">Total events&nbsp;&nbsp; <span style="color:#c9d1d9">${d.total_events}</span></div>
          <div style="color:#8b949e">Avg severity&nbsp; <span style="color:${color}">${d.avg_severity.toFixed(1)}</span></div>
        </div>
      `);

      // Pulse ring for high-severity districts
      if (d.avg_severity >= 4) {
        L.circleMarker(coords, {
          radius: radius + 6,
          color,
          fillColor: 'transparent',
          fillOpacity: 0,
          weight: 1,
          opacity: 0.3,
          className: 'glow-pulse',
        }).addTo(layerRef.current!);
      }

      marker.addTo(layerRef.current!);
    });
  }, [districts]);

  return (
    <div
      ref={containerRef}
      className="w-full h-full"
      style={{ minHeight: 0 }}
    />
  );
}
