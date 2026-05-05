'use client';

import { useEffect, useState, useRef } from 'react';

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
}

interface Stats {
  connected_clients: number;
  total_events: number;
  timestamp: number;
}

export default function Home() {
  const [events, setEvents] = useState<CrimeEvent[]>([]);
  const [stats, setStats] = useState<Stats>({
    connected_clients: 0,
    total_events: 0,
    timestamp: 0,
  });
  const [status, setStatus] = useState<
    'connecting' | 'connected' | 'disconnected'
  >('disconnected');
  const wsRef = useRef<WebSocket | null>(null);
  const statsIntervalRef = useRef<NodeJS.Timeout | null>(null);

  useEffect(() => {
    // Connect to WebSocket
    const connectWebSocket = () => {
      try {
        const ws = new WebSocket('ws://localhost:8080/ws');

        ws.onopen = () => {
          setStatus('connected');
          console.log('✓ Connected to WebSocket');
        };

        ws.onmessage = (event) => {
          const message = JSON.parse(event.data);

          if (message.type === 'event') {
            setEvents((prev) => [
              message.data as CrimeEvent,
              ...prev.slice(0, 99),
            ]);
          }
        };

        ws.onerror = (error) => {
          console.error('WebSocket error:', error);
          setStatus('disconnected');
        };

        ws.onclose = () => {
          setStatus('disconnected');
          // Attempt to reconnect after 3 seconds
          setTimeout(connectWebSocket, 3000);
        };

        wsRef.current = ws;
      } catch (error) {
        console.error('Failed to connect:', error);
        setStatus('disconnected');
      }
    };

    connectWebSocket();

    // Poll stats every 2 seconds
    statsIntervalRef.current = setInterval(async () => {
      try {
        const response = await fetch('http://localhost:8080/stats');
        const data = await response.json();
        setStats(data);
      } catch (error) {
        console.error('Failed to fetch stats:', error);
      }
    }, 2000);

    return () => {
      if (wsRef.current) {
        wsRef.current.close();
      }
      if (statsIntervalRef.current) {
        clearInterval(statsIntervalRef.current);
      }
    };
  }, []);

  const getSeverityColor = (severity?: number) => {
    switch (severity) {
      case 5:
        return 'severity-5';
      case 4:
        return 'severity-4';
      case 3:
        return 'severity-3';
      case 2:
        return 'severity-2';
      default:
        return 'severity-1';
    }
  };

  const getSeverityLabel = (severity?: number) => {
    switch (severity) {
      case 5:
        return 'CRITICAL';
      case 4:
        return 'HIGH';
      case 3:
        return 'MEDIUM';
      case 2:
        return 'LOW';
      default:
        return 'INFO';
    }
  };

  const statusColor =
    status === 'connected'
      ? 'bg-green-500'
      : status === 'connecting'
        ? 'bg-yellow-500'
        : 'bg-red-500';

  return (
    <main className="min-h-screen bg-gradient-to-br from-gray-900 via-slate-900 to-gray-900 p-4">
      <div className="max-w-6xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-4xl font-bold text-white mb-2 flex items-center gap-3">
            <span className="text-2xl">🚨</span> WhiteChristmas
          </h1>
          <p className="text-gray-400">
            Real-time Crime Analytics & Intelligent Alert System
          </p>
        </div>

        {/* Status Bar */}
        <div className="bg-slate-800 border border-slate-700 rounded-lg p-4 mb-8">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              <div className="flex items-center gap-2">
                <div
                  className={`w-3 h-3 rounded-full ${statusColor} animate-pulse`}
                />
                <span className="text-sm font-medium capitalize">
                  {status === 'connected' ? 'Connected' : 'Disconnected'}
                </span>
              </div>
              <div className="text-sm text-gray-400">|</div>
              <div className="text-sm">
                👥 <strong>{stats.connected_clients}</strong> clients
              </div>
              <div className="text-sm">
                📊 <strong>{stats.total_events}</strong> events
              </div>
            </div>
            <div className="text-xs text-gray-500">
              {new Date(stats.timestamp).toLocaleTimeString()}
            </div>
          </div>
        </div>

        {/* Events Feed */}
        <div className="space-y-3">
          <h2 className="text-xl font-bold text-white mb-4">
            📖 Live Event Feed
          </h2>

          {events.length === 0 ? (
            <div className="bg-slate-800 border border-slate-700 rounded-lg p-8 text-center">
              <p className="text-gray-400">
                {status === 'connected'
                  ? 'Waiting for events...'
                  : 'Connect to see events'}
              </p>
            </div>
          ) : (
            <div className="space-y-2">
              {events.map((event, idx) => (
                <div
                  key={`${event.event_id}-${idx}`}
                  className={`event-card ${getSeverityColor(event.severity)}`}
                >
                  <div className="flex items-start justify-between gap-4">
                    <div className="flex-1">
                      <div className="flex items-center gap-3 mb-2">
                        <code className="text-xs text-cyan-400 font-mono bg-black/30 px-2 py-1 rounded">
                          {event.event_id.slice(0, 8)}...
                        </code>
                        <span
                          className={`badge ${getSeverityColor(event.severity)}`}
                        >
                          {getSeverityLabel(event.severity)}
                        </span>
                      </div>

                      <div className="grid grid-cols-2 gap-4 text-sm">
                        <div>
                          <span className="text-gray-400">Date:</span>{' '}
                          <span className="text-white font-medium">
                            {event.incident_date} {event.incident_time || ''}
                          </span>
                        </div>
                        <div>
                          <span className="text-gray-400">District:</span>{' '}
                          <span className="text-white font-medium">
                            {event.district || 'Unknown'}
                          </span>
                        </div>
                        <div>
                          <span className="text-gray-400">Location:</span>{' '}
                          <span className="text-white font-medium">
                            {event.location || 'Unknown'}
                          </span>
                        </div>
                        <div>
                          <span className="text-gray-400">Type:</span>{' '}
                          <span className="text-white font-medium">
                            {event.injury_type || 'Unknown'}
                          </span>
                        </div>
                      </div>

                      {event.victim_id && (
                        <div className="text-xs text-gray-400 mt-2">
                          Victim: {event.victim_id}
                        </div>
                      )}
                    </div>

                    <div className="text-xs text-gray-500 text-right">
                      {new Date(event.processed_timestamp).toLocaleTimeString()}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="mt-12 text-center text-xs text-gray-600 border-t border-slate-800 pt-8">
          <p>WhiteChristmas v0.1.0</p>
        </div>
      </div>
    </main>
  );
}
