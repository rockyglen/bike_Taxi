'use client';

import {
    LineChart, Line, XAxis, YAxis, CartesianGrid,
    Tooltip, Legend, ResponsiveContainer, Brush,
} from 'recharts';

const STATION_COLORS = ['#ff4b4b', '#38bdf8', '#4ade80'];

function CustomTooltip({ active, payload, label }) {
    if (!active || !payload?.length) return null;
    return (
        <div className="rounded-xl border border-white/10 bg-nyc-dark/95 px-4 py-3 shadow-glass backdrop-blur-xl">
            <p className="text-xs font-medium text-white/60 mb-2">{label}</p>
            {payload.map((entry) => (
                <p key={entry.dataKey} className="text-sm font-bold" style={{ color: entry.color }}>
                    {entry.name}: {entry.value?.toFixed(1)}{' '}
                    <span className="text-xs font-normal text-white/40">trips/hr</span>
                </p>
            ))}
        </div>
    );
}

export default function DemandAreaChart({ data, stationIds }) {
    if (!data || data.length === 0) return null;

    const stations = stationIds && stationIds.length > 0 ? stationIds : [];

    const chartData = data.map((d) => {
        const point = {
            time: new Date(d.targetHour).toLocaleTimeString('en-US', {
                hour: 'numeric',
                minute: '2-digit',
                timeZone: 'America/New_York',
            }),
            rawTime: d.targetHour,
        };
        for (const sid of stations) {
            point[sid] = d[sid] ?? 0;
        }
        return point;
    });

    return (
        <div className="chart-container">
            <div className="mb-4 flex items-center justify-between">
                <span className="rush-badge">24H Projection Horizon</span>
            </div>
            <ResponsiveContainer width="100%" height={400}>
                <LineChart data={chartData} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
                    <XAxis
                        dataKey="time"
                        stroke="rgba(255,255,255,0.2)"
                        tick={{ fontSize: 11 }}
                        interval="preserveStartEnd"
                    />
                    <YAxis
                        stroke="rgba(255,255,255,0.2)"
                        tick={{ fontSize: 11 }}
                        label={{
                            value: 'Trip Demand',
                            angle: -90,
                            position: 'insideLeft',
                            style: { fill: 'rgba(255,255,255,0.4)', fontSize: 12 },
                        }}
                    />
                    <Tooltip content={<CustomTooltip />} />
                    <Legend
                        formatter={(value, entry, index) => (
                            <span style={{ color: 'rgba(255,255,255,0.6)', fontSize: 11 }}>
                                Station {index + 1}
                            </span>
                        )}
                    />
                    {stations.map((sid, i) => (
                        <Line
                            key={sid}
                            type="monotone"
                            dataKey={sid}
                            name={`Station ${i + 1}`}
                            stroke={STATION_COLORS[i] || '#fff'}
                            strokeWidth={2}
                            dot={false}
                            animationDuration={1500}
                        />
                    ))}
                    <Brush
                        dataKey="time"
                        height={40}
                        stroke="#ff4b4b"
                        fill="#161b22"
                        travellerWidth={8}
                    />
                </LineChart>
            </ResponsiveContainer>
        </div>
    );
}
