'use client';

import {
    AreaChart, Area, XAxis, YAxis, CartesianGrid,
    Tooltip, ResponsiveContainer, Brush,
} from 'recharts';

function CustomTooltip({ active, payload, label }) {
    if (!active || !payload?.length) return null;
    return (
        <div className="rounded-xl border border-white/10 bg-nyc-dark/95 px-4 py-3 shadow-glass backdrop-blur-xl">
            <p className="text-xs font-medium text-white/60">{label}</p>
            <p className="mt-1 text-lg font-bold text-nyc-red">
                {payload[0].value?.toFixed(1)} <span className="text-xs font-normal text-white/40">trips/hr</span>
            </p>
        </div>
    );
}

export default function DemandAreaChart({ data }) {
    if (!data || data.length === 0) return null;

    // Format data for Recharts
    const chartData = data.map((d) => ({
        time: new Date(d.targetHour).toLocaleTimeString('en-US', {
            hour: 'numeric',
            minute: '2-digit',
            timeZone: 'America/New_York',
        }),
        trips: d.predictedTrips,
        rawTime: d.targetHour,
    }));

    return (
        <div className="chart-container">
            <div className="mb-4 flex items-center justify-between">
                <span className="rush-badge">24H Projection Horizon</span>
            </div>
            <ResponsiveContainer width="100%" height={400}>
                <AreaChart data={chartData} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                    <defs>
                        <linearGradient id="tripGradient" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="0%" stopColor="#ff4b4b" stopOpacity={0.4} />
                            <stop offset="100%" stopColor="#ff4b4b" stopOpacity={0.02} />
                        </linearGradient>
                    </defs>
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
                    <Area
                        type="monotone"
                        dataKey="trips"
                        stroke="#ff4b4b"
                        strokeWidth={3}
                        fill="url(#tripGradient)"
                        animationDuration={1500}
                    />
                    <Brush
                        dataKey="time"
                        height={40}
                        stroke="#ff4b4b"
                        fill="#161b22"
                        travellerWidth={8}
                    />
                </AreaChart>
            </ResponsiveContainer>
        </div>
    );
}
