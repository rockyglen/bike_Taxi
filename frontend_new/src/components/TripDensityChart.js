'use client';

import {
    AreaChart, Area, XAxis, YAxis, CartesianGrid,
    Tooltip, ResponsiveContainer, Legend,
} from 'recharts';

function CustomTooltip({ active, payload, label }) {
    if (!active || !payload?.length) return null;
    return (
        <div className="rounded-xl border border-white/10 bg-nyc-dark/95 px-4 py-3 shadow-glass backdrop-blur-xl">
            <p className="text-xs font-medium text-white/60">Hour: {label}:00</p>
            {payload.map((entry) => (
                <p key={entry.name} className="mt-1 text-sm font-semibold" style={{ color: entry.color }}>
                    {entry.name}: {entry.value?.toLocaleString()}
                </p>
            ))}
        </div>
    );
}

export default function TripDensityChart({ data }) {
    if (!data || data.length === 0) return null;

    // Pivot data: group by hour with member/casual as separate columns
    const pivoted = {};
    data.forEach(({ hour, type, count }) => {
        if (!pivoted[hour]) pivoted[hour] = { hour };
        pivoted[hour][type] = (pivoted[hour][type] || 0) + count;
    });
    const chartData = Object.values(pivoted).sort((a, b) => a.hour - b.hour);

    return (
        <div className="chart-container">
            <h3 className="section-title mb-4">📈 Temporal Trip Density</h3>
            <ResponsiveContainer width="100%" height={350}>
                <AreaChart data={chartData} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                    <defs>
                        <linearGradient id="memberGrad" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="0%" stopColor="#00d4ff" stopOpacity={0.5} />
                            <stop offset="100%" stopColor="#00d4ff" stopOpacity={0.05} />
                        </linearGradient>
                        <linearGradient id="casualGrad" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="0%" stopColor="#ff4b4b" stopOpacity={0.5} />
                            <stop offset="100%" stopColor="#ff4b4b" stopOpacity={0.05} />
                        </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
                    <XAxis dataKey="hour" stroke="rgba(255,255,255,0.2)" tick={{ fontSize: 11 }} />
                    <YAxis stroke="rgba(255,255,255,0.2)" tick={{ fontSize: 11 }} />
                    <Tooltip content={<CustomTooltip />} />
                    <Legend
                        wrapperStyle={{ fontSize: 12, color: 'rgba(255,255,255,0.6)' }}
                    />
                    <Area
                        type="monotone"
                        dataKey="member"
                        stroke="#00d4ff"
                        fill="url(#memberGrad)"
                        strokeWidth={2}
                        animationDuration={1200}
                    />
                    <Area
                        type="monotone"
                        dataKey="casual"
                        stroke="#ff4b4b"
                        fill="url(#casualGrad)"
                        strokeWidth={2}
                        animationDuration={1200}
                    />
                </AreaChart>
            </ResponsiveContainer>
        </div>
    );
}
