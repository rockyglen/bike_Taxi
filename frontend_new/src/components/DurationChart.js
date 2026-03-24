'use client';

import {
    BarChart, Bar, XAxis, YAxis, CartesianGrid,
    Tooltip, ResponsiveContainer, Legend,
} from 'recharts';

function CustomTooltip({ active, payload, label }) {
    if (!active || !payload?.length) return null;
    return (
        <div className="rounded-xl border border-white/10 bg-nyc-dark/95 px-4 py-3 shadow-glass backdrop-blur-xl">
            <p className="text-xs font-medium text-white/60">{label} min</p>
            {payload.map((entry) => (
                <p key={entry.name} className="mt-1 text-sm font-semibold" style={{ color: entry.color }}>
                    {entry.name}: {entry.value?.toLocaleString()}
                </p>
            ))}
        </div>
    );
}

export default function DurationChart({ data }) {
    if (!data || data.length === 0) return null;

    // Pivot: bin as X axis, member/casual as separate bars
    const pivoted = {};
    data.forEach(({ bin, binLabel, type, count }) => {
        if (!pivoted[bin]) pivoted[bin] = { bin, binLabel };
        pivoted[bin][type] = (pivoted[bin][type] || 0) + count;
    });
    const chartData = Object.values(pivoted).sort((a, b) => a.bin - b.bin);

    return (
        <div className="chart-container">
            <h3 className="section-title mb-4">⏱ Trip Duration Distribution</h3>
            <ResponsiveContainer width="100%" height={350}>
                <BarChart data={chartData} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
                    <XAxis dataKey="binLabel" stroke="rgba(255,255,255,0.2)" tick={{ fontSize: 10 }} />
                    <YAxis stroke="rgba(255,255,255,0.2)" tick={{ fontSize: 11 }} />
                    <Tooltip content={<CustomTooltip />} />
                    <Legend wrapperStyle={{ fontSize: 12, color: 'rgba(255,255,255,0.6)' }} />
                    <Bar dataKey="member" fill="#00d4ff" radius={[4, 4, 0, 0]} animationDuration={1200} />
                    <Bar dataKey="casual" fill="#ff4b4b" radius={[4, 4, 0, 0]} animationDuration={1200} />
                </BarChart>
            </ResponsiveContainer>
        </div>
    );
}
