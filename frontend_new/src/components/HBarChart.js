'use client';

import {
    BarChart, Bar, XAxis, YAxis, CartesianGrid,
    Tooltip, ResponsiveContainer,
} from 'recharts';

function CustomTooltip({ active, payload }) {
    if (!active || !payload?.length) return null;
    return (
        <div className="rounded-xl border border-white/10 bg-nyc-dark/95 px-4 py-3 shadow-glass backdrop-blur-xl">
            <p className="text-sm font-semibold text-white">{payload[0]?.payload?.label}</p>
            <p className="text-xs text-white/60">{payload[0].value?.toLocaleString()} trips</p>
        </div>
    );
}

/**
 * Horizontal bar chart. Used for Top Routes and Top Stations.
 * @param {{ data: { label: string, count: number }[], title: string, color?: string }} props
 */
export default function HBarChart({ data, title, color = '#ff4b4b' }) {
    if (!data || data.length === 0) return null;

    // Truncate long labels
    const chartData = data.map((d) => ({
        ...d,
        shortLabel: d.label.length > 40 ? d.label.slice(0, 37) + '...' : d.label,
    }));

    return (
        <div className="chart-container">
            <h3 className="section-title mb-4">{title}</h3>
            <ResponsiveContainer width="100%" height={350}>
                <BarChart
                    data={chartData}
                    layout="vertical"
                    margin={{ top: 0, right: 20, left: 10, bottom: 0 }}
                >
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" horizontal={false} />
                    <XAxis type="number" stroke="rgba(255,255,255,0.2)" tick={{ fontSize: 11 }} />
                    <YAxis
                        dataKey="shortLabel"
                        type="category"
                        width={180}
                        stroke="rgba(255,255,255,0.2)"
                        tick={{ fontSize: 10 }}
                    />
                    <Tooltip content={<CustomTooltip />} />
                    <Bar
                        dataKey="count"
                        fill={color}
                        radius={[0, 8, 8, 0]}
                        animationDuration={1200}
                    />
                </BarChart>
            </ResponsiveContainer>
        </div>
    );
}
