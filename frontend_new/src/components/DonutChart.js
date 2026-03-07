'use client';

import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer, Legend } from 'recharts';

const COLORS = ['#00d4ff', '#ff4b4b', '#a78bfa', '#34d399', '#fbbf24'];

function CustomTooltip({ active, payload }) {
    if (!active || !payload?.length) return null;
    return (
        <div className="rounded-xl border border-white/10 bg-nyc-dark/95 px-4 py-3 shadow-glass backdrop-blur-xl">
            <p className="text-sm font-semibold text-white">{payload[0].name}</p>
            <p className="text-xs text-white/60">{payload[0].value?.toLocaleString()} trips</p>
        </div>
    );
}

export default function DonutChart({ data }) {
    if (!data || data.length === 0) return null;

    return (
        <div className="chart-container">
            <h3 className="section-title mb-4">🚲 Rideable Preferences</h3>
            <ResponsiveContainer width="100%" height={350}>
                <PieChart>
                    <Pie
                        data={data}
                        dataKey="count"
                        nameKey="type"
                        cx="50%"
                        cy="50%"
                        innerRadius={60}
                        outerRadius={120}
                        paddingAngle={3}
                        animationDuration={1200}
                    >
                        {data.map((_, index) => (
                            <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                        ))}
                    </Pie>
                    <Tooltip content={<CustomTooltip />} />
                    <Legend
                        wrapperStyle={{ fontSize: 12, color: 'rgba(255,255,255,0.6)' }}
                    />
                </PieChart>
            </ResponsiveContainer>
        </div>
    );
}
