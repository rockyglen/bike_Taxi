'use client';

import {
    ScatterChart, Scatter, XAxis, YAxis, ZAxis,
    Tooltip, ResponsiveContainer,
} from 'recharts';

function CustomTooltip({ active, payload }) {
    if (!active || !payload?.length) return null;
    const d = payload[0]?.payload;
    return (
        <div className="rounded-xl border border-white/10 bg-nyc-dark/95 px-4 py-3 shadow-glass backdrop-blur-xl">
            <p className="text-sm font-semibold text-white">{d?.station}</p>
            <p className="text-xs text-white/60">{d?.count?.toLocaleString()} trips</p>
        </div>
    );
}

/**
 * Geospatial scatter chart using lat/lng coordinates.
 * Mirrors the Altair Mercator projection scatter from monthly_insights.py.
 */
export default function StationMap({ data }) {
    if (!data || data.length === 0) return null;

    // Compute domain for proper framing
    const lats = data.map((d) => d.lat).filter(Boolean);
    const lngs = data.map((d) => d.lng).filter(Boolean);
    const latMin = Math.min(...lats) - 0.005;
    const latMax = Math.max(...lats) + 0.005;
    const lngMin = Math.min(...lngs) - 0.005;
    const lngMax = Math.max(...lngs) + 0.005;

    // Normalize count for bubble size
    const maxCount = Math.max(...data.map((d) => d.count));

    return (
        <div className="chart-container">
            <h3 className="section-title mb-4"> System Demand Heatmap</h3>
            <ResponsiveContainer width="100%" height={500}>
                <ScatterChart margin={{ top: 10, right: 10, left: 10, bottom: 10 }}>
                    <XAxis
                        dataKey="lng"
                        type="number"
                        domain={[lngMin, lngMax]}
                        name="Longitude"
                        stroke="rgba(255,255,255,0.15)"
                        tick={{ fontSize: 10 }}
                    />
                    <YAxis
                        dataKey="lat"
                        type="number"
                        domain={[latMin, latMax]}
                        name="Latitude"
                        stroke="rgba(255,255,255,0.15)"
                        tick={{ fontSize: 10 }}
                    />
                    <ZAxis
                        dataKey="count"
                        type="number"
                        range={[20, 500]}
                        name="Volume"
                    />
                    <Tooltip content={<CustomTooltip />} />
                    <Scatter
                        data={data}
                        fill="#00d4ff"
                        fillOpacity={0.6}
                        animationDuration={1500}
                    />
                </ScatterChart>
            </ResponsiveContainer>
        </div>
    );
}
