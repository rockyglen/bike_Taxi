'use client';

import { useEffect, useState } from 'react';
import dynamic from 'next/dynamic';

// Leaflet must be dynamically imported (client-only, no SSR)
// This avoids "window is not defined" errors in Next.js
const MapComponent = dynamic(() => import('./_LeafletMapInner'), { ssr: false });

export default function LeafletMap({ data }) {
    if (!data || data.length === 0) return null;
    return (
        <div className="chart-container">
            <h3 className="section-title mb-2"> System Demand Heatmap</h3>
            <p className="mb-4 text-xs text-white/40">Station trip volume across NYC — circle size = demand intensity</p>
            <div className="overflow-hidden rounded-xl" style={{ height: 480 }}>
                <MapComponent data={data} />
            </div>
        </div>
    );
}
