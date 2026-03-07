'use client';

import { useEffect } from 'react';
import { MapContainer, TileLayer, CircleMarker, Tooltip } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';

// NYC center coordinates
const NYC_CENTER = [40.7128, -74.006];

function getColor(count, maxCount) {
    // Interpolate from cyan (#00d4ff) at low to red (#ff4b4b) at high
    const ratio = Math.min(count / maxCount, 1);
    if (ratio < 0.33) return '#00d4ff';
    if (ratio < 0.66) return '#a78bfa';
    return '#ff4b4b';
}

function getRadius(count, maxCount) {
    const ratio = Math.min(count / maxCount, 1);
    return 4 + ratio * 22; // 4px min, 26px max
}

export default function LeafletMapInner({ data }) {
    const maxCount = Math.max(...data.map((d) => d.count));

    return (
        <MapContainer
            center={NYC_CENTER}
            zoom={12}
            style={{ height: '100%', width: '100%', background: '#0e1117' }}
            preferCanvas
        >
            <TileLayer
                attribution='&copy; <a href="https://carto.com/">CARTO</a>'
                url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
            />
            {data.map((station, i) => (
                <CircleMarker
                    key={i}
                    center={[station.lat, station.lng]}
                    radius={getRadius(station.count, maxCount)}
                    pathOptions={{
                        color: getColor(station.count, maxCount),
                        fillColor: getColor(station.count, maxCount),
                        fillOpacity: 0.7,
                        weight: 1,
                    }}
                >
                    <Tooltip>
                        <span className="font-semibold">{station.station}</span>
                        <br />
                        {station.count.toLocaleString()} trips
                    </Tooltip>
                </CircleMarker>
            ))}
        </MapContainer>
    );
}
