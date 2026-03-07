import { NextResponse } from 'next/server';
import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse/sync';

/**
 * GET /api/monthly
 * Reads local monthly CSV data and computes all aggregates
 * needed by the Monthly Insights dashboard.
 */
export async function GET() {
    try {
        // Look for the CSV file in the project's data directory
        const dataDir = path.join(process.cwd(), '..', 'data');
        const csvFiles = fs.existsSync(dataDir)
            ? fs.readdirSync(dataDir).filter((f) => f.endsWith('-citibike-tripdata.csv'))
            : [];

        if (csvFiles.length === 0) {
            return NextResponse.json(
                { error: 'Monthly data file not found. Please download the CSV to the data/ folder.' },
                { status: 404 }
            );
        }

        // Use the most recent CSV file
        const csvPath = path.join(dataDir, csvFiles.sort().reverse()[0]);
        const csvContent = fs.readFileSync(csvPath, 'utf-8');

        const rawRecords = parse(csvContent, {
            columns: true,
            skip_empty_lines: true,
            relax_column_count: true,
        });

        // Process each record
        const records = [];
        for (const row of rawRecords) {
            const startedAt = new Date(row.started_at);
            const endedAt = new Date(row.ended_at);
            const durationMin = (endedAt - startedAt) / 60000;

            // Outlier removal (same as Streamlit: >1 min and <240 min)
            if (durationMin <= 1 || durationMin >= 240) continue;
            if (isNaN(durationMin)) continue;

            records.push({
                hour: startedAt.getHours(),
                dayName: startedAt.toLocaleDateString('en-US', { weekday: 'long' }),
                memberCasual: row.member_casual || '',
                rideableType: row.rideable_type || '',
                durationMin,
                startStationName: row.start_station_name || 'Unknown',
                endStationName: row.end_station_name || 'Unknown',
                startLat: parseFloat(row.start_lat) || null,
                startLng: parseFloat(row.start_lng) || null,
            });
        }

        const totalTrips = records.length;
        const avgDuration = records.reduce((s, r) => s + r.durationMin, 0) / totalTrips;
        const memberCount = records.filter((r) => r.memberCasual === 'member').length;
        const memberRatio = (memberCount / totalTrips) * 100;

        // Peak hour (mode)
        const hourCounts = {};
        records.forEach((r) => {
            hourCounts[r.hour] = (hourCounts[r.hour] || 0) + 1;
        });
        const peakHour = Object.entries(hourCounts).reduce((a, b) => (b[1] > a[1] ? b : a))[0];

        // --- Hourly by member/casual ---
        const hourlyByType = {};
        records.forEach((r) => {
            const key = `${r.hour}-${r.memberCasual}`;
            hourlyByType[key] = (hourlyByType[key] || 0) + 1;
        });
        const hourlyDensity = Object.entries(hourlyByType).map(([key, count]) => {
            const [hour, type] = key.split('-');
            return { hour: parseInt(hour), type, count };
        }).sort((a, b) => a.hour - b.hour);

        // --- Rideable type counts ---
        const rideableCounts = {};
        records.forEach((r) => {
            rideableCounts[r.rideableType] = (rideableCounts[r.rideableType] || 0) + 1;
        });
        const rideableData = Object.entries(rideableCounts).map(([type, count]) => ({ type, count }));

        // --- Duration distribution (histogram bins) ---
        const binSize = 5; // 5-minute bins
        const maxBin = 60; // Up to 60 min
        const durationBins = {};
        records.forEach((r) => {
            const bin = Math.min(Math.floor(r.durationMin / binSize) * binSize, maxBin);
            const key = `${bin}-${r.memberCasual}`;
            durationBins[key] = (durationBins[key] || 0) + 1;
        });
        const durationData = Object.entries(durationBins).map(([key, count]) => {
            const [bin, type] = key.split('-');
            return { bin: parseInt(bin), binLabel: `${bin}-${parseInt(bin) + binSize}`, type, count };
        }).sort((a, b) => a.bin - b.bin);

        // --- Top routes ---
        const routeCounts = {};
        records.forEach((r) => {
            const route = `${r.startStationName} → ${r.endStationName}`;
            if (r.startStationName !== 'Unknown' && r.endStationName !== 'Unknown') {
                routeCounts[route] = (routeCounts[route] || 0) + 1;
            }
        });
        const topRoutes = Object.entries(routeCounts)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 10)
            .map(([route, count]) => ({ route, count }));

        // --- Station geo data (for heatmap) ---
        const stationGeo = {};
        records.forEach((r) => {
            if (r.startLat && r.startLng && r.startStationName !== 'Unknown') {
                if (!stationGeo[r.startStationName]) {
                    stationGeo[r.startStationName] = { lat: r.startLat, lng: r.startLng, count: 0 };
                }
                stationGeo[r.startStationName].count += 1;
            }
        });
        const geoData = Object.entries(stationGeo)
            .map(([name, d]) => ({ station: name, lat: d.lat, lng: d.lng, count: d.count }))
            .sort((a, b) => b.count - a.count);

        // --- Top 10 stations ---
        const stationCounts = {};
        records.forEach((r) => {
            if (r.startStationName !== 'Unknown') {
                stationCounts[r.startStationName] = (stationCounts[r.startStationName] || 0) + 1;
            }
        });
        const topStations = Object.entries(stationCounts)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 10)
            .map(([station, count]) => ({ station, count }));

        return NextResponse.json({
            summary: {
                totalTrips,
                avgDuration: Math.round(avgDuration * 10) / 10,
                memberRatio: Math.round(memberRatio * 10) / 10,
                peakHour: parseInt(peakHour),
                fileName: path.basename(csvPath),
            },
            hourlyDensity,
            rideableData,
            durationData,
            topRoutes,
            geoData: geoData.slice(0, 500), // Limit for performance
            topStations,
        });
    } catch (err) {
        console.error('Monthly API Error:', err);
        return NextResponse.json(
            { error: `Failed to process monthly data: ${err.message}` },
            { status: 500 }
        );
    }
}
