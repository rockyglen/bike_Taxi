'use client';

import { useState, useEffect } from 'react';
import MetricCard from '@/components/MetricCard';
import TripDensityChart from '@/components/TripDensityChart';
import DonutChart from '@/components/DonutChart';
import DurationChart from '@/components/DurationChart';
import HBarChart from '@/components/HBarChart';
import StationMap from '@/components/LeafletMap';
import { Info, Lightbulb, Wrench } from 'lucide-react';

export default function MonthlyInsights() {
    const [data, setData] = useState(null);
    const [error, setError] = useState(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        fetch('/api/monthly')
            .then((res) => res.json())
            .then((json) => {
                if (json.error) {
                    setError(json.error);
                } else {
                    setData(json);
                }
                setLoading(false);
            })
            .catch((err) => {
                setError(err.message);
                setLoading(false);
            });
    }, []);

    if (loading) {
        return (
            <div className="flex min-h-[60vh] items-center justify-center">
                <div className="text-center">
                    <div className="mx-auto mb-4 h-12 w-12 animate-spin rounded-full border-4 border-nyc-cyan border-t-transparent" />
                    <p className="text-sm text-white/50">Processing monthly data...</p>
                </div>
            </div>
        );
    }

    if (error) {
        return (
            <div className="flex min-h-[60vh] items-center justify-center">
                <div className="max-w-lg text-center">
                    <p className="text-lg font-bold text-yellow-400"> Monthly stats not yet on S3.</p>
                    <div className="mt-4 text-left text-sm text-white/60">
                        <p className="mb-3">Run the feature engineering pipeline to generate them:</p>
                        <code className="block rounded-lg bg-white/5 px-4 py-2 text-xs text-white/70">
                            uv run scripts/feature_engineering.py
                        </code>
                        <p className="mt-4 text-xs text-white/40">
                            This runs automatically via GitHub Actions on the 1st of each month.
                            After the first run, this page will self-update monthly.
                        </p>
                    </div>
                </div>
            </div>
        );
    }

    const { summary, hourlyDensity, rideableData, durationData, topRoutes, geoData, topStations } = data;

    return (
        <div className="space-y-8">
            {/* Header */}
            <div>
                <h1 className="font-display text-4xl font-bold tracking-tight">
                    Monthly Insights <span className="text-nyc-cyan">Deep Dive</span>
                </h1>
                <p className="mt-2 text-base text-white/50">
                    Historical Performance Analysis: {summary.fileName?.replace('-citibike-tripdata.csv', '') || 'Latest Month'}
                </p>
            </div>

            {/* Metrics Row */}
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
                <MetricCard label="Total Trips" value={summary.totalTrips?.toLocaleString()} accentColor="cyan" />
                <MetricCard label="Avg Duration" value={`${summary.avgDuration} min`} accentColor="cyan" />
                <MetricCard label="Member Ratio" value={`${summary.memberRatio}%`} accentColor="cyan" />
                <MetricCard label="Peak Hour" value={`${summary.peakHour}:00`} accentColor="cyan" />
            </div>

            {/* Main Content: Charts + Sidebar */}
            <div className="flex flex-col gap-6 lg:flex-row">
                {/* Chart Area */}
                <div className="flex-1 space-y-6">
                    {/* Row 1: Density + Donut */}
                    <div className="grid grid-cols-1 gap-6 xl:grid-cols-3">
                        <div className="xl:col-span-2">
                            <TripDensityChart data={hourlyDensity} />
                        </div>
                        <div>
                            <DonutChart data={rideableData} />
                        </div>
                    </div>

                    {/* Row 2: Duration + Routes */}
                    <div className="grid grid-cols-1 gap-6 xl:grid-cols-2">
                        <DurationChart data={durationData} />
                        <HBarChart
                            data={topRoutes.map((r) => ({ label: r.route, count: r.count }))}
                            title=" High-Traffic Routes (O-D Pairs)"
                            color="#ff4b4b"
                        />
                    </div>

                    {/* Row 3: Geo Heatmap */}
                    <StationMap data={geoData} />

                    {/* Row 4: Top Stations */}
                    <HBarChart
                        data={topStations.map((s) => ({ label: s.station, count: s.count }))}
                        title=" Operational Hubs: Top 10 Stations"
                        color="#00d4ff"
                    />
                </div>

                {/* Sidebar */}
                <div className="w-full space-y-4 lg:w-72">
                    {/* Analysis Context */}
                    <div className="sidebar-panel rounded-2xl">
                        <div className="flex items-center gap-2 text-sm font-semibold text-white/80">
                            <Info size={16} className="text-nyc-cyan" />
                            Analysis Context
                        </div>
                        <div className="mt-3 rounded-lg bg-blue-500/10 px-3 py-2 text-xs text-blue-300">
                            Month: <span className="font-bold">{summary.fileName?.replace('-citibike-tripdata.csv', '') || 'Latest'}</span>
                            <br />
                            Dataset: <span className="font-bold">Citi Bike Open Data</span>
                        </div>
                    </div>

                    {/* AI Insights */}
                    <div className="sidebar-panel rounded-2xl">
                        <div className="flex items-center gap-2 text-sm font-semibold text-white/80">
                            <Lightbulb size={16} className="text-yellow-400" />
                            AI Insights
                        </div>
                        <p className="mt-3 text-xs leading-relaxed text-white/50">
                            Analysis shows a strong preference for electric bikes during peak hours,
                            particularly among casual riders.
                        </p>
                    </div>

                    {/* Data Source */}
                    <div className="sidebar-panel rounded-2xl">
                        <div className="flex items-center gap-2 text-sm font-semibold text-white/80">
                            <Wrench size={16} className="text-nyc-red" />
                            Data Source
                        </div>
                        <pre className="mt-3 rounded-lg bg-white/5 px-3 py-2 text-xs text-white/50">
                            {`S3: tripdata/${summary.fileName?.replace('.csv', '') || '...'}`}
                        </pre>
                    </div>
                </div>
            </div>

            {/* Footer */}
            <p className="pb-8 text-center text-xs text-white/20">
                NYC Fleet Intelligence | Built with Next.js, Recharts & S3
            </p>
        </div>
    );
}
