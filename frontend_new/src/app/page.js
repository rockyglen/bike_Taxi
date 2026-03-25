'use client';

import { useState } from 'react';
import { motion } from 'framer-motion';
import MetricCard from '@/components/MetricCard';
import DemandAreaChart from '@/components/DemandAreaChart';
import { useAutoRefresh } from '@/hooks/useAutoRefresh';
import {
    RefreshCw, AlertTriangle, Cpu, Database,
    GitBranch, Zap, BarChart2, Clock, ExternalLink,
    ChevronDown, ChevronUp, CheckCircle, Activity,
} from 'lucide-react';

//  Pipeline Stage Card 
function PipelineStage({ icon: Icon, title, subtitle, badge, delay = 0 }) {
    return (
        <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, delay }}
            className="relative flex flex-col gap-2 rounded-2xl border border-white/10 bg-white/[0.03] p-5 backdrop-blur"
        >
            <div className="flex items-center gap-3">
                <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-nyc-red/10">
                    <Icon size={18} className="text-nyc-red" />
                </div>
                <div>
                    <p className="text-sm font-bold text-white">{title}</p>
                    <p className="text-xs text-white/40">{subtitle}</p>
                </div>
            </div>
            {badge && (
                <span className="w-fit rounded-full bg-green-500/10 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-green-400">
                    {badge}
                </span>
            )}
        </motion.div>
    );
}

//  Stat Pill 
function StatPill({ value, label }) {
    return (
        <div className="rounded-full border border-white/10 bg-white/5 px-4 py-1.5">
            <span className="font-display text-sm font-bold text-white">{value}</span>
            <span className="ml-2 text-xs text-white/40">{label}</span>
        </div>
    );
}

//  Live Dot 
function LiveDot() {
    return (
        <span className="relative flex h-2.5 w-2.5">
            <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-green-400 opacity-75" />
            <span className="relative inline-flex h-2.5 w-2.5 rounded-full bg-green-400" />
        </span>
    );
}

//  Main Page 
export default function LiveDashboard() {
    const { data, error, loading, lastRefreshed, refresh } = useAutoRefresh('/api/predictions', 30 * 60 * 1000);
    const { data: metricsData } = useAutoRefresh('/api/model-metrics', 60 * 60 * 1000);
    const [showTable, setShowTable] = useState(false);
    const [isRefreshing, setIsRefreshing] = useState(false);

    const handleRefresh = async () => {
        setIsRefreshing(true);
        await refresh();
        setIsRefreshing(false);
    };

    //  Loading 
    if (loading) {
        return (
            <div className="flex min-h-[70vh] flex-col items-center justify-center gap-4">
                <div className="h-14 w-14 animate-spin rounded-full border-4 border-nyc-red border-t-transparent" />
                <p className="text-sm text-white/40">Connecting to S3 — fetching live predictions...</p>
            </div>
        );
    }

    //  Error 
    if (error) {
        return (
            <div className="flex min-h-[70vh] flex-col items-center justify-center gap-4 text-center">
                <p className="text-xl font-bold text-nyc-red"> {error}</p>
                <p className="text-sm text-white/40">Trigger the inference pipeline to generate predictions:</p>
                <code className="rounded-xl bg-white/5 px-5 py-3 text-sm text-white/60">
                    uv run scripts/inference.py
                </code>
            </div>
        );
    }

    const { predictions, summary, stationIds = [] } = data;

    return (
        <div className="space-y-12">

            {/*  HERO  */}
            <motion.section
                initial={{ opacity: 0, y: -10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.6 }}
                className="relative overflow-hidden rounded-3xl border border-white/10 bg-gradient-to-br from-white/[0.04] to-transparent p-8 backdrop-blur-xl"
            >
                {/* Background glow */}
                <div className="pointer-events-none absolute -right-20 -top-20 h-72 w-72 rounded-full bg-nyc-red/10 blur-3xl" />
                <div className="pointer-events-none absolute -bottom-20 -left-20 h-72 w-72 rounded-full bg-blue-500/10 blur-3xl" />

                <div className="relative flex flex-col gap-6 lg:flex-row lg:items-center lg:justify-between">
                    <div>
                        <div className="mb-4 flex flex-wrap items-center gap-3">
                            <span className="rush-badge"> Live System</span>
                            <div className="flex items-center gap-2 rounded-full border border-green-500/30 bg-green-500/10 px-3 py-1">
                                <LiveDot />
                                <span className="text-xs font-semibold text-green-400">Inference running hourly</span>
                            </div>
                        </div>
                        <h1 className="font-display text-4xl font-bold leading-tight tracking-tight lg:text-5xl">
                            NYC Citi Bike
                            <br />
                            <span className="text-nyc-red">Demand Intelligence</span> Core
                        </h1>
                        <p className="mt-3 max-w-xl text-base leading-relaxed text-white/50">
                            A production-grade MLOps pipeline that bridges a 20-day data lag using a
                            custom <span className="font-semibold text-white/70">Recursive Bridge strategy</span>,
                            delivering hourly 24-hour demand forecasts for NYC fleet operations.
                        </p>

                        {/* Stats pills */}
                        <div className="mt-5 flex flex-wrap gap-2">
                            <StatPill value="12 months" label="training window" />
                            <StatPill value="28 lags" label="feature horizon" />
                            <StatPill value="LightGBM" label="champion model" />
                            <StatPill value="Evidently AI" label="drift detection" />
                            <StatPill value="MLflow" label="experiment tracking" />
                        </div>
                    </div>

                    {/* Refresh control */}
                    <div className="flex flex-col items-start gap-2 lg:items-end">
                        <button
                            onClick={handleRefresh}
                            disabled={isRefreshing}
                            className="flex items-center gap-2 rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm font-medium text-white/70 transition-all hover:border-nyc-red hover:text-white disabled:opacity-40"
                        >
                            <RefreshCw size={14} className={isRefreshing ? 'animate-spin' : ''} />
                            {isRefreshing ? 'Refreshing...' : 'Refresh'}
                        </button>
                        {lastRefreshed && (
                            <p className="text-xs text-white/30">
                                Last synced: {lastRefreshed.toLocaleTimeString('en-US', {
                                    hour: '2-digit',
                                    minute: '2-digit',
                                    timeZone: 'America/New_York',
                                    timeZoneName: 'short'
                                })}
                            </p>
                        )}
                    </div>
                </div>
            </motion.section>

            {/*  ML PIPELINE CARDS  */}
            <section>
                <div className="mb-4 flex items-center gap-2">
                    <GitBranch size={16} className="text-nyc-red" />
                    <h2 className="text-sm font-bold uppercase tracking-widest text-white/40">Automated MLOps Pipeline</h2>
                </div>
                <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
                    <PipelineStage
                        icon={Database}
                        title="Feature Engineering"
                        subtitle="Runs 1st of every month · GitHub Actions"
                        badge=" Automated"
                        delay={0.05}
                    />
                    <PipelineStage
                        icon={BarChart2}
                        title="Champion/Challenger Training"
                        subtitle="LightGBM · Drift detection · MLflow tracked"
                        badge=" Automated"
                        delay={0.1}
                    />
                    <PipelineStage
                        icon={Zap}
                        title="Recursive Bridge Inference"
                        subtitle="Runs hourly · Bridges 20-day data lag"
                        badge=" Automated"
                        delay={0.15}
                    />
                </div>
            </section>

            {/*  LIVE FORECAST METRICS  */}
            <section>
                <div className="mb-4 flex items-center gap-2">
                    <Activity size={16} className="text-nyc-red" />
                    <h2 className="text-sm font-bold uppercase tracking-widest text-white/40">Live Forecast Output</h2>
                </div>
                <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
                    <MetricCard
                        label="Live Forecast"
                        numericValue={summary.liveForecast}
                        suffix=" trips/hr"
                        decimals={1}
                        delta="Current hour"
                    />
                    <MetricCard
                        label="24h Peak Demand"
                        numericValue={summary.peakDemand}
                        suffix=" trips/hr"
                        decimals={1}
                        delta="Projected"
                    />
                    <MetricCard
                        label="Peak Window"
                        value={
                            summary.peakHourET
                                ? summary.peakHourET.split(',')[1]?.trim().replace(':00:00', '').trim()
                                : '—'
                        }
                    />
                    <MetricCard
                        label="Last Data Sync (ET)"
                        value={
                            summary.lastSyncET
                                ? summary.lastSyncET.split(',')[1]?.trim().split(':').slice(0, 2).join(':').trim()
                                : '—'
                        }
                    />
                </div>
            </section>

            {/*  FORECAST CHART + SIDEBAR  */}
            <section className="flex flex-col gap-6 lg:flex-row">
                <div className="flex-1">
                    <DemandAreaChart data={predictions} stationIds={stationIds} />
                    {/* Recursive Bridge annotation */}
                    <div className="mt-3 flex items-start gap-3 rounded-xl border border-blue-500/20 bg-blue-500/5 px-4 py-3">
                        <span className="mt-0.5 text-base"></span>
                        <div>
                            <p className="text-xs font-bold text-blue-300">Recursive Bridge Active</p>
                            <p className="text-xs text-white/40">
                                The model walked forward hour-by-hour from the last known Citi Bike data point (~20 days ago)
                                to now, using each prediction as the next input — before generating this 24h forecast.
                            </p>
                        </div>
                    </div>
                </div>

                {/* Sidebar */}
                <div className="w-full space-y-4 lg:w-72">

                    {/* Model Intelligence */}
                    <div className="sidebar-panel rounded-2xl space-y-3">
                        <div className="flex items-center gap-2 text-sm font-bold text-white">
                            <Cpu size={16} className="text-nyc-cyan" />
                            Model Intelligence
                        </div>
                        {metricsData ? (
                            <>
                                {/* Performance Metrics */}
                                <div className="grid grid-cols-3 gap-2">
                                    {[
                                        { label: 'MAE', value: metricsData.mae?.toFixed(2) },
                                        { label: 'RMSE', value: metricsData.rmse?.toFixed(2) },
                                        { label: 'MAPE', value: metricsData.mape != null ? `${metricsData.mape.toFixed(1)}%` : '—' },
                                    ].map(({ label, value }) => (
                                        <div key={label} className="flex flex-col items-center rounded-lg bg-white/5 py-2">
                                            <span className="text-[10px] text-white/40 uppercase tracking-widest">{label}</span>
                                            <span className="text-sm font-bold text-nyc-cyan">{value ?? '—'}</span>
                                        </div>
                                    ))}
                                </div>

                                {/* Promotion status badge */}
                                <div className={`rounded-lg px-3 py-1.5 text-xs font-semibold flex items-center gap-2 ${metricsData.promotion_status === 'Promoted'
                                        ? 'bg-green-500/10 text-green-400'
                                        : 'bg-white/5 text-white/40'
                                    }`}>
                                    <span>{metricsData.promotion_status === 'Promoted' ? '' : ''}</span>
                                    <span>{metricsData.promotion_status === 'Promoted' ? 'Challenger Promoted' : 'Champion Retained'}</span>
                                </div>

                                {/* Top Features Mini-Bar */}
                                {metricsData.top_features && (
                                    <div className="space-y-1.5">
                                        <p className="text-[10px] uppercase tracking-widest text-white/30">Top Features</p>
                                        {Object.entries(metricsData.top_features).slice(0, 5).map(([feat, pct]) => (
                                            <div key={feat} className="flex items-center gap-2">
                                                <span className="w-14 truncate text-[10px] text-white/40">{feat}</span>
                                                <div className="flex-1 rounded-full bg-white/10 h-1.5">
                                                    <div
                                                        className="h-1.5 rounded-full bg-nyc-cyan"
                                                        style={{ width: `${pct}%` }}
                                                    />
                                                </div>
                                                <span className="text-[10px] text-white/40 w-6 text-right">{pct}%</span>
                                            </div>
                                        ))}
                                    </div>
                                )}

                                {/* Metadata */}
                                <div className="space-y-1 border-t border-white/5 pt-2">
                                    {[
                                        { label: 'Algorithm', value: 'LightGBM GBDT' },
                                        { label: 'Strategy', value: 'Recursive Bridge' },
                                        { label: 'Samples', value: `${(metricsData.n_train ?? 0).toLocaleString()} train` },
                                        { label: 'Tracking', value: 'MLflow / DagsHub' },
                                        { label: 'Last Run', value: metricsData.run_date ? new Date(metricsData.run_date).toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'America/New_York' }) : '—' },
                                    ].map(({ label, value }) => (
                                        <div key={label} className="flex items-center justify-between rounded-lg bg-white/5 px-3 py-1.5">
                                            <span className="text-xs text-white/40">{label}</span>
                                            <span className="text-xs font-semibold text-white">{value}</span>
                                        </div>
                                    ))}
                                </div>
                            </>
                        ) : (
                            // Fallback while loading or if no metrics exist yet
                            <div className="space-y-2">
                                {[
                                    { label: 'Algorithm', value: 'LightGBM GBDT' },
                                    { label: 'Strategy', value: 'Recursive Bridge' },
                                    { label: 'Feature Lags', value: '28 hours' },
                                    { label: 'Training Data', value: '12 months' },
                                    { label: 'Tracking', value: 'MLflow / DagsHub' },
                                    { label: 'Promotion', value: 'Champion vs Challenger' },
                                ].map(({ label, value }) => (
                                    <div key={label} className="flex items-center justify-between rounded-lg bg-white/5 px-3 py-1.5">
                                        <span className="text-xs text-white/40">{label}</span>
                                        <span className="text-xs font-semibold text-white">{value}</span>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>

                    {/* Rush Hour Alert */}
                    <div className="sidebar-panel rounded-2xl">
                        <div className="flex items-center gap-2 text-sm font-bold text-white">
                            <AlertTriangle size={16} className="text-yellow-400" />
                            Rush Hour Alert
                        </div>
                        <div className="mt-3">
                            {summary.rushStart ? (
                                <div className="rounded-lg bg-yellow-500/10 px-3 py-2 text-xs text-yellow-300">
                                     High demand between{' '}
                                    <span className="font-bold">{summary.rushStart?.split(',')[1]?.trim() || summary.rushStart}</span>
                                    {' '}and{' '}
                                    <span className="font-bold">{summary.rushEnd?.split(',')[1]?.trim() || summary.rushEnd}</span>
                                </div>
                            ) : (
                                <div className="rounded-lg bg-green-500/10 px-3 py-2 text-xs text-green-300">
                                    <CheckCircle size={12} className="mr-1 inline" />
                                    No critical peak demand expected.
                                </div>
                            )}
                        </div>
                    </div>

                    {/* Infrastructure */}
                    <div className="sidebar-panel rounded-2xl">
                        <div className="flex items-center gap-2 text-sm font-bold text-white">
                            <Database size={16} className="text-nyc-red" />
                            Infrastructure
                        </div>
                        <pre className="mt-3 rounded-lg bg-white/5 px-3 py-2 text-xs leading-relaxed text-white/50">
                            {`Cloud:   AWS S3 (Parquet)
Runners: GitHub Actions
Model:   S3 + MLflow
Drift:   Evidently AI
Img:     Docker + uv`}
                        </pre>
                    </div>
                </div>
            </section>

            {/*  DATA EXPLORER  */}
            <section>
                <button
                    onClick={() => setShowTable(!showTable)}
                    className="flex w-full items-center justify-between rounded-2xl border border-white/10 bg-white/[0.02] px-6 py-4 text-sm font-semibold text-white/50 transition-colors hover:text-white"
                >
                    <span> Developer Inspection — Raw Prediction Data</span>
                    {showTable ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
                </button>
                {showTable && (
                    <div className="mt-2 overflow-x-auto rounded-2xl border border-white/10 bg-white/[0.02] px-6 py-4">
                        <table className="w-full text-left text-xs text-white/60">
                            <thead>
                                <tr className="border-b border-white/10">
                                    <th className="pb-2 pr-4 font-semibold text-white/30">Time (ET)</th>
                                    <th className="pb-2 pr-4 font-semibold text-white/30">Total Trips/hr</th>
                                    {stationIds.map((sid, i) => (
                                        <th key={sid} className="pb-2 pr-4 font-semibold text-white/30">Station {i + 1}</th>
                                    ))}
                                    <th className="pb-2 font-semibold text-white/30">Generated At (ET)</th>
                                </tr>
                            </thead>
                            <tbody>
                                {predictions.map((row, i) => {
                                    const isPeak = row.totalTrips === summary.peakDemand;
                                    return (
                                        <tr key={i} className={`border-b border-white/5 ${isPeak ? 'bg-nyc-red/10 text-nyc-red' : ''}`}>
                                            <td className="py-1.5 pr-4 font-mono">{row.targetHourET}</td>
                                            <td className={`py-1.5 pr-4 font-mono font-bold ${isPeak ? 'text-nyc-red' : ''}`}>{row.totalTrips}</td>
                                            {stationIds.map((sid) => (
                                                <td key={sid} className="py-1.5 pr-4 font-mono text-white/50">{row[sid] ?? '—'}</td>
                                            ))}
                                            <td className="py-1.5 font-mono text-white/30">{row.generatedAtET}</td>
                                        </tr>
                                    );
                                })}
                            </tbody>
                        </table>
                    </div>
                )}
            </section>

            {/* Footer */}
            <p className="pb-8 text-center text-xs text-white/20">
                Built by Glen Louis · NYC Citi Bike Demand Intelligence · LightGBM + MLflow + AWS S3
            </p>
        </div>
    );
}
