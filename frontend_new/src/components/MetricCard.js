'use client';

import { useCountUp } from '@/hooks/useCountUp';
import { motion } from 'framer-motion';

export default function MetricCard({ label, value, delta, accentColor = 'red', numericValue, prefix = '', suffix = '', decimals = 1 }) {
    // If numericValue is provided, animate it; otherwise display value directly
    const animated = useCountUp(numericValue ?? null, 1400, decimals);
    const displayValue = numericValue != null
        ? `${prefix}${animated.toLocaleString()}${suffix}`
        : value;

    const borderClass = accentColor === 'cyan' ? 'hover:border-nyc-cyan' : 'hover:border-nyc-red';
    const accentTextClass = accentColor === 'cyan' ? 'text-nyc-cyan' : 'text-nyc-red';

    return (
        <motion.div
            initial={{ opacity: 0, y: 16 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5 }}
            className={`glass-card ${borderClass} group`}
        >
            <p className="text-xs font-semibold uppercase tracking-widest text-white/40">
                {label}
            </p>
            <p className="mt-2 font-display text-3xl font-bold tracking-tight text-white transition-all duration-300 group-hover:scale-105">
                {displayValue}
            </p>
            {delta && (
                <p className={`mt-1 text-xs font-semibold ${accentTextClass}`}>
                    {delta}
                </p>
            )}
        </motion.div>
    );
}
