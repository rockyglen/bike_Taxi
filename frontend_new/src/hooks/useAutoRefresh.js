'use client';

import { useState, useEffect, useCallback } from 'react';

/**
 * Polls a URL at a given interval and returns { data, error, loading, lastRefreshed, refresh }.
 * @param {string} url - The URL to fetch
 * @param {number} intervalMs - Polling interval in milliseconds (default 30 min)
 */
export function useAutoRefresh(url, intervalMs = 30 * 60 * 1000) {
    const [data, setData] = useState(null);
    const [error, setError] = useState(null);
    const [loading, setLoading] = useState(true);
    const [lastRefreshed, setLastRefreshed] = useState(null);

    const refresh = useCallback(async () => {
        try {
            const res = await fetch(url, { cache: 'no-store' });
            const json = await res.json();
            if (json.error) {
                setError(json.error);
                setData(null);
            } else {
                setData(json);
                setError(null);
                setLastRefreshed(new Date());
            }
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    }, [url]);

    // Initial fetch
    useEffect(() => {
        refresh();
    }, [refresh]);

    // Set up polling interval
    useEffect(() => {
        const interval = setInterval(refresh, intervalMs);
        return () => clearInterval(interval);
    }, [refresh, intervalMs]);

    return { data, error, loading, lastRefreshed, refresh };
}
