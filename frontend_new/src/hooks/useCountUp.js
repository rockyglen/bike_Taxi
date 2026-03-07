'use client';

import { useState, useEffect, useRef } from 'react';

/**
 * Animates a number from 0 to `target` over `duration` ms.
 * Returns the current animated value (rounded to `decimals` places).
 */
export function useCountUp(target, duration = 1200, decimals = 1) {
    const [value, setValue] = useState(0);
    const startTime = useRef(null);
    const animationFrame = useRef(null);

    useEffect(() => {
        if (target === undefined || target === null || isNaN(target)) return;

        const start = () => {
            startTime.current = null;
            const step = (timestamp) => {
                if (!startTime.current) startTime.current = timestamp;
                const progress = Math.min((timestamp - startTime.current) / duration, 1);
                // Ease out cubic
                const eased = 1 - Math.pow(1 - progress, 3);
                setValue(parseFloat((eased * target).toFixed(decimals)));
                if (progress < 1) {
                    animationFrame.current = requestAnimationFrame(step);
                }
            };
            animationFrame.current = requestAnimationFrame(step);
        };

        start();
        return () => cancelAnimationFrame(animationFrame.current);
    }, [target, duration, decimals]);

    return value;
}
