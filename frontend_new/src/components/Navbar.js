'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { Activity, BarChart3 } from 'lucide-react';

const navItems = [
    { href: '/', label: 'Live Dashboard', icon: Activity },
    { href: '/monthly', label: 'Monthly Insights', icon: BarChart3 },
];

export default function Navbar() {
    const pathname = usePathname();

    return (
        <nav className="sticky top-0 z-50 border-b border-white/10 bg-nyc-dark/80 backdrop-blur-xl">
            <div className="mx-auto flex h-16 max-w-7xl items-center justify-between px-6">
                {/* Logo */}
                <Link href="/" className="flex items-center gap-3 group">
                    <span className="text-2xl">🏙️</span>
                    <div>
                        <span className="font-display text-lg font-bold tracking-tight text-white">
                            NYC Citi Bike
                        </span>
                        <span className="ml-2 text-xs font-semibold uppercase tracking-widest text-nyc-red">
                            Intelligence
                        </span>
                    </div>
                </Link>

                {/* Nav Links */}
                <div className="flex items-center gap-1">
                    {navItems.map(({ href, label, icon: Icon }) => {
                        const isActive = pathname === href;
                        return (
                            <Link
                                key={href}
                                href={href}
                                className={`flex items-center gap-2 rounded-lg px-4 py-2 text-sm font-medium transition-all duration-200 ${isActive
                                        ? 'bg-white/10 text-white'
                                        : 'text-white/50 hover:bg-white/5 hover:text-white/80'
                                    }`}
                            >
                                <Icon size={16} />
                                {label}
                            </Link>
                        );
                    })}
                </div>
            </div>
        </nav>
    );
}
