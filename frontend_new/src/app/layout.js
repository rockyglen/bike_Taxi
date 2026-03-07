import './globals.css';
import Navbar from '@/components/Navbar';

export const metadata = {
    title: 'NYC Citi Bike | Demand Intelligence',
    description: 'Premium predictive analytics for the NYC Citi Bike fleet — real-time demand forecasting and historical insights.',
};

export default function RootLayout({ children }) {
    return (
        <html lang="en" className="dark">
            <body className="min-h-screen bg-nyc-dark text-white antialiased">
                <Navbar />
                <main className="mx-auto max-w-7xl px-6 py-8">
                    {children}
                </main>
            </body>
        </html>
    );
}
