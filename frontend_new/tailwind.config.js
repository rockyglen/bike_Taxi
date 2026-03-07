/** @type {import('tailwindcss').Config} */
module.exports = {
    content: [
        './src/**/*.{js,jsx,ts,tsx}',
    ],
    darkMode: 'class',
    theme: {
        extend: {
            fontFamily: {
                sans: ['Inter', 'system-ui', 'sans-serif'],
                display: ['Outfit', 'system-ui', 'sans-serif'],
            },
            colors: {
                nyc: {
                    dark: '#0e1117',
                    card: '#161b22',
                    border: 'rgba(255, 255, 255, 0.1)',
                    red: '#ff4b4b',
                    'red-light': '#ff8a8a',
                    cyan: '#00d4ff',
                    'cyan-light': '#66e3ff',
                },
            },
            boxShadow: {
                glass: '0 8px 32px 0 rgba(0, 0, 0, 0.37)',
            },
            backdropBlur: {
                glass: '10px',
            },
        },
    },
    plugins: [],
};
