module.exports = {
    purge: ['./src/**/*.{js,jsx,ts,tsx}', './public/index.html'],
    darkMode: false,
    theme: {
        extend: {
            textColor: {
                primary: '#17171b',
                secondary: '#718096',
                danger: '#D32F2F',
                warning: '#FFAB40',
                success: '#00893E',
                none: '#17171b',
                'vitess-blue': "#3D5AFE",
                'vitess-blue-50': "#8187ff",
                'vitess-blue-200': "#0031ca"
            },
            inset: {
                '-3full': '-300%'
            }
        },
        fontFamily: {
            mono: ['NotoMono', 'source-code-pro', 'menlo', 'monaco', 'consolas', 'Courier New', 'monospace'],
            sans: [
                'NotoSans',
                '-apple-system',
                'blinkmacsystemfont',
                'Segoe UI',
                'Roboto',
                'Oxygen',
                'Ubuntu',
                'Cantarell',
                'Fira Sans',
                'Droid Sans',
                'Helvetica Neue',
                'sans-serif',
            ],
        },
        // Presently, we use a 1rem = 10px basis, which diverges from
        // tailwind's (or, more specifically, most browsers') 16px default basis.
        fontSize: {
            sm: '1.2rem',
            base: '1.4rem',
            lg: '1.6rem',
            xl: '2rem',
            '2xl': '2.8rem',
            '3xl': '3.6rem',
        },
    },
    variants: {
        extend: {},
    },
    plugins: [],
};
