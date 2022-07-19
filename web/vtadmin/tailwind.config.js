module.exports = {
    content: ['./src/**/*.{js,jsx,ts,tsx}', './public/index.html'],
    theme: {
        extend: {
            colors: {
                danger: {
                    DEFAULT: '#D32F2F',
                    50: '#FF6659',
                    200: '#9a0007',
                },
                gray: {
                    75: '#FAFAFA',
                    100: '#F3F3F3',
                    200: '#EDF2F7',
                    400: '#CBD5E0',
                    600: '#7A8096',
                    800: '#2D3748',
                    900: '#1E2531',
                },
                success: {
                    DEFAULT: '#00893E',
                    50: '#4CBA6A',
                    200: '#005A13',
                },
                vtblue: {
                    DEFAULT: '#3D5AfE',
                    50: '#8187FF',
                    200: '#0031CA',
                    dark: {
                        DEFAULT: '#8187FF',
                        50: '#B6B7FF',
                        200: '#4A5ACB',
                    },
                },
                warning: {
                    DEFAULT: '#FFAB40',
                    50: '#FFDD71',
                    200: '#C77C02',
                },
            },
            textColor: {
                primary: '#17171b',
                secondary: '#718096',
                none: '#17171b',
            },
            inset: {
                '-3full': '-300%',
            },
        },
        fontFamily: {
            mono: ['source-code-pro', 'menlo', 'monaco', 'consolas', 'Courier New', 'monospace'],
            sans: [
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
    plugins: [],
};
