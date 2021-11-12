module.exports = {
    purge: ['./src/**/*.{js,jsx,ts,tsx}', './public/index.html'],
    darkMode: false,
    theme: {
        extend: {},
        fontFamily: {
            mono: ['NotoMono', 'source-code-pro', 'menlo', 'monaco', 'consolas', 'Courier New', 'monospace'],
        },
    },
    variants: {
        extend: {},
    },
    plugins: [],
};
