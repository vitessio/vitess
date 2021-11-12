module.exports = {
    style: {
        postcss: {
            plugins: [
                require('tailwindcss'),
                // Required for tailwindcss
                require('autoprefixer'),
            ],
        },
    },
};
