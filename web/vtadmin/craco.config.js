module.exports = {
    style: {
        postcssOptions: {
            plugins: [
                require('tailwindcss'),
                // Required for tailwindcss
                require('autoprefixer'),
            ],
        },
    },
};
