import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig(() => {
  return {
    build: {
      outDir: 'build',
    },
    plugins: [react()],
    test: {
      globals: true,
      setupFiles: './tests/setup.js',
      environment: 'jsdom',
    }
  };
});