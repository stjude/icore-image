import { defineConfig } from '@rsbuild/core';
import { pluginReact } from '@rsbuild/plugin-react';

// API routes served by Django; everything else is an SPA route handled by
// react-router. The dev server proxies these to the Django dev server so the
// HMR workflow stays same-origin (cookies, CSRF).
const DJANGO_PREFIXES = [
  '/run_',
  '/api/',
  '/load_settings',
  '/save_settings',
  '/load_admin_settings',
  '/save_admin_settings',
  '/get_log_content',
  '/get_protocol_settings',
  '/get_spreadsheet_columns',
  '/cancel_task',
  '/delete_task',
  '/validate_sas_url',
  '/test_pacs_connection',
  '/verify_admin_password',
  '/reset_deid_settings',
  // NOTE: do not proxy '/static/' — the dev server emits its own chunks under
  // /static/js and /static/css, and proxying them to Django 404s the app.
];

export default defineConfig(({ command }) => ({
  plugins: [pluginReact()],
  source: {
    entry: { index: './src/main.tsx' },
  },
  html: {
    title: 'iCore',
  },
  output: {
    distPath: {
      root: '../deid/static/app',
    },
    assetPrefix: '/static/app/',
    // Only clean for real builds: `rsbuild dev` serves from memory, and
    // cleaning there would delete the production bundle Django serves.
    cleanDistPath: command === 'build',
  },
  server: {
    port: 3000,
    proxy: Object.fromEntries(
      DJANGO_PREFIXES.map((prefix) => [prefix, 'http://127.0.0.1:8000']),
    ),
    historyApiFallback: true,
  },
}));
