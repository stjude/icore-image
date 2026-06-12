/** Mirrors the previous cdn.tailwindcss.com default configuration so the
 * compiled stylesheet renders identically to the CDN-era pages. */
module.exports = {
  content: ['./src/**/*.{ts,tsx}'],
  theme: {
    extend: {},
  },
  plugins: [],
};
