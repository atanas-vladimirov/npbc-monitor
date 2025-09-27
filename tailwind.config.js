/** @type {import('tailwindcss').Config} */
module.exports = {
  // This is the crucial line that enables class-based dark mode.
  darkMode: 'class',
  content: [
    // This tells Tailwind to scan all your React components for class names.
    "./src/**/*.{js,jsx,ts,tsx}",
  ],
  theme: {
    extend: {},
  },
  plugins: [],
}
