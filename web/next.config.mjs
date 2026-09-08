/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  // Standalone output for a minimal Docker runtime image (deploy/web.Dockerfile):
  // just the server, its resolved dependency subset, and .next/static.
  output: "standalone",
};

export default nextConfig;
