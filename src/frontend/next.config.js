const nextConfig = {
  eslint: { ignoreDuringBuilds: true },
  // Produces a self-contained output in .next/standalone — used by Docker
  output: 'standalone',
};

module.exports = nextConfig;
