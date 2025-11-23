import type { NextConfig } from "next";


const nextConfig: NextConfig = {
  async rewrites() {
    return [
      {
        source: "/api/geocode",
        destination: "http://localhost:5000/api/geocode"
      }
    ];
  },
};

export default nextConfig;
