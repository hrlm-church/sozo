import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactCompiler: true,
  output: "standalone",
  serverExternalPackages: ["mssql"],
  devIndicators: false,
};

export default nextConfig;
