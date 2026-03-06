// frontend/vite.config.ts
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    host: true,
    port: 5173,
    proxy: {
      // Forward requests starting with /api -> http://localhost:8000 (your FastAPI)
      // The rewrite removes the /api prefix so /api/query -> /query on backend
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ""),
      },
      // Optional: expose metrics if you call /api/metrics
      "/metrics": {
        target: "http://localhost:8000",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/metrics/, "/metrics"),
      },
    },
  },
});