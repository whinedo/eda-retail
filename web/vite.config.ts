/// <reference types="vitest/config" />
import react from "@vitejs/plugin-react";
import { defineConfig, loadEnv } from "vite";

// El backend (eda-web) escucha en 127.0.0.1:WEB_PORT (8000 por defecto) y solo admite Host y Origin
// de localhost. WEB_PORT se lee del entorno o del .env de la raíz del repositorio, como eda-web.
export default defineConfig(({ mode }) => {
  const webPort = loadEnv(mode, "..", "").WEB_PORT || "8000";
  return {
    plugins: [react()],
    server: {
      host: "localhost",
      port: 5173,
      strictPort: true,
      proxy: { "/api": { target: `http://127.0.0.1:${webPort}`, changeOrigin: false } },
    },
    test: {
      environment: "jsdom",
      setupFiles: ["./src/test/setup.ts"],
      restoreMocks: true,
    },
  };
});
