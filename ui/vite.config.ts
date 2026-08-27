import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      "/api": "http://localhost:5100",
      "/v2": "http://localhost:5100",
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
});
