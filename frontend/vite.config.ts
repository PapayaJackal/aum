import { svelte } from "@sveltejs/vite-plugin-svelte";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [svelte()],
  server: {
    proxy: {
      "/api": "http://localhost:8000",
      "/metrics": "http://localhost:8000",
    },
  },
});
