import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: ".",
  use: { baseURL: "http://127.0.0.1:4173", browserName: "chromium" },
  webServer: {
    cwd: "..",
    command: "npm run preview -- --host 127.0.0.1 --port 4173 --strictPort",
    url: "http://127.0.0.1:4173",
    reuseExistingServer: !process.env.CI,
  },
});
