import { test, expect, type Page, type Route } from "@playwright/test";

const result = (name: string) => ({
  results: [
    {
      doc_id: name,
      display_path: name,
      display_path_highlighted: "",
      score: 1,
      snippet: "Quarterly revenue",
      metadata: { content_type: "text/plain" },
      index: "docs",
    },
  ],
  total: 1,
  facets: { Author: ["Alice", "Bob"] },
});

async function setup(page: Page) {
  await page.route("**/api/auth/providers", (route) => route.fulfill({ json: { providers: [], public_mode: false } }));
  await page.route("**/api/auth/login", (route) =>
    route.fulfill({ json: { session_token: "test-session", token_type: "bearer" } }),
  );
  await page.route("**/api/indices", (route) =>
    route.fulfill({ json: { indices: [{ name: "docs", has_embeddings: false }] } }),
  );
  await page.route("**/api/search?*", (route) => route.fulfill({ json: result("report.txt") }));
  await page.goto("/");
  await page.getByLabel("Username").fill("reader");
  await page.getByLabel("Password").fill("Test1234!");
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  await expect(page.getByRole("button", { name: "Logout" })).toBeVisible();
}

async function search(page: Page) {
  await page.getByRole("searchbox").fill("revenue");
  await page.getByRole("button", { name: "Search", exact: true }).click();
  await expect(page.getByRole("option").filter({ hasText: "report.txt" })).toBeVisible();
}

test("login, search, document text, and original download", async ({ page }) => {
  await setup(page);
  await page.route("**/api/documents/report.txt?*", (route) =>
    route.fulfill({
      json: {
        doc_id: "report.txt",
        display_path: "report.txt",
        content: "Quarterly revenue increased significantly.",
        metadata: { content_type: "text/plain" },
        attachments: [],
        extracted_from: null,
        thread: [],
      },
    }),
  );
  await page.route("**/api/documents/report.txt/download?*", (route) =>
    route.fulfill({
      body: "Quarterly revenue increased significantly.",
      contentType: "text/plain",
      headers: { "Content-Disposition": 'attachment; filename="report.txt"' },
    }),
  );
  await search(page);
  await page.getByRole("option").filter({ hasText: "report.txt" }).click();
  await expect(page.getByText("increased significantly.", { exact: false })).toBeVisible();
  const downloaded = page.waitForEvent("download");
  await page.getByRole("button", { name: "Original" }).click();
  expect((await downloaded).suggestedFilename()).toBe("report.txt");
});

test("logout revokes the session and supports retry after failure", async ({ page }) => {
  await setup(page);
  let attempts = 0;
  await page.route("**/api/auth/logout", (route) => {
    expect(route.request().method()).toBe("POST");
    expect(route.request().headers().authorization).toBe("Bearer test-session");
    return route.fulfill({ status: ++attempts === 1 ? 500 : 204 });
  });
  await page.getByRole("button", { name: "Logout" }).click();
  await expect(page.getByRole("alert")).toContainText("Could not sign out");
  expect(await page.evaluate(() => localStorage.getItem("aum_session_token"))).toBe("test-session");
  await page.getByRole("button", { name: "Logout" }).click();
  await expect(page.getByLabel("Username")).toBeVisible();
  expect(await page.evaluate(() => localStorage.getItem("aum_session_token"))).toBeNull();
});

test("logout handles an already expired session", async ({ page }) => {
  await setup(page);
  await page.route("**/api/auth/logout", (route) => route.fulfill({ status: 401 }));
  await page.getByRole("button", { name: "Logout" }).click();
  await expect(page.getByLabel("Username")).toBeVisible();
  expect(await page.evaluate(() => localStorage.getItem("aum_session_token"))).toBeNull();
});

for (const clear of [false, true]) {
  test(
    clear ? "clearing a search ignores its pending response" : "late responses cannot replace newer results",
    async ({ page }) => {
      // Simulate a transport that completes after cancellation to test the state guard.
      await page.addInitScript(() => {
        const original = window.fetch;
        window.fetch = (input, options) => original(input, { ...options, signal: undefined });
      });
      await setup(page);
      await search(page);
      let stale: Route | undefined;
      await page.route("**/api/search?*", async (route) => {
        const filters = new URL(route.request().url()).searchParams.get("filters") ?? "";
        if (filters.includes("Bob")) await route.fulfill({ json: result("newest.txt") });
        else stale = route;
      });
      await page.getByRole("checkbox", { name: "Alice" }).click();
      await expect.poll(() => !!stale).toBe(true);
      if (clear) {
        await page.getByRole("link", { name: "aum — home" }).click();
      } else {
        await page.getByRole("checkbox", { name: "Bob" }).click();
        await expect(page.getByRole("option").filter({ hasText: "newest.txt" })).toBeVisible();
      }
      const oldResponse = page.waitForResponse((response) => response.url() === stale!.request().url());
      await stale!.fulfill({ json: result("stale.txt") });
      await oldResponse;
      await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))));
      await expect(page.getByRole("option").filter({ hasText: "stale.txt" })).toHaveCount(0);
      if (clear) await expect(page.getByRole("listbox", { name: "Search results" })).toHaveCount(0);
      else await expect(page.getByRole("option").filter({ hasText: "newest.txt" })).toBeVisible();
    },
  );
}
