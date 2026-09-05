# Browser regression tests

From `frontend/`:

```sh
npm ci
npm ci --prefix tests
(cd tests && npx playwright install chromium)
npm run check
npm run build
npm test
```

The suite starts a local Vite preview server and uses Chromium. API responses
are mocked in the browser tests; the Rust HTTP regression test separately
exercises real authentication, session revocation, and index permissions.
The test dependencies have a separate lockfile to preserve the production
frontend dependency hash used by the Nix build.

Pull requests run these checks along with the Rust workspace and Meilisearch
feature tests. Container publishing on main and version tags depends on all
checks passing.
