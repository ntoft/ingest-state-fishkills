# ingest-state-fishkills

Fish-kill attribution: MD DNR state fish-kill hourly ingest sprite

## Role in the fish-kill attribution pipeline

See [environmental-attribution-setup.md](https://github.com/ntoft) and the runbook in the ops workspace for the full topology.

## Runtime contract

This sprite runs inside the WarmHub sprite runtime. The runtime injects:

- `WARMHUB_API_URL` — WarmHub API endpoint
- `WARMHUB_TOKEN` — short-lived minted access token
- `WARMHUB_REPO` — target repo identifier (e.g. `fish-kill-attribution/noaa-sst-daily`)

Additional credentials (e.g. `OPENROUTER_API_KEY` for persona sprites) are injected via a bound WarmHub credential set.

## Entry point

`bun run action` → `src/action.ts`.

## Local development

Requires a `.npmrc` configured for `@warmhub/sdk-ts` (GitHub Packages auth).

```bash
bun install
bun run check
```
