# Polymarket Data Analytics

This repository aggregates, enriches, and visualizes activity from selected Polymarket traders. The Node.js backend continuously polls the Polymarket Data API, normalizes trades, stores the results locally, and exposes a dashboard for analysing historical volume, profit/loss, and position snapshots. This project never places or mirrors trades; it is strictly a read-only collector built for monitoring, analytics, and research.

## Highlights

- **Live data stream** — polls trader feeds every second (configurable) and keeps the feeds in sync with local storage.
- **Structured storage** — writes JSON/SQLite snapshots inside `data/` and the accompanying logs so you can export or inspect the raw dataset anytime.
- **Express-backed dashboard** — serves `dashboard/dist` plus REST helpers that surface aggregated leaderboards, user history, and Binance ticker prices.
- **Utility scripts** — `src/scripts/` contains ad-hoc audits, simulations, and helpers that can be used to backfill data or verify the collector’s assumptions.

## Prerequisites

- Node.js 18 or newer
- (Optional) `dashboard` dev tools: `vite`, `TypeScript`


## Environment

Create a `.env` file in the project root (copy `.env.example`) and populate the values below:

| Variable | Description |
|----------|-------------|
| `USER_ADDRESSES` | Comma-separated list or JSON array of trader wallet addresses you want to analyse. |
| `PROXY_WALLET` | Your public wallet address used for activity tracking (no private actions). |
| `PRIVATE_KEY` | Private key for the proxy wallet; the collector uses it for read-only verification (no trades are executed). |
| `CLOB_HTTP_URL`<br>`CLOB_WS_URL` | Polymarket’s HTTP and WebSocket endpoints. |
| `RPC_URL` | Polygon RPC endpoint (Infura, Alchemy, etc.). |
| `USDC_CONTRACT_ADDRESS` | USDC token contract (default: Polygon mainnet address). |
| `POLYMARKET_API_*` | Optional API credentials if you want to elevate access (see `src/config/env.ts`). |

There are also numeric guards: `FETCH_INTERVAL`, `RETRY_LIMIT`, `REQUEST_TIMEOUT_MS`, `NETWORK_RETRY_LIMIT`, etc., to modulate how aggressively the collector polls the APIs. The code will throw an error if any critical value is missing or malformed.

## Building and Running

```bash
npm run build:all       # compiles TypeScript and builds the dashboard assets
npm run health-check    # validates the configuration and connectivity
NODE_ENV=production     # optional, but recommended
npm start               # launches the collector + API server
```

The Express app serves `dashboard/dist` (if it exists) so the UI and APIs live on the same host. The server also exposes helper endpoints for Binance tickers, user activity, and market leaderboards used by the dashboard.

## Development

- Run `npm run dev` for local backend execution (uses `ts-node`).
- Inside `dashboard/`, run `npm install` and `npm run dev` to experiment with the React/Vite frontend.
- When changing UI code, re-run `npm run build:ui` so `dashboard/dist` stays in sync with the backend.

## Data & Logging

The collector writes files into the `data/` directory (trade history, balances, etc.) and keeps detailed logs under `logs/`. These files can be analysed with your own scripts or exported for reporting.

## Scripts

Several helper scripts live under `src/scripts/`; run them directly with `npm run <script>` (for example, `npm run check-activity` or `npm run simulate`). These scripts share the same configuration as the main collector.

## Security & Deployment

- Keep `.env` out of version control; runtime secrets must be provided by your deployment platform (systemd, Docker, pm2, etc.).
- Regularly rotate the `PRIVATE_KEY` if you reuse the wallet elsewhere, even though no trades are executed from this project.
- For CI/CD, build both the backend and `dashboard` assets, upload `dist/` and `dashboard/dist/`, then restart the process manager with the updated files.

## Acknowledgements

- Built using the Polymarket library stack (`@polymarket/clob-client`, Express, TypeScript).
- Data visualized via a Vite + React frontend.
