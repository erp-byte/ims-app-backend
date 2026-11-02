> **Merged into the Candor backend as `fdk`.** This is the original fyersdk design document with paths, env vars and routes rewritten to the merged layout (`services/fdk_*`, `shared/fdk_*`, `FDK_*` env vars, `/fdk/*` routes). `fdk_main.py` (fdk_lifespan() + fdk_mount(app), imported by the backend's `main.py`) replaces the standalone entry point; there is no fdk-only process. The authoritative file list is `FDK_FILES.md`.

# Fyers Volume Spike Detector

## Project Overview
Dual volume spike detector for NSE stocks. Monitors Fyers WebSocket ticks and alerts on large trades via Telegram + Google Sheets.

- **Fyers detector**: Rs 3 Cr threshold, 540 large-cap symbols
- **Penny detector**: Rs 52 Lakh threshold, 209 small-cap symbols
- Both share one Shoonya session (authenticated once; the Fyers login was removed)

## Architecture

```
fdk_main.py (fdk_lifespan() + fdk_mount(app); imported by the backend's main.py — not a standalone process)
  GET  /fdk/health            -> status (disabled / unavailable / stopped + error / healthy / on_hold) + login_state
  POST /fdk/webhook/telegram  -> commands from Telegram (webhook mode; with no FDK_BASE_URL the login bot is long-polled instead)
  *    /fdk/mcp               -> MCP endpoint (news + detector tools; /fdk/mcp-<FDK_MCP_PATH_SECRET> if set)
  lifespan (only when FDK_ENABLED=true; any failure is logged and the backend carries on):
    startup: build orchestrator (worker thread, 120 s cap) -> Telegram webhooks (FDK_BASE_URL set) or long polling
             -> optional self-ping (FDK_SELF_PING) -> orchestrator.run() as a task
    shutdown: stop detectors, cancel the task

Orchestrator (fdk_supervisor_service/ochestrator.py)
  -> ShoonyaAuthenticator (headless login; one shared session token)
  -> FyersService (isolated fyers detector + controller)
  -> PennyService (isolated penny detector + controller)
  -> FyersSummaryService (isolated fyers summary generation)
  -> PennySummaryService (isolated penny summary generation)
  -> Market-hour scheduling (09:13 - 16:00 IST)
  -> Reactive auth refresh (only when Fyers rejects token)
```

## Directory Structure

```
Candor backend — only the fdk-owned paths are shown (FDK_FILES.md is the authoritative list)
├── fdk_main.py                      # fdk_lifespan() + fdk_mount(app); imported by the backend's main.py
├── fdk_requirements.txt             # fdk deps, pulled in by requirements.txt via -r
├── fdk_requirements_nodeps.txt      # fyers-apiv3, installed with pip --no-deps (see Dockerfile)
├── fdk_env.example                  # every FDK_* variable (values live in the backend's .env)
├── FDK_FILES.md                     # inventory: paths + functions (python scripts/fdk_files_md.py)
├── shared/                          # NOTE: the unprefixed shared/config_loader.py, constants.py, logger.py,
│   │                                #       exceptions.py, models.py are the backend's own — not fdk
│   ├── fdk_config_loader.py         # Env var loading -> AppConfig dataclass, fdk_enabled()
│   ├── fdk_constants.py             # Thresholds, timing, /fdk route prefix, FDK_BASE_URL
│   ├── fdk_logger.py                # get_logger(name) -> logging.getLogger("fdk.<name>")
│   ├── fdk_exceptions.py            # Custom exceptions
│   └── fdk_models.py                # Pydantic: TickData, TradeAlert, DetectorConfig
├── services/
│   ├── fdk_auth_service/
│   │   ├── shoonya_login.py         # Shoonya OAuth: code -> GenAcsTok, UserDetails, headless-browser login
│   │   ├── shoonya_authenticator.py # ShoonyaAuthenticator — cached -> browser -> Telegram link
│   │   ├── token_manager.py         # Supabase CRUD for the session table (shoonya_tokens)
│   │   ├── server.py                # handle_text(): commands + pasted login URL; POST /fdk/webhook/telegram
│   │   └── tools.py                 # register/delete Telegram webhook
│   ├── fdk_fyers_service/               # Isolated fyers detector + summary
│   │   ├── service.py               # FyersService (detector + controller lifecycle)
│   │   └── summary.py               # FyersSummaryService (summary generation)
│   ├── fdk_penny_service/               # Isolated penny detector + summary
│   │   ├── service.py               # PennyService (detector + controller lifecycle)
│   │   └── summary.py               # PennySummaryService (summary generation)
│   ├── fdk_detector_service/
│   │   ├── detector.py              # VolumeSpikeDetector (parameterized)
│   │   ├── tick_handler.py          # parse_tick(message)
│   │   ├── trade_analyzer.py        # analyze_trade() -> TradeAlert
│   │   └── websocket_manager.py     # Fyers data_ws wrapper
│   ├── fdk_supervisor_service/
│   │   ├── ochestrator.py           # Manages services + scheduling
│   │   ├── run_controller.py        # Thread mgmt for one detector
│   │   ├── schedular.py             # is_market_hours()
│   │   └── health_monitor.py        # Self-ping for Render keep-alive
│   ├── fdk_telegram_service/
│   │   ├── bot_handler.py           # TelegramSender (one per bot+chat pair)
│   │   ├── poller.py                # TelegramPoller — long polling on the login bot (no public URL)
│   │   ├── message_template.py      # Alert/auth message templates
│   │   └── command_parser.py        # parse_command(), parse_date()
│   ├── fdk_summary_service/
│   │   ├── summary_generator.py     # SummaryGenerator (shared base class)
│   │   └── summary_scheduler.py     # Sends at 16:30 IST daily
│   ├── fdk_sheets_service/
│   │   ├── sheet_manager.py         # GoogleSheetsManager
│   │   └── row_builder.py           # TradeAlert -> SheetRow
│   ├── fdk_mcp_service/
│   │   ├── server.py                # build_mcp(), served at /fdk/mcp (or /fdk/mcp-<secret>)
│   │   ├── news_tools.py            # RSS/article tools + send_news_to_telegram
│   │   └── fyers_tools.py           # get_detector_status, get_volume_summary, list_monitored_symbols, get_sector_mapping
│   └── fdk_sector_service/
│       ├── symbol_manager.py        # SymbolManager — Supabase CRUD for symbols + sectors
│       └── sector_mapper.py         # get_sector(symbol), initialized from Supabase
├── config/
│   ├── fdk_sectors.json             # 890 symbol->sector mappings (first-run seed)
│   └── fdk_symbols.json             # {"fyers": [...], "penny": [...]} (first-run seed)
├── fdk_tests/                       # pytest suite: test_*.py + conftest.py
│                                    # (the pre-modular monoliths fyers.py/penny.py/… were NOT carried over)
├── docs/fdk_CLAUDE.md               # this file
└── scripts/fdk_files_md.py          # regenerates FDK_FILES.md
```

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `FDK_FYERS_CLIENT_ID` | Optional. Fyers app id read only by the dormant Fyers data socket (there is no Fyers login any more); leave empty |
| `FDK_SHOONYA_USERNAME` | Shoonya user id (e.g. `FA12345`). The Shoonya OAuth login is the only login; with scheduling on the first login runs at 09:13 IST and `/rst` forces one at any time |
| `FDK_SHOONYA_PASSWORD` | Shoonya login password (typed into Shoonya's own login page by the headless browser) |
| `FDK_SHOONYA_TOTP_SECRET` | TOTP secret (Profile → Security → TOTP Setup) |
| `FDK_SHOONYA_CLIENT_ID` | "Client ID" on the API Key Generation screen, e.g. `FA12345_U` (default `<username>_U`; `FDK_SHOONYA_VENDORCODE` accepted as a fallback) |
| `FDK_SHOONYA_SECRET_CODE` | "Secret Code" on the same screen — checksum input for GenAcsTok (`FDK_SHOONYA_API_KEY` accepted as a fallback) |
| `FDK_SHOONYA_AUTO_LOGIN` | `true` (default): headless-browser login first (two attempts), Telegram link as fallback; `false`: Telegram link only |
| `FDK_SHOONYA_REDIRECT_URI` | Optional. Redirect URL registered on the Shoonya account, if any; the browser login also accepts the code when the post-login redirect goes there |
| `FDK_CHROME_BINARY` / `FDK_CHROMEDRIVER` | Optional paths for the headless browser (the image installs `/usr/bin/chromium` and `/usr/bin/chromedriver`) |
| `FDK_LOGIN_BOT_TOKEN` | Login bot token |
| `FDK_LOGIN_CHAT_ID` | Login chat ID — login success/failure, daily token reset, /hld /rst responses |
| `FDK_FYERS_TRADE_BOT_TOKEN` | Fyers trade bot token |
| `FDK_FYERS_TRADE_CHAT_ID` | Fyers trade alerts chat ID |
| `FDK_FYERS_SUMMARY_BOT_TOKEN` | Fyers summary bot token |
| `FDK_FYERS_SUMMARY_CHAT_ID` | Fyers daily summary chat ID |
| `FDK_PENNY_TRADE_BOT_TOKEN` | Penny trade bot token |
| `FDK_PENNY_TRADE_CHAT_ID` | Penny trade alerts chat ID |
| `FDK_PENNY_SUMMARY_BOT_TOKEN` | Penny summary bot token |
| `FDK_PENNY_SUMMARY_CHAT_ID` | Penny daily summary chat ID |
| `FDK_FYERS_GOOGLE_SHEETS_ID` | Fyers Google Sheet ID |
| `FDK_PENNY_GOOGLE_SHEETS_ID` | Penny Google Sheet ID |
| `FDK_GOOGLE_CREDENTIALS_JSON` | Google service account JSON blob |
| `FDK_SUPABASE_DSN` | PostgreSQL connection string |
| `FDK_PORT` | Read for parity only; unused inside the backend (uvicorn owns the port) |
| `FDK_SCHEDULING_ENABLED` | Enable market-hour scheduling (default true) |
| `FDK_ENABLED` | Master switch — nothing fdk starts unless truthy (default false) |
| `FDK_BASE_URL` | Public https URL of the backend; Telegram webhooks are registered to it. Empty = the login bot is long-polled instead (commands and the pasted login URL still work; no public URL needed) |
| `FDK_SELF_PING` | Opt-in 7-min self-ping thread, needs FDK_BASE_URL (default false; the backend has its own keep-alive) |
| `FDK_MCP_PATH_SECRET` | Optional; serves MCP at `/fdk/mcp-<secret>` instead of `/fdk/mcp` |
| `FDK_NEWS_BOT_TOKEN` / `FDK_NEWS_CHAT_ID` | Optional news bot used by the MCP `send_news_to_telegram` tool |
| `FDK_TELEGRAM_WEBHOOK_SECRET` | Recommended in webhook mode (1-256 chars of `A-Z a-z 0-9 _ -`, anything else fails start-up). Passed to `setWebhook` as `secret_token`; a delivery whose `X-Telegram-Bot-Api-Secret-Token` header does not match is answered 403. Without it, anyone who knows the public URL and a chat id can send commands |
| `FDK_GOOGLE_PROJECT_ID`, `FDK_GOOGLE_PRIVATE_KEY_ID`, `FDK_GOOGLE_PRIVATE_KEY`, `FDK_GOOGLE_CLIENT_EMAIL`, `FDK_GOOGLE_CLIENT_ID`, `FDK_GOOGLE_CLIENT_X509_CERT_URL` | Alternative to `FDK_GOOGLE_CREDENTIALS_JSON`: the individual service-account fields |

Every name carries the `FDK_` prefix; `fdk_env.example` is the authoritative list.

## Supabase Database

All tables auto-created on startup. DSN: `FDK_SUPABASE_DSN` (the project's PostgreSQL connection string)

**Table: `shoonya_tokens`** (managed by TokenManager; a `fyers_tokens` table from the Fyers era may still exist and is no longer used)
```sql
CREATE TABLE IF NOT EXISTS shoonya_tokens (
    id            SERIAL PRIMARY KEY,
    access_token  TEXT NOT NULL,
    timestamp     DOUBLE PRECISION NOT NULL,
    created_at    TEXT NOT NULL,
    refresh_token TEXT                              -- unused by Shoonya (NULL); kept for schema parity
);
```

**Table: `stock_symbols`** (managed by SymbolManager)
```sql
CREATE TABLE IF NOT EXISTS stock_symbols (
    id       SERIAL PRIMARY KEY,
    symbol   TEXT NOT NULL,
    detector TEXT NOT NULL,           -- 'fyers' or 'penny'
    active   BOOLEAN DEFAULT TRUE,
    UNIQUE(symbol, detector)
);
CREATE INDEX IF NOT EXISTS idx_stock_symbols_detector ON stock_symbols (detector, active);
```

**Table: `sector_mappings`** (managed by SymbolManager)
```sql
CREATE TABLE IF NOT EXISTS sector_mappings (
    id     SERIAL PRIMARY KEY,
    symbol TEXT UNIQUE NOT NULL,      -- 'NSE:TCS-EQ'
    sector TEXT NOT NULL              -- 'Information Technology'
);
CREATE INDEX IF NOT EXISTS idx_sector_mappings_symbol ON sector_mappings (symbol);
```

**Auto-seed:** On first run, if `stock_symbols` or `sector_mappings` are empty, `SymbolManager` seeds from `config/fdk_symbols.json` and `config/fdk_sectors.json`. After that, Supabase is the source of truth.

## Auth Flow (Shoonya OAuth)

Shoonya's API since 1 April 2026 (SEBI algo framework) has **no password/TOTP API login**; the
only way in is its OAuth page, and the token exchange must come from the **whitelisted static IP**.
The orchestrator runs this ladder (`services/fdk_auth_service/shoonya_authenticator.py`):

1. **Cached session**: `shoonya_tokens` row less than 8 h old → `UserDetails` check (Bearer token).
   Accepted → "Shoonya Authentication Successful! Session active." on the login bot.
2. **Headless-browser login** (`FDK_SHOONYA_AUTO_LOGIN=true`, Chrome in the image): opens
   `https://api.shoonya.com/OAuthlogin/investor-entry-level/login?api_key=<client id>&route_to=<user id>`,
   fills user id / password / TOTP, clicks LOGIN and reads the redirect's `code=` from the browser's
   network log — the recipe on Shoonya's own "Python Authentication (Selenium)" page. The redirect
   counts when it goes to a Shoonya host, to `FDK_SHOONYA_REDIRECT_URI`, or carries a long code. If
   no code arrives within 60 s the (re-rendered) form is re-read and resubmitted once with a fresh
   TOTP; a failure reports the page URL and its visible text. Two attempts a minute apart, then
   falls through; with no browser at all (`fatal`) it goes straight to the link. `/hld`, `/rst` and
   shutdown abort the browser mid-login.
3. **Telegram link**: the login bot sends `https://api.shoonya.com/OAuthlogin/authorize/oauth?client_id=<client id>`.
   The operator logs in (user id, password, OTP/TOTP) and pastes the full URL of the page they land
   on — it carries `?code=...` — into the login chat (webhook or long polling). The link is resent
   every 5 minutes until then; `/hld` and `/rst` cancel the wait. `/fdk/health` shows
   `login_state: awaiting_manual_login` meanwhile. Outside market hours (scheduling on) the wait
   pauses with a "Shoonya login paused" message and the supervisor re-enters it at the next open;
   `/rst` restarts it at once. While parked, `/fdk/health` shows `login_state: waiting_for_market_hours`
   and `last_auth_error` keeps the day's browser error, if any (codes are masked as `code=***`).
4. **Exchange**: `POST NorenWClientAPI/GenAcsTok` with `{"code", "checksum": sha256(client_id + secret_code + code), "uid"}`
   → `access_token` (valid for the trading day, no refresh). Stored in `shoonya_tokens`, sent as
   `Authorization: Bearer` afterwards. A rejected code is reported on Telegram and the wait continues.
5. **Then idle**: the tick feed is still the dormant Fyers socket (step 2 wires Shoonya's
   `wss://api.shoonya.com/NorenWSAPI/`), so no "Monitoring Started" message; `/fdk/health` shows
   `login_state: ok`, `login_method: cached|auto|manual` and `last_login_at`.

No redirect URL has to be hosted: the code is read from the browser (step 2) or pasted (step 3).
Timing: with `FDK_SCHEDULING_ENABLED=true` the first login runs at 09:13 IST; `/rst` forces one at
any time; the 17:00 IST wipe clears `shoonya_tokens` so the next open is a fresh login.

## Telegram Commands

Commands are received via webhook at `POST /fdk/webhook/telegram` (all 5 bots share this endpoint)
or, with `FDK_BASE_URL` empty, by long-polling the login bot — the webhook route then answers 404.
Only messages from the five configured chat ids are handled (`{"status": "ignored_chat"}` otherwise,
including a body with no chat); in webhook mode a set `FDK_TELEGRAM_WEBHOOK_SECRET` must match the
`X-Telegram-Bot-Api-Secret-Token` header (403 otherwise). A pasted login URL is redacted (`code=***`)
in the logs. `/fdk/health` reports `telegram_mode`: `webhook`, `webhook_failed` (a `setWebhook` call
failed at start-up — Telegram keeps delivering to the previous registration; restart to retry),
`polling` or `none`.

**Processing order:** commands → pasted `code=` URL (`code_received`, answered later by the exchange
result; `code_rejected` / `code_ignored`, each answered with a hint on the login bot) → `/sdt` date
input → ignore.
Any command cancels pending `/sdt` state (`_pending_summary_date` reset to `False`).

### `/hld [reason]` — Hold
Stops all detectors and enters hold mode. Use when market is closed (holidays, weekends, etc).

```
User sends:    /hld Holi holiday
Webhook:       server.py → parse_command() → _handle_hold()
Orchestrator:  hold() → stops both RunControllers, sets on_hold=True
Response:      Login bot sends hold confirmation message
Loop:          Orchestrator skips detector start while on_hold=True
Health API:    GET /fdk/health returns {"status": "on_hold", ...}
```

### `/rst` — Restart
Stops detectors, re-authenticates with Shoonya (works outside market hours too), and restarts everything.

```
User sends:    /rst
Webhook:       server.py → parse_command() → _handle_restart()
Orchestrator:  request_restart() → sets restart_requested=True, on_hold=False
Response:      Login bot sends restart confirmation message
Loop:          Orchestrator detects restart_requested → stops detectors →
               re-authenticates (full auth flow) → restarts detectors
```

### `/snd` — Send Summary Now
Immediately generates and sends today's summaries for both fyers and penny.

```
User sends:    /snd
Webhook:       server.py → _handle_send_summary()
Response:      Login bot: "Generating summaries..."
               Fyers summary bot: daily summary (+ 3-day/weekly based on day)
               Penny summary bot: daily summary (+ 3-day/weekly based on day)
```

### `/sdt` — Send Summary for Date
Two-step command: first prompts for a date, then generates summaries for that date.

```
User sends:    /sdt
Response:      Login bot: "Send the date (DD-MM-YYYY):"

User sends:    28-02-2026
Webhook:       server.py → _handle_summary_date_input()
               parse_date("28-02-2026") → checks day of week (Saturday)
Response:      Login bot: "Generating summaries for 28-02-2026 (Saturday)..."
               Fyers + Penny summary bots: summaries for that date

User sends:    abc (invalid)
Response:      Login bot: "Invalid date format. Use DD-MM-YYYY"
               (stays in date-waiting mode, user can retry)
```

### Command Registration
- `register_bot_commands()` in `fdk_auth_service/tools.py` auto-registers `/hld`, `/rst`, `/snd`, `/sdt` with Telegram Bot API on startup
- Optional: manually register via BotFather → select login bot → Edit Commands:
  ```
  hld - Hold — stop all detectors (market closed/holiday)
  rst - Restart — re-authenticate and restart all detectors
  snd - Send today's summary now
  sdt - Send summary for a specific date
  ```

## 5 Separate Telegram Bots

Each channel has its own bot_token + chat_id pair (`TelegramChannel` dataclass). The `TelegramSender` class wraps one channel — just call `sender.send(text)`.

| Bot | Env Vars | What gets sent there |
|-----|----------|---------------------|
| Login | `FDK_LOGIN_BOT_TOKEN` + `FDK_LOGIN_CHAT_ID` | Login success/failure, daily token reset, /hld /rst responses |
| Fyers Trade | `FDK_FYERS_TRADE_BOT_TOKEN` + `FDK_FYERS_TRADE_CHAT_ID` | Real-time fyers trade alerts (Rs 3Cr+) |
| Fyers Summary | `FDK_FYERS_SUMMARY_BOT_TOKEN` + `FDK_FYERS_SUMMARY_CHAT_ID` | Fyers end-of-day summary |
| Penny Trade | `FDK_PENNY_TRADE_BOT_TOKEN` + `FDK_PENNY_TRADE_CHAT_ID` | Real-time penny trade alerts (Rs 52L+) |
| Penny Summary | `FDK_PENNY_SUMMARY_BOT_TOKEN` + `FDK_PENNY_SUMMARY_CHAT_ID` | Penny end-of-day summary |

## Summary Service (16:30 IST Daily)

Reads trade data from Google Sheets, aggregates by symbol, sends top-15 ranking to summary bots.

```
At 16:30 IST (SummaryScheduler checks every 30s):
  │
  ├── Fyers SummaryGenerator:
  │     → Reads fyers Google Sheet (all today's rows)
  │     → Aggregates: count trades per symbol + total value in Cr
  │     → Sorts by count, takes top 15
  │     → Sends formatted HTML message via fyers_summary_sender
  │
  └── Penny SummaryGenerator:
        → Same logic, reads penny Google Sheet
        → Sends via penny_summary_sender
```

**Day-specific messages:**
- **Every day**: Daily summary (today's data)
- **Wednesday + Friday**: Daily + 3-day summary (last 3 days)
- **Friday**: Daily + 3-day + weekly summary (last 5 days)

**Column matching:** Finds `Date`, `Symbol`, and `Trd_Val_Cr` columns by header name (case-insensitive). Supports multiple date formats (DD-MM-YYYY, YYYY-MM-DD, etc).

**Independence:** Runs as a separate async task — not affected by `/hld` or detector state. Always sends if there's data in the sheet.

## Key Design Decisions

- **Shared token**: one ShoonyaAuthenticator, its session token passed to both services
- **Shared WebSocket via TickDispatcher**: `FyersDataSocket` is a singleton — only one WS connection per process. `TickDispatcher` subscribes to all 749 symbols on one WebSocket and routes each tick to the correct detector(s) by symbol lookup
- **Isolated services**: `FyersService` and `PennyService` each own their detector, controller, sheets, and senders — no shared state
- **Isolated summaries**: `FyersSummaryService` and `PennySummaryService` each own their `SummaryGenerator` — no shared state
- **Webhook on all bots**: All 5 bots have webhooks set to `/fdk/webhook/telegram` — commands work from any bot chat
- **Supabase not local JSON**: Token persists across deploys, INSERT-based audit trail
- **Parameterized detector**: Single `VolumeSpikeDetector` class serves both fyers/penny via `DetectorConfig`
- **Chat restriction, not user restriction**: messages are handled only when they come from one of the five configured chats; any member of those chats can send the commands
- **Reactive token refresh**: No timer/polling — detectors run until Fyers rejects the token, then orchestrator re-authenticates and restarts
- **Telegram commands**: `/hld` stops everything (holidays), `/rst` re-authenticates and restarts
- **IST everywhere**: All timestamps use `datetime.now(IST)` — works correctly regardless of server timezone

## Deployment

- Runs **inside the Candor backend process** — there is no fdk-only service. The backend `Dockerfile`
  (python:3.11-slim, `uvicorn main:app`) hosts it; nothing fdk starts unless `FDK_ENABLED=true`, and only
  ONE backend instance should enable it (each enabled instance runs its own detector and re-points the
  Telegram webhooks to its `FDK_BASE_URL`).
- Dependencies: `requirements.txt` pulls in `-r fdk_requirements.txt`; `fyers-apiv3` is installed separately
  with `pip install --no-deps -r fdk_requirements_nodeps.txt` because its metadata pins an older `requests`.
  A Render-native build command must run both installs, not just `pip install -r requirements.txt`.
- Host: the Shoonya token exchange and API calls only work from the static IP whitelisted on the
  account, so fdk runs on the EC2 box (Elastic IP), not on App Runner. With no public HTTPS URL
  there, leave `FDK_BASE_URL` empty: the login bot is long-polled for commands and the pasted
  login URL. With `FDK_BASE_URL` set, webhooks are registered to `FDK_BASE_URL` + `/fdk/webhook/telegram`.
  The MCP endpoint is `/fdk/mcp` (or `/fdk/mcp-<FDK_MCP_PATH_SECRET>`).
- The image installs chromium + chromium-driver for the headless login (`--build-arg FDK_BROWSER=0`
  skips them; the Telegram-link login still works without a browser).
- Self-ping is opt-in (`FDK_SELF_PING=true`, needs `FDK_BASE_URL`); the backend already runs its own
  keep-alive job. Hosts have run on UTC, so always use `datetime.now(IST)`, never a plain `datetime.now()`.

## Changelog

### 2026-11-02 - Browser login hardened; Telegram inbound locked to the configured chats
- First EC2 run: the headless login resubmitted the TOTP against stale form elements ("stale element
  reference") and would have kept resubmitting forever. `auto_login_code()` now re-reads the visible
  inputs before the single resubmit, is bounded (about 2 x 60 s plus a 30 s page-load timeout), polls
  a `cancelled` hook (`/hld`, `/rst`, shutdown), reports the page URL and visible text on failure,
  accepts the redirect when it goes to a Shoonya host, to `FDK_SHOONYA_REDIRECT_URI` or carries a
  long code, and flags a missing browser as `fatal` so the authenticator skips to the Telegram link.
- `AUTO_LOGIN_MAX_ATTEMPTS` 5 -> 2 (the recipe already resubmits once). The manual wait pauses
  outside market hours (`keep_waiting`, set by the orchestrator) so the link is not resent all night.
- Inbound Telegram: `handle_text(text, chat_id)` ignores chats other than the five configured ones,
  the poller listens to the login chat only, the webhook enforces `FDK_TELEGRAM_WEBHOOK_SECRET` when
  set, pasted codes are redacted in the logs, and a stray or unusable `code=` paste gets a hint back.
- `FDK_SHOONYA_CLIENT_ID` may be left blank (default `<username>_U`) instead of raising.
- Review follow-ups: the chat allowlist fails closed (a body with no chat is ignored; the poller
  hands the login chat id to the dispatcher); the webhook route answers 404 in polling mode;
  `FDK_TELEGRAM_WEBHOOK_SECRET` is validated against Telegram's charset at start-up; a failed
  `setWebhook` no longer skips the remaining bots and shows as `telegram_mode: webhook_failed`;
  the browser resubmit refills user id and password too (re-created inputs start blank);
  page diagnostics mask `code=` and point at `FDK_SHOONYA_REDIRECT_URI`; `login_state` reads
  `waiting_for_market_hours` while parked even after a failed day.

### 2026-11-02 - Shoonya OAuth login (the API in force since 1 April 2026)
- The QuickAuth/vendor-code login and the `NorenWClientTP` base were retired by Shoonya under SEBI's
  algo framework (they answer 502). `shoonya_login.py` now mirrors the vendor's NorenRestApiOAuth
  SDK: REST base `NorenWClientAPI`, `GenAcsTok` with `checksum = sha256(client_id + secret_code + code)`,
  `Authorization: Bearer` on every call, `UserDetails` to validate a stored session.
- Ladder in `ShoonyaAuthenticator`: cached session → headless-browser login on Shoonya's own page
  (official Selenium recipe; `FDK_SHOONYA_AUTO_LOGIN`) → Telegram link: the login bot sends the OAuth
  URL, the operator logs in and pastes the URL they land on (`code=...`), resent every 5 min.
  `/fdk/health` reports `login_state: awaiting_manual_login` and `login_method` (cached/auto/manual).
- Env: `FDK_SHOONYA_CLIENT_ID` (old `VENDORCODE`, default `<username>_U`) and `FDK_SHOONYA_SECRET_CODE`
  (old `API_KEY`) replace vendor code / API key / IMEI / redirect URI; old names still accepted.
- Telegram inbound without a public URL: `TelegramPoller` long-polls the login bot when
  `FDK_BASE_URL` is empty (`telegram_mode: polling` in health); `handle_text()` is the single dispatcher.
- Deployment: run on the host whose static IP is whitelisted at Shoonya; chromium + chromium-driver
  are installed in the image for the browser login (`--build-arg FDK_BROWSER=0` to skip).

### 2026-11-02 - Fyers login removed
- Deleted the Fyers webhook/TOTP login: `authenticator.py`, `totp_login.py`, `totp_handler.py`,
  `models.py` (AuthState), the `GET /fdk/auth/callback` route and the pasted-auth-code branch of the
  webhook, `auth_required_message`, the `FDK_BROKER` switch and the `FDK_FYERS_SECRET_KEY` /
  `REDIRECT_URI` / `TOTP_SECRET` / `PIN` / `USERNAME` variables, plus the two Fyers-login design docs.
- `ShoonyaAuthenticator` is the only login; `Orchestrator(config)` takes no auth state; the session
  table is `shoonya_tokens`.
- Kept, dormant: the Fyers data socket (`websocket_manager.py`, `fyers_apiv3`) and the optional
  `FDK_FYERS_CLIENT_ID` it reads. `TICK_FEED_WIRED = False` in `ochestrator.py` keeps the orchestrator
  idle after login until the Shoonya feed replaces that socket (step 2).
- Hardening from review: blank `FDK_SHOONYA_*` values are rejected at start-up (ValueError naming the
  variable, visible in `/fdk/health`); the supervisor retries its start-up Supabase loads instead of dying;
  `/fdk/health` reports `status: stopped` with the exception if the supervisor task ever ends.

### 2026-11-02 - Shoonya login (step 1 of the broker switch; FDK_BROKER since removed)
- `FDK_BROKER` selects the login: `shoonya` (default) or `fyers`; only the active broker's
  credential block is required (`shared/fdk_config_loader.py`: `ShoonyaConfig`, `AppConfig.broker`).
- `services/fdk_auth_service/shoonya_login.py`: headless login over the broker's REST API — the
  QuickAuth form post exactly as the vendor's NorenRestApiPy client builds it, plus the UserDetails
  post from the Shoonya REST docs (the vendor client has no such call) — with explicit timeouts and
  the broker's `emsg` surfaced; bounded, cancellable retries (`login_with_retry`). A malformed TOTP
  secret is rejected at fdk start-up (visible in `/fdk/health`) and reported as a login error otherwise.
- `services/fdk_auth_service/shoonya_authenticator.py`: ladder cached session (time-valid AND
  accepted by UserDetails) -> fresh login -> Telegram failure report + cancellable cooldown. Same
  control surface as `FyersAuthenticator` (`authenticate`, `cancel_auth`, `reset_cancel`, `reset`).
- `TokenManager(table=...)`: one Supabase table per broker (`fyers_tokens`, `shoonya_tokens`).
- Orchestrator: builds the broker's authenticator; for any broker other than Fyers it stays logged
  in and idles (warning logged once) instead of opening the Fyers data socket. `/rst` forces a
  login at any time; the 17:00 token wipe applies to both brokers.
- `/fdk/health` and the MCP `get_detector_status` tool report `broker`, `login_state`
  (ok / failed / on_hold / waiting_for_market_hours / in_progress), `authenticated`, `last_login_at`,
  `last_auth_error`, `market_hours` and `scheduling_enabled`, for both brokers. Shutdown cancels an
  in-flight login so the process exits promptly.
- Not yet: the Shoonya tick feed (websocket `EXCH|TOKEN` subscriptions, symbol-to-token mapping).

### 2026-11-02 - Merged into the Candor backend as `fdk`
- Layout: `services/fdk_<name>_service/` (folder copies, inner file names unchanged), `shared/fdk_*.py`,
  `config/fdk_*.json`, `fdk_tests/`, `fdk_main.py`; every env var is `FDK_*`; every route is under `/fdk`;
  loggers are `fdk.<name>`. Wired into the backend by three marked lines in `main.py`; inventory in `FDK_FILES.md`.
- `fdk_main.py` replaces the standalone `main.py`: `FDK_ENABLED` master switch, orchestrator built in a worker
  thread (120 s cap), any start-up failure logged and contained, MCP mounted at `/fdk/mcp` through a deferred
  ASGI forwarder, self-ping opt-in (`FDK_SELF_PING`), webhooks registered only when `FDK_BASE_URL` is set.
- Event loop is now shared with the IMS, so blocking I/O moved off it: Sheets reads in
  `SummaryGenerator.send_summary*`, Supabase/Sheets calls in `Orchestrator.run()`/`_daily_token_reset()`,
  Supabase/Fyers HTTP calls in `FyersAuthenticator`, and the socket teardown (`_close_dispatcher_async`,
  `hold_async` used by the `/hld` webhook) all run via `asyncio.to_thread`. `run()` re-checks
  `on_hold`/`restart_requested` after those await points before opening the socket or starting detectors.
- Dependencies: `fyers-apiv3` installed with `--no-deps` (`fdk_requirements_nodeps.txt`) because its metadata
  pins `requests==2.31.0`; its real needs are listed in `fdk_requirements.txt`; `setuptools<81` because newer
  releases dropped `pkg_resources`, which the SDK's websocket module imports.
- The pre-modular monoliths (`tests/fyers.py`, `penny.py`, `fyerssum.py`, `pennysum.py`, `health.py`) were NOT
  carried over: they embed live credentials and nothing imports them.

### 2026-09-19 - Penny Sheet data history: UTC timestamps and wrong-detector rows
- Found while joining Penny Sheet spikes to Fyers 1-minute candles in the
  analyzer project. **No code change**: current code stamps every row with
  explicit IST. This records what the historical Sheet data looks like, so any
  time-of-day analysis can correct it.
- **UTC until 2026-03-04 15:46 IST**: the old scripts (`fyersdk/tests/penny.py:1319`)
  and the first modular commit `f462b25` stamped Date/Time with a plain
  `datetime.now()`. The Render host evidently ran on UTC despite `TZ` in
  `render.yaml`, so don't rely on that setting. `6e6f0ae` (15:46 IST) switched
  `trade_analyzer` to `datetime.now(IST)`. All rows from 2026-03-05 on are IST.
  On 2026-03-04 only the last 2 rows (15:51–15:54) are IST.
- **Sanity bound**: the scanner window is 09:13–16:00 IST, so a UTC-stamped row
  always reads 03:43–10:30. Any row before the switch that reads later than
  10:30 is IST.
- **Fyers-scanner rows in the Penny Sheet**: 2,860 rows on 2026-02-02, 02-03,
  02-04, 02-13, 02-16 and 02-17. Every symbol is on the fyers list and none is
  on any penny list, the smallest `Trd_Val_Cr` is exactly 3.0 (the fyers
  threshold), and the times are IST. So on those days, IST-stamped rows are the
  fyers scanner's rows. The committed `fyersdk/tests/fyers.py` writes to a different
  Sheet ID, so the source was probably a local copy pointed at the Penny Sheet
  (not confirmed). The count is a floor: 57 symbols are on both lists.
- **Large caps at the penny threshold**: 1,488 rows of fyers-list symbols
  judged at the penny Rs 0.55 Cr threshold. On 2026-03-04 they read
  09:47–10:07 (UTC, so 15:17–15:37 IST, the first modular deploy); on
  2026-03-05 they run 09:15–09:59 IST. Before `TickDispatcher` (`199ef6c`,
  2026-03-05 11:14 IST), the shared `FyersDataSocket` delivered fyers ticks to
  the penny detector. Clean rows resume at 11:15.
- **Impact**: about 4,348 of 234,586 Penny Sheet rows (1.9%) come from the
  wrong detector. Drop or flag them rather than time-correcting them. `/sdt`
  summaries for those dates include them too.
- Not verified: the UTC reading of the 2026-03-04 burst rests on timing, not
  candle prices. It starts 11 minutes after `f462b25`, and the old script's
  rows stop at 15:14 IST.

### 2026-08-19 - WebSocket never recovered from a drop (mid-morning data stop)
- **Root cause**: the Fyers SDK's `__on_close` wipes `scrips_per_channel` and
  `symbol_token` before reconnecting, and its reconnect path never re-subscribes
  (`__on_open` sends only auth + mode). The app called `subscribe()` exactly
  once, at startup. So the first socket drop left it open and authenticated but
  subscribed to nothing — no ticks, no alerts, no sheet rows, for the rest of
  the process's life. Verified identical on 3.1.7 and the deployed 3.1.16.
- **Why it stayed invisible**: `RunController.is_running` stays True (the
  detector thread just blocks on `stop_event.wait()`), so `/fdk/health` reported
  healthy; the `on_close` callback fires *only* on a deliberate close, never on
  a drop; and after the retry budget is exhausted the SDK only prints
  "Connection abandoned" without any callback at all.
- **Fix**: register `on_connect` and re-subscribe on every open. Gated on the
  socket actually being open — `__on_open` REBINDS its outbound queue
  (`self.message = []`), so a timer-based subscribe races it and is discarded.
  Also registers `on_error`/`on_close` and raises `reconnect_retry` 5 -> 50.
- **Liveness supervision**: `is_alive()` reads `sock.connected` (the SDK's
  `is_connected()` only checks that its ws object exists, so it keeps returning
  True after abandonment). `needs_rebuild()` also covers "came up without a
  subscription". The orchestrator polls it and rebuilds, gated by the pure
  `should_rebuild_socket()` helper with `WS_REBUILD_COOLDOWN=60`.
- **token_expired is real now**: `TickDispatcher._on_auth_error` sets it on
  every detector when Fyers rejects auth (`type="cn"`, codes -99/-300). Nothing
  in the system had ever set it, so the documented reactive-refresh path was
  dead code.
- **Zombie-socket leak**: `close_connection()` is guarded on `__ws_object`, so
  closing during a reconnect window did nothing and left `restart_flag` True —
  the old instance kept reconnecting forever holding stale detector callbacks.
  `close()` now clears `restart_flag` first.
- `_connect_dispatcher()` moved to `asyncio.to_thread` — it can now block up to
  `SOCKET_OPEN_TIMEOUT`, which would otherwise stall the webhook, health check
  and summary scheduler.
- Tests: `fdk_tests/test_websocket_manager.py` (12), 4 added to
  `fdk_tests/test_schedular.py`. 48 passing.
- Still open: no tick-level watchdog (a socket that is connected but silently
  delivering nothing is not yet detected); `fyers-apiv3>=3.0.0` is unpinned and
  this fix depends on SDK internals.

### 2026-08-19 - Surface Telegram send failures (equity summary outage)
- **Root cause of silent outage**: `TelegramSender.send()` did
  `return resp.status_code == 200` and discarded the response body, so a
  rejected send (revoked token, wrong/migrated `chat_id`, bot removed from the
  group, HTML parse error, 429 flood control) was indistinguishable from
  "no data to send". Now logs status code, chat ID, and Telegram's
  `description` on any non-200. Covers both the trade-alert and summary paths.
- **Scheduler fan-out isolation**: extracted `SummaryScheduler._send_all()`.
  Previously a raising generator aborted the whole `for` loop — a fyers failure
  silently suppressed the penny summary too — and left `_last_sent_date` unset,
  re-running survivors every 60s until midnight. Each generator is now wrapped;
  `_send_all()` never propagates, so the day is always marked done.
- **Summary task GC hazard**: `asyncio.create_task(summary_scheduler.run())`
  kept no reference; the event loop holds only a weak one, so the task could be
  garbage-collected mid-flight, killing the 16:30 summaries for the life of the
  process. Now stored as `self._summary_task`.
- Diagnosis notes: the fyers detector, sheet writes, and summary generation were
  all verified healthy against the live deployment via the MCP
  `get_volume_summary` tool (it calls the same generator instance the 16:30
  scheduler uses). The fault was confined to Telegram delivery on both fyers
  bots. Known latent risk, not yet fixed: no HTML escaping anywhere, so
  `NSE:M&M-EQ`, `NSE:J&KBANK-EQ` and sectors like `Oil & Gas` go raw into
  `parse_mode=HTML`.
- Tests: `fdk_tests/test_telegram.py` (4), `fdk_tests/test_summary_scheduler.py` (4),
  plus `fdk_tests/conftest.py` to put the repo root on `sys.path`.

### 2026-07-23 - Daily 17:00 token wipe + summary trigger hardening
- **Daily fresh login**: at 17:00 IST (after market close) the orchestrator wipes
  `fyers_tokens` (`TokenManager.clear_tokens()`), resets in-memory auth state
  (`FyersAuthenticator.reset()`), and forces service rebuild (`_services_built`),
  so the next 09:13 is always a fresh full TOTP login. Gated once-per-day by the
  pure `should_reset_tokens()` helper (`shared.fdk_constants.DAILY_TOKEN_RESET_TIME`).
- **Summary scheduler**: send trigger changed from exact `== "16:30"` to
  `>= SUMMARY_SEND_TIME` (once-per-day guard unchanged) so a missed exact minute
  (process asleep / event loop blocked) still fires later that evening instead of
  dropping the day's summary.
- `services_built` promoted from a `run()` local to `self._services_built`.
- Tests: `fdk_tests/test_schedular.py` (5) covering the daily-reset time gate.

### 2026-07-23 - Automated headless login (REFERENCE parity)
- New `services/fdk_auth_service/totp_login.py`: headless 5-step Fyers TOTP flow
  (send_login_otp → verify_otp → verify_pin → /api/v3/token → validate-authcode),
  plus PIN-only `refresh_access_token` and bounded, cancellable
  `full_totp_login_with_retry` (5 attempts, 60s apart). `fyers_apiv3` lazy-imported.
- `FyersAuthenticator.authenticate()` is now a ladder: cached token → PIN-only
  refresh → automated TOTP → manual Telegram-link fallback (`_manual_login`).
  Blocking login runs via `asyncio.to_thread`; the 60s waits are sliced so `/hld`
  and `/rst` interrupt mid-login.
- `fyers_tokens` gains a nullable `refresh_token` column (auto-migrated via
  `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`); `load_token()` now returns a
  4-tuple `(token, timestamp, created_at, refresh_token)`.
- New env var `FDK_FYERS_USERNAME` (fy_id); new constants `AUTO_LOGIN_MAX_ATTEMPTS=5`,
  `AUTO_LOGIN_RETRY_DELAY=60`.
- Unit tests: `fdk_tests/test_totp_login.py` (19 tests, loaded via importlib so the
  suite runs without `fyers_apiv3`). No change to detectors, summaries, commands,
  scheduling, MCP, or the webhook path.

### 2026-03-04 - Initial modular architecture
- Merged `fyersdk/tests/fyers.py` (1721 lines), `fyersdk/tests/penny.py` (1552 lines), `fyersdk/tests/health.py` (57 lines) into modular service architecture
- Switched Telegram auth from polling to webhook (`/fdk/webhook/telegram`)
- Replaced local `fyers_access_token.json` with Supabase `fyers_tokens` table
- Moved all hardcoded credentials to environment variables
- Deduplicated detector code into single parameterized `VolumeSpikeDetector`
- Added dedicated `FDK_LOGIN_CHAT_ID` (5th channel) for auth link/TOTP/notifications
- Extracted 890 sector mappings to `config/fdk_sectors.json`, symbols to `config/fdk_symbols.json`

### 2026-03-04 - Reactive token refresh (no polling)
- Removed proactive hourly `AUTH_CHECK_INTERVAL` timer entirely
- Removed `AUTH_CHECK_INTERVAL` constant from `shared/fdk_constants.py`
- Token refresh is now **reactive**: only triggered when Fyers actually rejects a request
- Detector sets `self.token_expired = True` on rejection
- Orchestrator checks `_any_token_expired()` each loop iteration
- On expiry: stops both detectors, re-authenticates, updates tokens, detectors auto-restart
- No unnecessary downtime — detectors run uninterrupted until token actually fails

### 2026-03-04 - Telegram commands /hld and /rst
- Added `/hld [reason]` command: stops all detectors, enters hold mode (for holidays/weekends)
- Added `/rst` command: stops detectors, re-authenticates, restarts everything
- Updated webhook handler (`fdk_auth_service/server.py`) to parse commands via `command_parser.py`
- Webhook handler now receives orchestrator reference for command execution
- Added `hold()` and `request_restart()` methods to `Orchestrator`
- Orchestrator loop respects `on_hold` flag — skips detector start while held
- Added `register_bot_commands()` in `fdk_auth_service/tools.py` — auto-registers commands with Telegram on startup
- Health endpoint shows `"status": "on_hold"` when held
- All command responses sent to `FDK_LOGIN_CHAT_ID`

### 2026-03-04 - 5 separate Telegram bots
- Restructured from 1 shared bot token + 5 chat IDs to **5 separate bot+chat pairs**
- New `TelegramChannel` dataclass in `config_loader.py` holds `bot_token` + `chat_id` per channel
- `TelegramConfig` now has 5 `TelegramChannel` fields: login, fyers_trade, fyers_summary, penny_trade, penny_summary
- Renamed `TelegramBot` to `TelegramSender` — one instance per channel, chat_id baked in, call `send(text)` directly
- Orchestrator creates 5 `TelegramSender` instances and passes relevant ones to authenticator/detectors
- Removed `trade_chat_id` / `summary_chat_id` from `DetectorConfig` — senders passed directly to detector constructor
- `.env` restructured: 10 telegram vars (5 `*_BOT_TOKEN` + 5 `*_CHAT_ID`)
- Webhook registered on all 5 bots; commands registered on login bot

### 2026-03-04 - Summary service
- Merged `fyersdk/tests/fyerssum.py` and `fyersdk/tests/pennysum.py` into `services/fdk_summary_service/`
- `SummaryGenerator`: parameterized class — reads Google Sheet, aggregates trades by symbol, formats top-15 HTML message
- `SummaryScheduler`: async loop, triggers at 16:30 IST daily (once per day guard)
- Day-specific logic: Daily always, +3-day on Wed/Fri, +weekly on Fri
- Started as `asyncio.create_task()` in orchestrator — runs independently of detectors/hold
- Added `SUMMARY_SEND_TIME` constant to `shared/fdk_constants.py`

### 2026-03-04 - /snd and /sdt commands
- `/snd` — immediately sends today's summaries for both fyers and penny (day-appropriate: daily/3-day/weekly)
- `/sdt` — two-step: prompts for date, then generates summaries for that date with day-of-week logic
- Added `parse_date()` to `command_parser.py` — supports DD-MM-YYYY, DD/MM/YYYY, YYYY-MM-DD
- Added `generate_messages_for_date()` and `send_summary_for_date()` to `SummaryGenerator`
- Refactored `_get_records()` and `_format_message()` to accept optional `target_date` parameter
- Webhook uses `_pending_summary_date` flag to track /sdt two-step state
- Orchestrator exposes `self.summary_generators` list (created in `__init__`, not `run()`)
- Added message templates: `summary_generating_message`, `summary_date_prompt_message`, `summary_date_invalid_message`, `summary_date_confirm_message`
- Registered `/snd` and `/sdt` in bot commands via `tools.py`

### 2026-03-04 - Isolated fyers/penny services
- Created `services/fdk_fyers_service/` — `FyersService` (detector lifecycle) + `FyersSummaryService` (summary generation)
- Created `services/fdk_penny_service/` — `PennyService` (detector lifecycle) + `PennySummaryService` (summary generation)
- Each service owns its own detector, controller, sheets manager, and telegram senders — fully isolated, no shared state
- Orchestrator simplified: `self.fyers`, `self.penny`, `self.fyers_summary`, `self.penny_summary` replace raw controllers/generators
- Removed `_build_detector()`, `_fyers_det`, `_penny_det`, `fyers_controller`, `penny_controller`, `summary_generators` from orchestrator
- `/snd` and `/sdt` commands now call `fyers_summary.send_today()` + `penny_summary.send_today()` separately
- Added `GET /fdk/auth/callback` endpoint for local dev authentication (browser redirect with auth_code)
- Fixed all `datetime.now()` calls to use `datetime.now(IST)` — trade alerts, health check, token timestamps
- Suppressed uvicorn "Invalid HTTP request received" warnings from bot scanners

### 2026-03-04 - Symbols & sectors moved to Supabase
- Created `services/fdk_sector_service/symbol_manager.py` — `SymbolManager` class with Supabase CRUD
- Two new tables: `stock_symbols` (symbol + detector + active flag) and `sector_mappings` (symbol → sector)
- Auto-seeds from `config/fdk_symbols.json` and `config/fdk_sectors.json` on first run (empty tables)
- After seed, Supabase is the source of truth — JSON files kept as backup
- Updated `sector_mapper.py` — no longer loads from JSON at import, initialized via `init_sector_mapping()` from Supabase data
- Updated `ochestrator.py` — replaced `_load_symbols_and_sectors()` JSON reads with `SymbolManager` queries
- Symbols can now be added/deactivated via Supabase dashboard without redeploying

### 2026-03-04 - Cleaned invalid symbols + webhooks on all 5 bots
- Removed 193 invalid symbols from `stock_symbols` (182 fyers + 11 penny) and 191 from `sector_mappings` in Supabase
- Active symbols now: 540 fyers + 209 penny (down from 722 + 220)
- Registered Telegram webhook on all 5 bots (login, fyers_trade, fyers_summary, penny_trade, penny_summary) — commands now work from any bot chat
- Bot commands (`/hld`, `/rst`, `/snd`, `/sdt`) still registered only on login bot

### 2026-03-05 - Fix webhook command leakage
- Fixed `_pending_summary_date` flag leaking across commands — any command (`/hld`, `/rst`, `/snd`, or unrecognized `/` commands) now resets the flag
- Added try/except around `request.json()` — malformed requests (bot scanners) return 200 instead of crashing with 500
- Webhook processing order is now: commands → `/sdt` date input → auth code → ignore

### 2026-03-05 - TickDispatcher (shared WebSocket router)
- Added `TickDispatcher` class to `services/fdk_detector_service/websocket_manager.py`
- Fyers `FyersDataSocket` is a singleton — only one WebSocket connection per process
- `TickDispatcher` merges all symbols from both detectors (540 fyers + 209 penny = 749 total), opens one WebSocket
- Builds `_symbol_map` (symbol → list of owning detectors), routes each tick to the correct detector's `on_tick()`
- Orchestrator manages dispatcher lifecycle via `_connect_dispatcher()` / `_close_dispatcher()`
- Dispatcher is created after both services are built, closed on hold/restart/market-close/re-auth
- `VolumeSpikeDetector.start()` now just blocks via `stop_event.wait()` — no longer owns a WebSocket
- `RunController` unchanged — runs detector in a thread, ticks arrive via dispatcher callback
- Each service (`FyersService` / `PennyService`) no longer creates its own WebSocket connection

### 2026-03-06 - Fix auth loop and command handling
- **Auth link spam outside market hours**: Moved `authenticate()` call from before the main loop to inside it — auth now only runs during market hours (after scheduling check)
- **`/hld` and `/rst` ignored during auth**: Added `_cancel_event` to `FyersAuthenticator` with `cancel_auth()` / `reset_cancel()` methods. `hold()` and `request_restart()` now cancel any in-progress auth wait
- **Auth code not received from Telegram**: Re-added `_extract_auth_code()` to `server.py` — webhook handler now extracts `auth_code=` from user-pasted redirect URLs (was removed in 2026-03-05 refactor, breaking the primary auth flow since `FDK_FYERS_REDIRECT_URI` points to `fyersauth.vercel.app`, not the app's `/fdk/auth/callback`)
- Orchestrator tracks `services_built` flag — builds services once after first successful auth, not before the loop
- `_re_authenticate()` now handles auth cancellation gracefully (returns without updating tokens)
- **Invalid auth code on token exchange**: Split `_send_auth_url()` into `_create_session()` (called once) and `_send_auth_msg()` (resend-safe). Previously, every 5-min resend created a new `SessionModel` and overwrote `current_session` — if user authorized with an older URL, the auth code was exchanged on the wrong session → "invalid auth code". Now session + URL are created once per auth attempt, resends only refresh TOTP
