# Luna_Comms Portfolio Demo

This project is designed to be demoed manually rather than running continuously.

## What To Showcase

- Gmail report delivery job generation from finalized export + push confirmation
- Google Calendar outbound sync job generation and send path
- End-to-end traceability from source event to receipt and transition ledger

## One Command Demo (Safe)

Run a full dry-run cycle that creates realistic receipts without sending provider API calls:

```bash
python3 scripts/luna_comms_seed_demo.py --json
python3 scripts/luna_comms_prepare_jobs.py --json
python3 scripts/luna_comms_queue_jobs.py --json
python3 scripts/luna_comms_send_jobs.py --dry-run --limit 50 --json
```

## Live Gmail Demo

```bash
python3 scripts/luna_comms_seed_demo.py --json
python3 scripts/luna_comms_prepare_jobs.py --json
python3 scripts/luna_comms_queue_jobs.py --json
python3 scripts/luna_comms_send_jobs.py --limit 50 --json
```

Requirements:

- `GOOGLE_OAUTH_TOKEN_PATH` with `gmail.send` scope
- `LUNA_DAILY_REPORT_EMAIL_TO`

## Live Calendar Demo

```bash
python3 scripts/luna_comms_seed_demo.py --json
python3 scripts/luna_comms_prepare_jobs.py --json
python3 scripts/luna_comms_queue_jobs.py --json
python3 scripts/luna_comms_send_jobs.py --enable-calendar-live --limit 50 --json
```

Requirements:

- `GOOGLE_OAUTH_TOKEN_PATH` with calendar events scope

## Files To Show In Demo

- Source events: `exports/staged/luna_comms/inbox/*.json`
- Prepared jobs: `exports/staged/luna_comms/jobs/prepared/*.json`
- Receipts: `exports/staged/luna_comms/receipts/*/*.json`
- Transitions: `exports/staged/luna_comms/ledger/job_transitions.jsonl`
- Attempts: `exports/staged/luna_comms/ledger/send_attempts.jsonl`
- Dedupe index: `exports/staged/luna_comms/ledger/dedupe_index.jsonl`
- Device feed: `exports/staged/luna_comms/feeds/device_updates/latest.json`
