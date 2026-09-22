# fraud-dashboard

A small real-time fraud detection demo. A simulated transaction stream is scored
by an IsolationForest model, results are stored in SQLite, and flagged
transactions show up live in a browser dashboard where you can approve or
reject them.

## Setup

```bash
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

Open `http://localhost:8000`. New transactions appear every 1-2 seconds;
flagged ones are highlighted and have approve/reject buttons.

## How it works

- `main.py` generates fake transactions (amount, merchant, location, time),
  scores each one with an IsolationForest trained on synthetic "normal"
  spending patterns, saves it to `transactions.db`, and broadcasts it over a
  WebSocket.
- `static/index.html` is a plain JS dashboard — no build step, no framework.
  It loads recent history on load, then listens on the WebSocket for new
  transactions and analyst decisions.
- Approve/reject decisions are written back to SQLite. In a real system these
  labels would feed a retraining job.

IsolationForest was chosen because it doesn't need labeled fraud examples —
it just isolates points that are unusually easy to separate from the rest of
the data, which fits fraud detection where labeled fraud is rare.

## Notes on scope

The transaction stream, database, and model are all real and running — this
isn't a mockup. What's simplified compared to a production system:

- Transactions are generated in-process instead of coming from Kafka.
- SQLite instead of Postgres.
- Single model trained once at startup instead of a scheduled retraining
  pipeline.

Swapping any of these in would be a reasonable next step (`kafka-python` +
a local broker, a Postgres container, a cron job that refits the model on
accumulated decisions).

## Files

```
main.py              backend: model, transaction generator, WebSocket, API
static/index.html    dashboard UI
requirements.txt
```
