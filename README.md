# fraud-dashboard

A real-time fraud detection pipeline. A Kafka producer generates simulated
transactions, a FastAPI consumer scores each one with an IsolationForest
model as it arrives, results are stored in SQLite, and flagged transactions
show up live in a browser dashboard where you can approve or reject them.

## Setup

Requires Docker (for Kafka) and Python 3.9+.

```bash
docker compose up -d        # starts Kafka + Zookeeper
pip install -r requirements.txt
```

Then, in two separate terminals:

```bash
python producer.py          # generates and publishes transactions
```

```bash
uvicorn main:app --reload --port 8000   # consumes, scores, serves the dashboard
```

Open `http://localhost:8000`. New transactions appear every 1-2 seconds;
flagged ones are highlighted and have approve/reject buttons.

To stop everything: `Ctrl+C` in both terminals, then `docker compose down`.

## How it works

- `producer.py` generates fake transactions (amount, merchant, location,
  time) and publishes each one as a JSON message to the Kafka topic
  `transactions`.
- `main.py` runs a Kafka consumer in a background thread. As each message
  arrives, it's scored by an IsolationForest trained on synthetic "normal"
  spending patterns, saved to `transactions.db`, and broadcast to any open
  browser tab over a WebSocket.
- `static/index.html` is a plain JS dashboard — no build step, no framework.
  It loads recent history on load, then listens on the WebSocket for new
  transactions and analyst decisions.
- Approve/reject decisions are written back to SQLite. In a real system
  these labels would feed a retraining job.

IsolationForest was chosen because it doesn't need labeled fraud examples —
it just isolates points that are unusually easy to separate from the rest
of the data, which fits fraud detection where labeled fraud is rare.

Kafka decouples the producer from the consumer: multiple services could
publish transactions, and multiple consumers (fraud scoring, analytics,
audit logging) could read the same stream independently. Here it's one
producer and one consumer, but the pipeline is structured the same way a
larger system would be.

## Notes on scope

The stream, model, database, and dashboard are all real and running — this
isn't a mockup. What's simplified compared to a production system:

- SQLite instead of Postgres.
- Single model trained once at startup instead of a scheduled retraining
  pipeline.
- One producer and one consumer instead of many.

## Files

```
docker-compose.yml    Kafka + Zookeeper
producer.py           generates and publishes transactions to Kafka
main.py               consumer: model, scoring, WebSocket, API
static/index.html     dashboard UI
requirements.txt
```
