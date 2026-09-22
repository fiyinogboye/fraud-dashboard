"""
FastAPI backend for the fraud detection dashboard.

Generates a simulated transaction stream, scores each transaction with an
IsolationForest model, stores results in SQLite, and pushes updates to
connected clients over a WebSocket.
"""

import asyncio
import json
import random
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import numpy as np
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sklearn.ensemble import IsolationForest

DB_PATH = Path(__file__).parent / "transactions.db"
MERCHANTS = ["Amazon", "Steam", "Uber", "Walmart", "Shell Gas", "Netflix", "BestBuy", "Delta Airlines"]
LOCATIONS = ["Toronto,CA", "Hamilton,CA", "New York,US", "Lagos,NG", "London,UK", "Tokyo,JP"]

# ---------------------------------------------------------------------------
# 1. Train the anomaly model on synthetic "normal" historical data.
#    Features: amount, hour_of_day, is_foreign_location, merchant_risk_score
# ---------------------------------------------------------------------------

def make_training_data(n=2000):
    rng = np.random.default_rng(42)
    amount = rng.gamma(shape=2.0, scale=40, size=n)          # most purchases small-ish
    hour = rng.normal(loc=14, scale=4, size=n) % 24          # clustered around daytime
    foreign = rng.binomial(1, 0.05, size=n)                  # rarely foreign
    merchant_risk = rng.uniform(0, 0.3, size=n)              # normally low-risk merchants
    return np.column_stack([amount, hour, foreign, merchant_risk])


model = IsolationForest(n_estimators=150, contamination=0.08, random_state=42)
model.fit(make_training_data())


def score_transaction(amount, hour, foreign, merchant_risk):
    """Return (is_flagged, confidence 0-1). Lower isolation-forest score = more anomalous."""
    x = np.array([[amount, hour, foreign, merchant_risk]])
    raw_score = model.decision_function(x)[0]     # roughly -0.3 (anomalous) .. +0.3 (normal)
    is_flagged = model.predict(x)[0] == -1
    confidence = float(np.clip(0.5 - raw_score, 0, 1))  # map to 0..1, higher = more suspicious
    return is_flagged, round(confidence, 3)


# ---------------------------------------------------------------------------
# 2. Storage
# ---------------------------------------------------------------------------

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id TEXT PRIMARY KEY,
            ts REAL,
            amount REAL,
            merchant TEXT,
            location TEXT,
            hour REAL,
            foreign_flag INTEGER,
            merchant_risk REAL,
            is_flagged INTEGER,
            confidence REAL,
            analyst_decision TEXT DEFAULT 'pending'
        )
    """)
    conn.commit()
    conn.close()


def save_transaction(tx):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (tx["id"], tx["ts"], tx["amount"], tx["merchant"], tx["location"],
         tx["hour"], tx["foreign"], tx["merchant_risk"], int(tx["is_flagged"]),
         tx["confidence"], "pending"),
    )
    conn.commit()
    conn.close()


def update_decision(tx_id, decision):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE transactions SET analyst_decision=? WHERE id=?", (decision, tx_id))
    conn.commit()
    conn.close()


def recent_transactions(limit=100):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM transactions ORDER BY ts DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# 3. Connection manager for WebSocket broadcast
# ---------------------------------------------------------------------------

class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, message: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()


# ---------------------------------------------------------------------------
# 4. The "stream": generates a transaction every ~1-2s and scores it
# ---------------------------------------------------------------------------

async def transaction_generator():
    while True:
        await asyncio.sleep(random.uniform(0.8, 2.2))

        amount = round(random.gammavariate(2.0, 40), 2)
        hour = time.localtime().tm_hour + random.uniform(-0.5, 0.5)
        foreign = 1 if random.random() < 0.12 else 0
        merchant_risk = random.uniform(0, 0.3)

        # occasionally inject an obvious outlier so the demo has visible fraud
        if random.random() < 0.15:
            amount = round(random.uniform(800, 5000), 2)
            foreign = 1
            merchant_risk = random.uniform(0.6, 0.95)
            hour = random.choice([2, 3, 4])  # odd hours

        is_flagged, confidence = score_transaction(amount, hour, foreign, merchant_risk)

        tx = {
            "id": str(uuid.uuid4())[:8],
            "ts": time.time(),
            "amount": amount,
            "merchant": random.choice(MERCHANTS),
            "location": random.choice(LOCATIONS),
            "hour": round(hour, 1),
            "foreign": foreign,
            "merchant_risk": round(merchant_risk, 2),
            "is_flagged": bool(is_flagged),
            "confidence": confidence,
        }

        save_transaction(tx)
        await manager.broadcast({"type": "transaction", "data": tx})


# ---------------------------------------------------------------------------
# 5. FastAPI app
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    task = asyncio.create_task(transaction_generator())
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@app.get("/api/history")
async def history():
    return recent_transactions()


@app.post("/api/decision/{tx_id}/{decision}")
async def decide(tx_id: str, decision: str):
    update_decision(tx_id, decision)
    await manager.broadcast({"type": "decision", "id": tx_id, "decision": decision})
    return {"ok": True}


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()  # keep-alive; client doesn't need to send real data
    except WebSocketDisconnect:
        manager.disconnect(websocket)


app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")
