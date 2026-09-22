"""
Generates fake transactions and publishes them to the Kafka topic
'transactions'. Run this alongside main.py once Kafka is up.

    python producer.py
"""

import json
import random
import time

from kafka import KafkaProducer

MERCHANTS = ["Amazon", "Steam", "Uber", "Walmart", "Shell Gas", "Netflix", "BestBuy", "Delta Airlines"]
LOCATIONS = ["Toronto,CA", "Hamilton,CA", "New York,US", "Lagos,NG", "London,UK", "Tokyo,JP"]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def make_transaction():
    amount = round(random.gammavariate(2.0, 40), 2)
    hour = time.localtime().tm_hour + random.uniform(-0.5, 0.5)
    foreign = 1 if random.random() < 0.12 else 0
    merchant_risk = random.uniform(0, 0.3)

    # occasionally inject an obvious outlier so flags show up in the demo
    if random.random() < 0.15:
        amount = round(random.uniform(800, 5000), 2)
        foreign = 1
        merchant_risk = random.uniform(0.6, 0.95)
        hour = random.choice([2, 3, 4])

    return {
        "ts": time.time(),
        "amount": amount,
        "merchant": random.choice(MERCHANTS),
        "location": random.choice(LOCATIONS),
        "hour": round(hour, 1),
        "foreign": foreign,
        "merchant_risk": round(merchant_risk, 2),
    }


if __name__ == "__main__":
    print("Publishing transactions to Kafka topic 'transactions'... Ctrl+C to stop.")
    while True:
        tx = make_transaction()
        producer.send("transactions", tx)
        print(f"sent: ${tx['amount']} at {tx['merchant']}")
        time.sleep(random.uniform(0.8, 2.2))
