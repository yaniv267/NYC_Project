import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime, timedelta, timezone

# ==============================
# CONFIG
# ==============================
API_ENDPOINT = "https://data.cityofnewyork.us/resource/pvqr-7yc4.json"
APP_TOKEN = "gdLWTLhefvaSPLJI2AV4lTv4m"
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "nyc_parking_violation"

LIMIT = 1000
SLEEP_BETWEEN_CALLS = 1

# ==============================
# KAFKA PRODUCER
# ==============================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5
)

# ==============================
# DATE RANGE
# ==============================
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
# ניתן לשנות את last_n_years כדי להגדיל טווח
last_n_years = 2
start_date = (datetime.now(timezone.utc) - timedelta(days=365*last_n_years)).strftime("%Y-%m-%d")

print(f"📅 מושך נתונים בין {start_date} ל-{today}")

# ==============================
# FETCH LOOP
# ==============================
offset = 0
total_sent = 0

while True:
    params = {
        "$where": f"issue_date between '{start_date}' and '{today}'",
        "$order": "issue_date DESC",
        "$limit": LIMIT,
        "$offset": offset
    }

    headers = {
        "X-App-Token": APP_TOKEN
    }

    try:
        response = requests.get(API_ENDPOINT, params=params, headers=headers, timeout=30)

        if response.status_code == 429:
            print("⚠ Rate limit reached... sleeping 5 seconds")
            time.sleep(5)
            continue

        if response.status_code != 200:
            print("❌ API Error:", response.status_code)
            print(response.text)
            break

        data = response.json()

        if not data:
            print("✅ אין עוד נתונים.")
            break

        print(f"📦 התקבלו {len(data)} רשומות (offset={offset})")

        for record in data:
         
            issue_date = record.get("issue_date", "")[:10]
            if issue_date <= today:
                producer.send(TOPIC_NAME, value=record)
                total_sent += 1

        producer.flush()
        offset += LIMIT
        time.sleep(SLEEP_BETWEEN_CALLS)

    except Exception as e:
        print("❌ שגיאה כללית:", str(e))
        break