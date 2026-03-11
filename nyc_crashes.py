import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime, timedelta ,UTC

# ==============================
# CONFIG
# ==============================
# Filter for records from the last 24 hours only
last_week_date = (datetime.now() - timedelta(days=4)).strftime('%Y-%m-%d')

API_ENDPOINT = (
    f"https://data.cityofnewyork.us/resource/h9gi-nx95.json"
    f"?$where=crash_date >= '{last_week_date}' AND latitude IS NOT NULL"
    f"&$order=crash_date DESC,crash_time DESC"
    f"&$limit=5000"
)

APP_TOKEN = "gdLWTLhefvaSPLJI2AV4lTv4m"
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "nyc_crashes"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def fetch_weekly_crashes():
    headers = {"X-App-Token": APP_TOKEN}
    try:
        print(f"📡 Fetching all crashes since: {last_week_date}...")
        response = requests.get(API_ENDPOINT, headers=headers, timeout=30)
        response.raise_for_status()

        data = response.json()

        if data:
            print(f"✅ Found {len(data)} records from the last 7 days.")
            for record in data:
                # Adding an ingestion timestamp for Elasticsearch
                record['ingested_at'] = datetime.now(UTC).isoformat()
                producer.send(TOPIC_NAME, value=record)

            producer.flush()
            print(f"🎯 Successfully pushed weekly data to Kafka topic: {TOPIC_NAME}")
        else:
            print(f"⚪ No crashes found for the period starting {last_week_date}")

    except Exception as e:
        print(f"❌ Error fetching weekly data: {e}")
    finally:
        producer.close()


if __name__ == "__main__":
    fetch_weekly_crashes()