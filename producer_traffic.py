import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime, timedelta, UTC

# ==========================================
# CONFIGURATION
# ==========================================
BASE_URL = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json"
APP_TOKEN = "gdLWTLhefvaSPLJI2AV4lTv4m"
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "nyc_traffic_stream"
DAYS_BACK = 7

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks='all'
)


def send_to_kafka(data, label):
    """פונקציית עזר לשליחה לקפקא עם הדפסת השעה הכי חדשה במנה"""
    if not data:
        return

    # מציאת השעה הכי מאוחרת (הכי חדשה) בתוך המנה שקיבלנו מה-API
    latest_in_batch = max([r.get('data_as_of', '0000') for r in data])

    for record in data:
        record['ingested_at'] = datetime.now(UTC).isoformat()
        link_id = str(record.get('link_id', 'unknown'))
        producer.send(TOPIC_NAME, key=link_id.encode('utf-8'), value=record)

    producer.flush()
    print(f"✅ {label}: Sent {len(data)} records. (Latest record time: {latest_in_batch})")


def run_smart_producer():
    headers = {"X-App-Token": APP_TOKEN}

    # --- שלב 1: Real-time מיידי (הכי חדש שיש עכשיו) ---
    print(f"🚀 Step 1: Fetching IMMEDIATE current status...")
    try:
        # מושכים את ה-5000 האחרונים כדי לכסות את כל העיר ברגע זה
        params = {"$limit": 5000, "$order": "data_as_of DESC"}
        res = requests.get(BASE_URL, headers=headers, params=params, timeout=30)
        send_to_kafka(res.json(), "IMMEDIATE SNAPSHOT")
    except Exception as e:
        print(f"❌ Failed to get immediate data: {e}")

    # --- שלב 2: Historical Mass (השלמת פערים לאחור) ---
    print(f"📦 Step 2: Starting historical backfill (Last {DAYS_BACK} days)...")
    limit = 10000
    offset = 0
    start_date = (datetime.now() - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT%H:%M:%S")
    where_clause = f"data_as_of > '{start_date}'"

    while True:
        try:
            params = {
                "$limit": limit,
                "$offset": offset,
                "$where": where_clause,
                "$order": "data_as_of DESC"  # ממשיך מהחדש לישן
            }
            response = requests.get(BASE_URL, headers=headers, params=params, timeout=60)
            batch_data = response.json()

            if not batch_data or len(batch_data) == 0:
                print("🏁 Finished historical backfill.")
                break

            send_to_kafka(batch_data, f"HISTORY BATCH {offset}-{offset + len(batch_data)}")
            offset += limit
            time.sleep(1.5)  # נחמדות ל-API

        except Exception as e:
            print(f"❌ Error in history fetch: {e}")
            break

    # --- שלב 3: לולאת Real-time קבועה ---
    print(f"🔄 Step 3: Entering continuous update mode (Every 5 mins)...")
    while True:
        try:
            params = {"$limit": 2000, "$order": "data_as_of DESC"}
            res = requests.get(BASE_URL, headers=headers, params=params, timeout=30)
            send_to_kafka(res.json(), "REAL-TIME UPDATE")

            print(f"😴 Sleeping for 5 minutes...")
            time.sleep(300)
        except Exception as e:
            print(f"❌ Real-time error: {e}")
            time.sleep(10)


if __name__ == "__main__":
    run_smart_producer()