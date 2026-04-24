import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime, timedelta, timezone

# ==========================================
# 1. CONFIGURATION & KAFKA SETUP
# ==========================================
BASE_URL = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json"
APP_TOKEN = "gdLWTLhefvaSPLJI2AV4lTv4m"
KAFKA_BROKER = "course-kafka:9093"
TOPIC_NAME = "nyc_traffic_stream"
DAYS_BACK = 7

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks='all'
)

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def send_to_kafka(data, label):
    """Sends a batch of records to Kafka and logs the latest timestamp."""
    if not data:
        return

    # Find the latest data point in the current batch
    latest_in_batch = max([r.get('data_as_of', '0000') for r in data])

    for record in data:
        # Compatibility fix for Python 3.10 (timezone.utc instead of UTC)
        record['ingested_at'] = datetime.now(timezone.utc).isoformat()
        link_id = str(record.get('link_id', 'unknown'))
        producer.send(TOPIC_NAME, key=link_id.encode('utf-8'), value=record)

    producer.flush()
    print(f"✅ {label}: Sent {len(data)} records. (Latest record time: {latest_in_batch})")

# ==========================================
# 3. PRODUCTION EXECUTION LOGIC
# =========================
def run_smart_producer():
    headers = {"X-App-Token": APP_TOKEN}

    # --- STEP 1: IMMEDIATE REAL-TIME SNAPSHOT ---
    print(f"🚀 Step 1: Fetching IMMEDIATE current status...")
    try:
        params = {"$limit": 5000, "$order": "data_as_of DESC"}
        res = requests.get(BASE_URL, headers=headers, params=params, timeout=30)
        send_to_kafka(res.json(), "IMMEDIATE SNAPSHOT")
    except Exception as e:
        print(f"❌ Failed to get immediate data: {e}")

    # --- STEP 2: HISTORICAL BACKFILL (LAST 7 DAYS) ---
    print(f"📦 Step 2: Starting historical backfill (Last {DAYS_BACK} days)...")
    limit = 10000
    offset = 0
    start_date = (datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT%H:%M:%S")
    where_clause = f"data_as_of > '{start_date}'"

    while True:
        try:
            params = {
                "$limit": limit,
                "$offset": offset,
                "$where": where_clause,
                "$order": "data_as_of DESC"
            }
            response = requests.get(BASE_URL, headers=headers, params=params, timeout=60)
            batch_data = response.json()

            if not batch_data or len(batch_data) == 0:
                print("🏁 Finished historical backfill.")
                break

            send_to_kafka(batch_data, f"HISTORY BATCH {offset}-{offset + len(batch_data)}")
            offset += limit
            time.sleep(1.5)

        except Exception as e:
            print(f"❌ Error in history fetch: {e}")
            break

    # --- STEP 3: CONTINUOUS STREAMING MODE ---
    print(f"🔄 Step 3: Entering continuous update mode (Every 5 mins)...")
    #while True:
    try:
            params = {"$limit": 10000, "$order": "data_as_of DESC"}
            res = requests.get(BASE_URL, headers=headers, params=params, timeout=30)
            send_to_kafka(res.json(), "REAL-TIME UPDATE")

            print(f"end successfuly...")
            #print(f"😴 Sleeping for 5 minutes...")
            #time.sleep(300)
    except Exception as e:
            print(f"❌ Real-time error: {e}")
            time.sleep(10)

# =========================
# 4. ENTRY POINT
# =========================
if __name__ == "__main__":
    run_smart_producer()