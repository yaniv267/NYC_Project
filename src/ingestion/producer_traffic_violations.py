import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime, timedelta, timezone

# --- CONFIGURATION ---
API_ENDPOINT = "https://data.cityofnewyork.us/resource/pvqr-7yc4.json"
APP_TOKEN = "gdLWTLhefvaSPLJI2AV4lTv4m"
KAFKA_BROKER = "course-kafka:9093"
TOPIC_NAME = "nyc_traffic_violations_stream"

LIMIT = 1000
SLEEP_BETWEEN_CALLS = 1

# ==========================================
# 1. FIND LATEST AVAILABLE DATE IN API
# ==========================================
print("🔍 Checking NYC Open Data for the absolute newest records...")
try:
    # קריאה של רשומה אחת בלבד הממוינת מהחדש לישן
    max_date_response = requests.get(
        API_ENDPOINT,
        params={"$limit": 1, "$order": "issue_date DESC", "$select": "issue_date"},
        headers={"X-App-Token": APP_TOKEN}
    ).json()

    if max_date_response and 'issue_date' in max_date_response[0]:
        # חילוץ התאריך (מגיע בפורמט כמו '2026-02-15T00:00:00.000')
        latest_api_date_str = max_date_response[0]['issue_date'][:10]
        latest_api_date = datetime.strptime(latest_api_date_str, "%Y-%m-%d").date()
    else:
        latest_api_date = datetime.now(timezone.utc).date()
except Exception as e:
    print(f"⚠ Could not fetch max date, falling back to today. Error: {e}")
    latest_api_date = datetime.now(timezone.utc).date()

# ==========================================
# 2. DATE RANGE (90 Days backwards from the API's reality)
# ==========================================
start_date = (latest_api_date - timedelta(days=90)).strftime("%Y-%m-%d")
end_date_str = latest_api_date.strftime("%Y-%m-%d")

# --- KAFKA PRODUCER SETUP ---
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks='all',
    retries=5
)


def fetch_and_send_all():
    offset = 0
    total_sent = 0

    print(f"📌 NYC Database max date is: {end_date_str}")
    print(f"🚀 Starting Raw Data Ingestion: {start_date} to {end_date_str}")
    print(f"📡 Sending ALL columns to Topic: {TOPIC_NAME}")

    while True:
        params = {
            "$where": f"issue_date >= '{start_date}' AND issue_date <= '{end_date_str}'",
            "$order": "issue_date DESC",
            "$limit": LIMIT,
            "$offset": offset
        }

        headers = {"X-App-Token": APP_TOKEN}

        try:
            response = requests.get(API_ENDPOINT, params=params, headers=headers, timeout=30)

            if response.status_code == 429:
                print("⚠ Rate limit reached! Sleeping for 15s...")
                time.sleep(15)
                continue

            if response.status_code != 200:
                print(f"❌ API Error {response.status_code}: {response.text}")
                break

            data = response.json()
            if not data:
                print(f"✅ Finished! Total raw records sent: {total_sent}")
                break

            for record in data:
                producer.send(TOPIC_NAME, value=record)
                total_sent += 1

            producer.flush()
            print(f"📦 Sent {len(data)} full records | Total: {total_sent} | Offset: {offset}")

            offset += LIMIT
            time.sleep(SLEEP_BETWEEN_CALLS)

        except Exception as e:
            print(f"❌ Connection error: {str(e)}. Retrying in 30 seconds...")
            time.sleep(30)
            continue


if __name__ == "__main__":
    fetch_and_send_all()