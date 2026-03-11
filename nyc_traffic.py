import requests
import json
import time
from kafka import KafkaProducer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==============================
# CONFIG
# ==============================
# ה-Dataset של מהירות תנועה בזמן אמת
# כתובות מתעדכנות שמבקשות גם את העמודה של הקורדינטות למפה
API_ENDPOINT = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json?$select=link_name,speed,travel_time,data_as_of,link_id,link_points,borough"
# כתובת מעודכנת שמבקשת רק 5 עמודות ספציפיות כדי למנוע Timeout
# API_ENDPOINT = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json?$select=link_name,speed,travel_time,data_as_of,link_id"
APP_TOKEN = "gdLWTLhefvaSPLJI2AV4lTv4m"
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "nyc_traffic"



FETCH_INTERVAL = 60  # Time to wait between API polls
HTTP_TIMEOUT = 45  # Time to wait for NYC server before giving up

# ==========================================
# KAFKA PRODUCER SETUP
# ==========================================
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,  # Kafka will try to resend if the broker is busy
        acks='all'  # Ensures the data is safely stored in Kafka
    )
    print(f"✅ Successfully connected to Kafka: {KAFKA_BROKER}")
except Exception as e:
    print(f"❌ Kafka Connection Error: {e}")
    exit(1)


# ==========================================
# RESILIENT SESSION (THE PROTECTION LAYER)
# ==========================================
def create_robust_session():
    """
    Creates a session that automatically handles temporary network issues.
    This prevents the script from crashing during API lag.
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=5,  # Try 5 times for each request
        backoff_factor=2,  # Wait longer between tries (2s, 4s, 8s...)
        status_forcelist=[429, 500, 502, 503, 504],  # Retry on these specific errors
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.headers.update({"X-App-Token": APP_TOKEN})
    return session


# ==========================================
# MAIN EXECUTION LOOP
# ==========================================
def run_traffic_producer():
    session = create_robust_session()
    total_records_pushed = 0

    print(f"🚀 NYC Traffic Producer is running for Telegram Bot...")

    while True:
        cycle_start = time.time()
        try:
            print(f"\n📡 [{time.strftime('%H:%M:%S')}] Polling NYC Open Data API...")

            # Fetch the data
            response = session.get(API_ENDPOINT, timeout=HTTP_TIMEOUT)
            response.raise_for_status()

            data = response.json()

            if data:
                # Send each road segment to Kafka as an individual message
                for record in data:
                    producer.send(TOPIC_NAME, value=record)

                # Ensure all messages leave the buffer and reach Kafka
                producer.flush()

                total_records_pushed += len(data)
                print(f"✅ Success! Sent {len(data)} road records. (Total: {total_records_pushed})")
            else:
                print("⚪ Warning: API returned 0 records.")

        except Exception as e:
            # Catch-all to ensure the script NEVER stops
            print(f"❌ Error encountered: {e}")
            print("🔄 Refreshing session and retrying in 30 seconds...")
            session = create_robust_session()
            time.sleep(30)
            continue

        # Adjust sleep to maintain a consistent 60-second heartbeat
        execution_time = time.time() - cycle_start
        sleep_duration = max(1, FETCH_INTERVAL - execution_time)
        print(f"😴 Cycle finished in {int(execution_time)}s. Sleeping for {int(sleep_duration)}s...")
        time.sleep(sleep_duration)


if __name__ == "__main__":
    run_traffic_producer()