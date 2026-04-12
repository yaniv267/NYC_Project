import requests, json, time
from kafka import KafkaProducer
from datetime import datetime, timedelta, UTC

# ==========================================
# CONFIGURATION - NYC CRASHES MAINTENANCE
# ==========================================
APP_TOKEN = "gdLWTLhefvaSPLJI2AV4lTv4m"
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "nyc_crashes_stream"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks='all'
)

print(f"🚀 Crash Maintenance Producer is LIVE. Monitoring NYC for new accidents...")

while True:
    try:
        # 1. לקיחת חלון זמן של יומיים אחורה כדי למנוע חורים במידע
        start_date = (datetime.now(UTC) - timedelta(days=14)).strftime('%Y-%m-%d')

        # 2. שאילתה ל-API (מביאה את ה-2000 הכי חדשים מהיומיים האחרונים)
        url = (
            f"https://data.cityofnewyork.us/resource/h9gi-nx95.json"
            f"?$where=crash_date >= '{start_date}'"
            f"&$limit=2000&$order=crash_date DESC, crash_time DESC"
        )

        res = requests.get(url, headers={"X-App-Token": APP_TOKEN}, timeout=30)
        res.raise_for_status()
        data = res.json()

        if data and isinstance(data, list):
            for r in data:
                # Metadata לשכבת ה-Bronze
                r['ingested_at'] = datetime.now(UTC).isoformat()

                # שימוש ב-collision_id כמפתח (מבטיח שספארק ינקה כפילויות בקלות)
                crash_id = str(r.get('collision_id', 'unknown'))
                producer.send(TOPIC_NAME, key=crash_id.encode('utf-8'), value=r)

            producer.flush()
            print(f"✅ [{datetime.now().strftime('%H:%M:%S')}] Sync complete. Sent {len(data)} records.")
        else:
            print(f"⚪ [{datetime.now().strftime('%H:%M:%S')}] No new data found in the last 48h.")

    except Exception as e:
        print(f"❌ Error during sync: {e}")

    # 3. הולך לישון לשעה. תאונות ב-API לא מתעדכנות כל דקה.
    print("😴 Sleeping for 1 hour...")
    time.sleep(3600)