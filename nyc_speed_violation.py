import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime, timedelta

# ==========================================
# CONFIGURATION
# ==========================================
APP_TOKEN = "gdLWTLhefvaSPLJI2AV4lTv4m"
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "nyc_speed_cameras"
VIOLATION_CODES = "('36', '7', '5', '12')"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# התחלה מ-30 יום אחורה
last_checkpoint = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%dT00:00:00')


def fetch_all_missing_data():
    global last_checkpoint
    headers = {"X-App-Token": APP_TOKEN}

    while True:
        # משתמשים ב-Limit המקסימלי כדי לסיים מהר
        query_url = (
            f"https://data.cityofnewyork.us/resource/pvqr-7yc4.json"
            f"?$where=violation_code IN {VIOLATION_CODES} AND issue_date >= '{last_checkpoint}'"
            f"&$limit=50000"
            f"&$order=issue_date ASC"
        )

        try:
            print(f"📡 Sending request for data after {last_checkpoint}...")
            response = requests.get(query_url, headers=headers, timeout=60)
            response.raise_for_status()
            data = response.json()

            if not data or len(data) <= 1:  # אם קיבלנו רק את הרשומה האחרונה שוב
                print("🏁 All caught up! No more historical data to fetch.")
                break

            print(f"📥 Received {len(data)} records. Streaming to Kafka...")

            for record in data:
                producer.send(TOPIC_NAME, value=record)
                # מעדכנים את ה-Checkpoint תוך כדי תנועה
                current_date = record.get('issue_date')
                if current_date and current_date > last_checkpoint:
                    last_checkpoint = current_date

            producer.flush()
            print(f"✅ Batch completed. New checkpoint: {last_checkpoint}")

            # אם קיבלנו פחות מ-50,000, סימן שסיימנו את כל מה שיש בשרת כרגע
            if len(data) < 50000:
                print("✅ Finished streaming all available records.")
                break

        except Exception as e:
            print(f"❌ Error during streaming: {e}")
            break


if __name__ == "__main__":
    print(f"🚀 Starting Fast Stream from {last_checkpoint}...")

    # שלב 1: השלמת כל הפער מהחודש האחרון ברצף
    fetch_all_missing_data()

    # שלב 2: כניסה למצב המתנה לעדכונים שוטפים (כל שעה במקום כל 10 דקות)
    while True:
        print(f"⏳ Waiting for new updates... (Check in 1 hour)")
        time.sleep(3600)
        fetch_all_missing_data()