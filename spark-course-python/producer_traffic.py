# import requests
# import json
# import time
# from kafka import KafkaProducer
# from datetime import datetime

# # ==========================================
# # CONFIGURATION
# # ==========================================
# # Querying for the most recent speed data across all links
# # Using $order to get the latest timestamps first
# API_ENDPOINT = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json?$limit=2000&$order=data_as_of DESC"
# APP_TOKEN = "gdLWTLhefvaSPLJI2AV4lTv4m"
# KAFKA_BROKER = "localhost:9092"
# TOPIC_NAME = "nyc_traffic_"

# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BROKER,
#     value_serializer=lambda v: json.dumps(v).encode("utf-8")
# )

# def fetch_latest_road_status():
#     """
#     Fetches the most recent reported speed for every road segment (Link).
#     """
#     headers = {"X-App-Token": APP_TOKEN}
#     try:
#         print(f"📡 [{datetime.now().strftime('%H:%M:%S')}] Fetching latest status for all New York roads...")
#         response = requests.get(API_ENDPOINT, headers=headers, timeout=45)
#         response.raise_for_status()
#         data = response.json()

#         if not data:
#             print("⚪ No data received from the sensors.")
#             return

#         # We use a dictionary to keep only the absolute latest record for each unique Link ID
#         latest_updates = {}
#         for record in data:
#             link_id = record.get('link_id')
#             if link_id not in latest_updates:
#                 latest_updates[link_id] = record

#         # Send the unique latest records to Kafka
#         for link_id, record in latest_updates.items():
#             producer.send(TOPIC_NAME, value=record)

#         producer.flush()
#         print(f"✅ Success! Sent latest status for {len(latest_updates)} unique road segments.")

#     except Exception as e:
#         print(f"❌ Error: {e}")

# if __name__ == "__main__":
#     while True:
#         fetch_latest_road_status()
#         time.sleep(60) # Poll every minute for updates

# import requests
# import json
# import time
# from kafka import KafkaProducer
# from datetime import datetime, UTC  # הוספנו את UTC לכאן

# # ==========================================
# # CONFIGURATION - BRONZE LAYER
# # ==========================================
# API_ENDPOINT = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json?$limit=2000&$order=data_as_of DESC"
# APP_TOKEN = "gdLWTLhefvaSPLJI2AV4lTv4m"
# KAFKA_BROKER = "localhost:9092"
# TOPIC_NAME = "nyc_traffic_bronze"

# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BROKER,
#     value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#     acks='all'
# )


# def run_bronze_producer():
#     headers = {"X-App-Token": APP_TOKEN}
#     print(f"🚀 NYC Traffic Bronze Producer is running...")

#     while True:
#         try:
#             start_time = datetime.now()
#             print(f"📡 [{start_time.strftime('%H:%M:%S')}] Fetching raw data from NYC API...")

#             response = requests.get(API_ENDPOINT, headers=headers, timeout=30)
#             response.raise_for_status()
#             raw_data = response.json()

#             if raw_data:
#                 for record in raw_data:
#                     # תיקון השגיאה (DeprecationWarning)
#                     record['ingested_at'] = datetime.now(UTC).isoformat()

#                     link_id = str(record.get('link_id', 'unknown'))
#                     producer.send(TOPIC_NAME, key=link_id.encode('utf-8'), value=record)

#                 producer.flush()
#                 print(f"✅ Success! Sent {len(raw_data)} raw records to {TOPIC_NAME}.")

#             # הדפסת חיווי כדי שלא תחשוב שזה תקוע
#             print(f"😴 Finished cycle. Sleeping for 60 seconds...")
#             time.sleep(60)

#         except Exception as e:
#             print(f"❌ Error: {e}")
#             print("🔄 Retrying in 10 seconds...")
#             time.sleep(10)


# if __name__ == "__main__":
#     run_bronze_producer()

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
TOPIC_NAME = "nyc_traffic_bronze"

# הגדרה לשבוע אחד (7 ימים) - ה-Sweet Spot לפרויקט
DAYS_BACK = 7

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks='all'
)


def run_smart_producer():
    headers = {"X-App-Token": APP_TOKEN}

    # --- שלב 1: משיכת היסטוריה בשיטת ה-Paging (מנות קטנות) ---
    print(f"🚀 Starting Smart Ingestion: History for the last {DAYS_BACK} days...")

    limit = 10000  # גודל כל "מנה"
    offset = 0
    start_date = (datetime.now() - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT%H:%M:%S")
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
            response.raise_for_status()
            batch_data = response.json()

            if not batch_data:
                print("🏁 Finished fetching all historical data.")
                break

            print(f"📦 Processing batch: Rows {offset} to {offset + len(batch_data)}...")

            for record in batch_data:
                record['ingested_at'] = datetime.now(UTC).isoformat()
                link_id = str(record.get('link_id', 'unknown'))
                producer.send(TOPIC_NAME, key=link_id.encode('utf-8'), value=record)

            producer.flush()
            offset += limit

            # הפסקה קצרה כדי לא לחנוק את ה-API
            print("😴 Short break to be nice to the API...")
            time.sleep(1.5)

        except Exception as e:
            print(f"❌ Error during history fetch: {e}")
            time.sleep(10)
            continue

    # --- שלב 2: מעבר למצב Real-time (עדכונים שוטפים) ---
    print(f"📡 Transitioning to Real-time Mode. Updating every 5 minutes...")

    while True:
        try:
            # כאן אנחנו מבקשים רק את ה-2000 הכי חדשים שיש כרגע
            params = {"$limit": 2000, "$order": "data_as_of DESC"}
            response = requests.get(BASE_URL, headers=headers, params=params, timeout=30)
            current_data = response.json()

            if current_data:
                for record in current_data:
                    record['ingested_at'] = datetime.now(UTC).isoformat()
                    link_id = str(record.get('link_id', 'unknown'))
                    producer.send(TOPIC_NAME, key=link_id.encode('utf-8'), value=record)

                producer.flush()
                print(f"✅ Real-time update success: Sent {len(current_data)} records.")

            print(f"😴 Sleeping for 5 minutes until next update...")
            time.sleep(300)  # 5 דקות הן זמן אידיאלי למניעת כפילויות מיותרות

        except Exception as e:
            print(f"❌ Real-time error: {e}")
            time.sleep(10)


if __name__ == "__main__":
    run_smart_producer()