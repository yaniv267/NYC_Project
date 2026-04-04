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

import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime, UTC  # הוספנו את UTC לכאן

# ==========================================
# CONFIGURATION - BRONZE LAYER
# ==========================================
API_ENDPOINT = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json?$limit=2000&$order=data_as_of DESC"
APP_TOKEN = "gdLWTLhefvaSPLJI2AV4lTv4m"
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "nyc_traffic_bronze"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks='all'
)


def run_bronze_producer():
    headers = {"X-App-Token": APP_TOKEN}
    print(f"🚀 NYC Traffic Bronze Producer is running...")

    while True:
        try:
            start_time = datetime.now()
            print(f"📡 [{start_time.strftime('%H:%M:%S')}] Fetching raw data from NYC API...")

            response = requests.get(API_ENDPOINT, headers=headers, timeout=30)
            response.raise_for_status()
            raw_data = response.json()

            if raw_data:
                for record in raw_data:
                    # תיקון השגיאה (DeprecationWarning)
                    record['ingested_at'] = datetime.now(UTC).isoformat()

                    link_id = str(record.get('link_id', 'unknown'))
                    producer.send(TOPIC_NAME, key=link_id.encode('utf-8'), value=record)

                producer.flush()
                print(f"✅ Success! Sent {len(raw_data)} raw records to {TOPIC_NAME}.")

            # הדפסת חיווי כדי שלא תחשוב שזה תקוע
            print(f"😴 Finished cycle. Sleeping for 60 seconds...")
            time.sleep(60)

        except Exception as e:
            print(f"❌ Error: {e}")
            print("🔄 Retrying in 10 seconds...")
            time.sleep(10)


if __name__ == "__main__":
    run_bronze_producer()