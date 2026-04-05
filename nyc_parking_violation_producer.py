# import requests
# import json
# import time
# from kafka import KafkaProducer
# from datetime import datetime, timedelta, timezone

# # ==============================
# # CONFIG
# # ==============================
# API_ENDPOINT = "https://data.cityofnewyork.us/resource/pvqr-7yc4.json"
# APP_TOKEN = "gdLWTLhefvaSPLJI2AV4lTv4m"
# KAFKA_BROKER = "localhost:9092"
# TOPIC_NAME = "nyc_parking_violations_bronze"

# LIMIT = 1000
# SLEEP_BETWEEN_CALLS = 1

# # ==============================
# # KAFKA PRODUCER
# # ==============================
# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BROKER,
#     value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#     retries=5
# )

# # ==============================
# # DATE RANGE
# # ==============================
# today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
# # ניתן לשנות את last_n_years כדי להגדיל טווח
# last_n_years = 2
# start_date = (datetime.now(timezone.utc) - timedelta(days=365*last_n_years)).strftime("%Y-%m-%d")

# print(f"📅 מושך נתונים בין {start_date} ל-{today}")

# # ==============================
# # FETCH LOOP
# # ==============================
# offset = 0
# total_sent = 0

# while True:
#     params = {
#         "$where": f"issue_date between '{start_date}' and '{today}'",
#         "$order": "issue_date DESC",
#         "$limit": LIMIT,
#         "$offset": offset
#     }

#     headers = {
#         "X-App-Token": APP_TOKEN
#     }

#     try:
#         response = requests.get(API_ENDPOINT, params=params, headers=headers, timeout=30)

#         if response.status_code == 429:
#             print("⚠ Rate limit reached... sleeping 5 seconds")
#             time.sleep(5)
#             continue

#         if response.status_code != 200:
#             print("❌ API Error:", response.status_code)
#             print(response.text)
#             break

#         data = response.json()

#         if not data:
#             print("✅ אין עוד נתונים.")
#             break

#         print(f"📦 התקבלו {len(data)} רשומות (offset={offset})")

#         for record in data:
         
#             issue_date = record.get("issue_date", "")[:10]
#             if issue_date <= today:
#                 producer.send(TOPIC_NAME, value=record)
#                 total_sent += 1

#         producer.flush()
#         offset += LIMIT
#         time.sleep(SLEEP_BETWEEN_CALLS)

#     except Exception as e:
#         print("❌ שגיאה כללית:", str(e))
#         break

import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime, timedelta, timezone

# --- CONFIGURATION ---
API_ENDPOINT = "https://data.cityofnewyork.us/resource/pvqr-7yc4.json"
APP_TOKEN = "gdLWTLhefvaSPLJI2AV4lTv4m"
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "nyc_parking_violations_bronze"

LIMIT = 1000
SLEEP_BETWEEN_CALLS = 1

# --- DATE RANGE (Last 3 Months) ---
# החישוב מתבצע לפי התאריך של היום (אפריל 2026)
today = datetime.now(timezone.utc).date()
start_date = (today - timedelta(days=90)).strftime("%Y-%m-%d")
today_str = today.strftime("%Y-%m-%d")

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
    
    print(f"🚀 Starting Raw Data Ingestion: {start_date} to {today_str}")
    print(f"📡 Sending ALL columns to Topic: {TOPIC_NAME}")

    while True:
        # הסרנו את ה-$select כדי לקבל את כל הנתונים הגולמיים
        params = {
            "$where": f"issue_date >= '{start_date}' AND issue_date <= '{today_str}'",
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

            # שליחה לקפקא כ-JSON גולמי
            for record in data:
                producer.send(TOPIC_NAME, value=record)
                total_sent += 1

            producer.flush() 
            print(f"📦 Sent {len(data)} full records | Total: {total_sent} | Offset: {offset}")
            
            offset += LIMIT
            time.sleep(SLEEP_BETWEEN_CALLS)

        except Exception as e:
            print(f"❌ Critical Error: {str(e)}")
            break

if __name__ == "__main__":
    fetch_and_send_all()