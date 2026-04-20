# import json
# import time
# import requests
# from kafka import KafkaProducer

# # --- הגדרות ---
# KAFKA_BROKER = 'localhost:9092'  # שנה לכתובת הקפקה שלך
# TOPIC_NAME = 'complaints_Stream'
# # ה-Endpoint הרשמי של NYC 311 Service Requests
# NYC_311_API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

# # אתחול ה-Producer עם סריאליזציה ל-JSON
# producer = KafkaProducer(
#     bootstrap_servers=[KAFKA_BROKER],
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )


# def fetch_and_produce():
#     # הבאת 100 התלונות האחרונות (בפרודקשן כדאי לנהל offset או לשלוף לפי תאריכים)
#     params = {
#         "$limit": 10000,
#         "$order": "created_date DESC"
#     }

#     print("Fetching data from NYC 311 API...")
#     response = requests.get(NYC_311_API_URL, params=params)

#     if response.status_code == 200:
#         complaints = response.json()
#         print(f"Successfully fetched {len(complaints)} records.")

#         for complaint in complaints:
#             # שליחת התלונה ל-Kafka Topic
#             producer.send(TOPIC_NAME, complaint)
#             unique_key = complaint.get('unique_key', 'N/A')
#             print(f"Produced complaint ID: {unique_key} to topic {TOPIC_NAME}")

#             # השהייה קלה כדי לדמות Streaming (אופציונלי)
#             time.sleep(0.1)

#             # וידוא שכל ההודעות נשלחו
#         producer.flush()
#         print("Finished sending batch to Kafka.")
#     else:
#         print(f"Failed to fetch data. Status code: {response.status_code}")


# if __name__ == "__main__":
#     fetch_and_produce()

import json
import time
import requests
from kafka import KafkaProducer

# =========================
# 2. CONFIGURATION & KAFKA SETUP
# =========================
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'complaints_stream'
NYC_311_API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

# Initialize Kafka Producer with JSON serialization
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


# =========================
# 3. DATA FETCHING & PRODUCTION LOGIC
# =========================
def fetch_and_produce():
    # Set parameters to fetch the latest 10,000 complaints
    params = {
        "$limit": 10000,
        "$order": "created_date DESC"
    }

    print(f"🚀 Fetching data from NYC 311 API: {NYC_311_API_URL}")
    response = requests.get(NYC_311_API_URL, params=params)

    if response.status_code == 200:
        complaints = response.json()
        print(f"✅ Successfully fetched {len(complaints)} records.")

        for complaint in complaints:
            # Extract unique key for Kafka partitioning
            unique_key = complaint.get('unique_key', 'N/A')

            # Send record to Kafka with unique_key as the message key
            producer.send(TOPIC_NAME, key=str(unique_key).encode('utf-8'), value=complaint)

            # Optional: Minimal sleep to simulate real-time stream flow
            # time.sleep(0.01)

        # Ensure all buffered messages are sent
        producer.flush()
        print(f"🏁 Finished sending batch of {len(complaints)} records to Kafka.")
    else:
        print(f"❌ Failed to fetch data. Status code: {response.status_code}")


# =========================
# 4. EXECUTION ENTRY POINT
# =========================
if __name__ == "__main__":
    fetch_and_produce()