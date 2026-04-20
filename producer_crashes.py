# # import requests, json, time
# # from kafka import KafkaProducer
# # from datetime import datetime, timedelta, UTC


# # # ==========================================
# # # CONFIGURATION - NYC CRASHES MAINTENANCE
# # # ==========================================
# # APP_TOKEN = "gdLWTLhefvaSPLJI2AV4lTv4m"
# # KAFKA_BROKER = "localhost:9092"
# # TOPIC_NAME = "nyc_crashes_stream"

# # producer = KafkaProducer(
# #     bootstrap_servers=KAFKA_BROKER,
# #     value_serializer=lambda v: json.dumps(v).encode("utf-8"),
# #     acks='all'
# # )

# # print(f"🚀 Crash Maintenance Producer is LIVE. Monitoring NYC for new accidents...")

# # while True:
# #     try:
# #         # 1. לקיחת חלון זמן של עמות הזמן שמוגדר אחורה כדי למנוע חורים במידע
# #         start_date = (datetime.now(UTC) - timedelta(days=14)).strftime('%Y-%m-%d')

# #         # 2. שאילתה ל-API (מביאה את ה-2000 הכי חדשים מהיומיים האחרונים)
# #         url = (
# #             f"https://data.cityofnewyork.us/resource/h9gi-nx95.json"
# #             f"?$where=crash_date >= '{start_date}'"
# #             f"&$limit=2000&$order=crash_date DESC, crash_time DESC"
# #         )

# #         res = requests.get(url, headers={"X-App-Token": APP_TOKEN}, timeout=30)
# #         res.raise_for_status()
# #         data = res.json()

# #         if data and isinstance(data, list):
# #             for r in data:
# #                 # Metadata לשכבת ה-Bronze
# #                 r['ingested_at'] = datetime.now(UTC).isoformat()

# #                 # שימוש ב-collision_id כמפתח (מבטיח שספארק ינקה כפילויות בקלות)
# #                 crash_id = str(r.get('collision_id', 'unknown'))
# #                 producer.send(TOPIC_NAME, key=crash_id.encode('utf-8'), value=r)

# #             producer.flush()
# #             print(f"✅ [{datetime.now().strftime('%H:%M:%S')}] Sync complete. Sent {len(data)} records.")
# #         else:
# #             print(f"⚪ [{datetime.now().strftime('%H:%M:%S')}] No new data found in the last 48h.")

# #     except Exception as e:
# #         print(f"❌ Error during sync: {e}")

# #     # 3. הולך לישון לשעה. תאונות ב-API לא מתעדכנות כל דקה.
# #     print("😴 Sleeping for 1 hour...")
# #     time.sleep(3600)

# ###v.s.code producer####
# import requests, json, time
# from kafka import KafkaProducer
# from datetime import datetime, timedelta, timezone # שינוי כאן

# # ==========================================
# # CONFIGURATION - NYC CRASHES MAINTENANCE
# # ==========================================
# APP_TOKEN = "gdLWTLhefvaSPLJI2AV4lTv4m"
# KAFKA_BROKER = "localhost:9092"
# TOPIC_NAME = "nyc_crashes_stream"

# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BROKER,
#     value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#     acks='all'
# )

# print(f"🚀 Crash Maintenance Producer is LIVE. Monitoring NYC for new accidents...")

# while True:
#     try:
#         # 1. שימוש ב-timezone.utc במקום UTC עבור Python 3.10
#         start_date = (datetime.now(timezone.utc) - timedelta(days=14)).strftime('%Y-%m-%d')

#         # 2. שאילתה ל-API
#         url = (
#             f"https://data.cityofnewyork.us/resource/h9gi-nx95.json"
#             f"?$where=crash_date >= '{start_date}'"
#             f"&$limit=2000&$order=crash_date DESC, crash_time DESC"
#         )

#         res = requests.get(url, headers={"X-App-Token": APP_TOKEN}, timeout=30)
#         res.raise_for_status()
#         data = res.json()

#         if data and isinstance(data, list):
#             for r in data:
#                 # Metadata לשכבת ה-Bronze - גם כאן שימוש ב-timezone.utc
#                 r['ingested_at'] = datetime.now(timezone.utc).isoformat()

#                 # שימוש ב-collision_id כמפתח
#                 crash_id = str(r.get('collision_id', 'unknown'))
#                 producer.send(TOPIC_NAME, key=crash_id.encode('utf-8'), value=r)

#             producer.flush()
#             print(f"✅ [{datetime.now().strftime('%H:%M:%S')}] Sync complete. Sent {len(data)} records.")
#         else:
#             print(f"⚪ [{datetime.now().strftime('%H:%M:%S')}] No new data found in the last 14 days.")

#     except Exception as e:
#         print(f"❌ Error during sync: {e}")

#     # 3. מנוחה לשעה
#     print("😴 Sleeping for 1 hour...")
#     time.sleep(3600)
    
    
import requests, json, time
from kafka import KafkaProducer
from datetime import datetime, timedelta, UTC

# =========================
# 2. CONFIGURATION & KAFKA SETUP
# =========================
APP_TOKEN = "gdLWTLhefvaSPLJI2AV4lTv4m"
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "nyc_crashes_stream"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks='all'
)

print(f"🚀 Crash Maintenance Producer is LIVE. Monitoring NYC for new accidents...")

# =========================
# 3. CONTINUOUS INGESTION LOOP
# =========================
while True:
    try:
        # Step A: Define Lookback Window (60 days)
        start_date = (datetime.now(UTC) - timedelta(days=60)).strftime('%Y-%m-%d')

        offset = 0
        limit = 5000  # Max records per API call
        total_in_cycle = 0

        print(f"⏳ Starting sync cycle for data since {start_date}...")

        # לולאת ה-Pagination: ממשיכה לרוץ כל עוד יש נתונים נוספים ב-60 הימים האחרונים
        while True:
            # Step B: Construct API URL with Offset for Pagination
            url = (
                f"https://data.cityofnewyork.us/resource/h9gi-nx95.json"
                f"?$where=crash_date >= '{start_date}'"
                f"&$limit={limit}&$offset={offset}"
                f"&$order=crash_date DESC, crash_time DESC"
            )

            # Step C: Fetch data from NYC Open Data API
            res = requests.get(url, headers={"X-App-Token": APP_TOKEN}, timeout=60)
            res.raise_for_status()
            data = res.json()

            if not data or not isinstance(data, list):
                break  # Finished fetching all records for this cycle

            # Step D: Process and Send records to Kafka
            for r in data:
                r['ingested_at'] = datetime.now(UTC).isoformat()
                crash_id = str(r.get('collision_id', 'unknown'))
                producer.send(TOPIC_NAME, key=crash_id.encode('utf-8'), value=r)

            producer.flush()
            batch_size = len(data)
            total_in_cycle += batch_size
            print(f"📦 Batch Sent: {batch_size} records | Total so far: {total_in_cycle} | Offset: {offset}")

            # אם קיבלנו פחות מה-limit, סימן שהגענו לסוף הנתונים ב-60 הימים
            if batch_size < limit:
                break

            offset += limit  # Increment offset for next page
            time.sleep(0.5)  # Short pause to be kind to the API

        print(f"✅ [{datetime.now().strftime('%H:%M:%S')}] Cycle complete. Total sent: {total_in_cycle}")

    except Exception as e:
        print(f"❌ Error during sync: {e}")

    # =========================
    # 4. SLEEP INTERVAL
    # =========================
    print("😴 Sleeping for 1 hour until next maintenance check...")
    time.sleep(3600)