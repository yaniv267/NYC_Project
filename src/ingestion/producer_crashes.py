# import requests, json, time
# from kafka import KafkaProducer
# from datetime import datetime, timedelta, timezone # השינוי הראשון כאן

# # =========================
# # 2. CONFIGURATION & KAFKA SETUP
# # =========================
# APP_TOKEN = "gdLWTLhefvaSPLJI2AV4lTv4m"
# KAFKA_BROKER = "course-kafka:9093"
# TOPIC_NAME = "nyc_crashes_stream"

# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BROKER,
#     value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#     acks='all'
# )

# print(f"🚀 Crash Maintenance Producer is LIVE. Monitoring NYC for new accidents...")

# # =========================
# # 3. CONTINUOUS INGESTION LOOP
# # =========================
# while True:
#     try:
#         # Step A: Define Lookback Window (60 days) - השינוי השני כאן
#         start_date = (datetime.now(timezone.utc) - timedelta(days=60)).strftime('%Y-%m-%d')

#         offset = 0
#         limit = 5000  # Max records per API call
#         total_in_cycle = 0

#         print(f"⏳ Starting sync cycle for data since {start_date}...")

#         # לולאת ה-Pagination: ממשיכה לרוץ כל עוד יש נתונים נוספים ב-60 הימים האחרונים
#         while True:
#             # Step B: Construct API URL with Offset for Pagination
#             url = (
#                 f"https://data.cityofnewyork.us/resource/h9gi-nx95.json"
#                 f"?$where=crash_date >= '{start_date}'"
#                 f"&$limit={limit}&$offset={offset}"
#                 f"&$order=crash_date DESC, crash_time DESC"
#             )

#             # Step C: Fetch data from NYC Open Data API
#             res = requests.get(url, headers={"X-App-Token": APP_TOKEN}, timeout=60)
#             res.raise_for_status()
#             data = res.json()

#             if not data or not isinstance(data, list):
#                 break  # Finished fetching all records for this cycle

#             # Step D: Process and Send records to Kafka
#             for r in data:
#                 # השינוי השלישי כאן
#                 r['ingested_at'] = datetime.now(timezone.utc).isoformat()
#                 crash_id = str(r.get('collision_id', 'unknown'))
#                 producer.send(TOPIC_NAME, key=crash_id.encode('utf-8'), value=r)

#             producer.flush()
#             batch_size = len(data)
#             total_in_cycle += batch_size
#             print(f"📦 Batch Sent: {batch_size} records | Total so far: {total_in_cycle} | Offset: {offset}")

#             # אם קיבלנו פחות מה-limit, סימן שהגענו לסוף הנתונים ב-60 הימים
#             if batch_size < limit:
#                 break

#             offset += limit  # Increment offset for next page
#             time.sleep(0.5)  # Short pause to be kind to the API

#         print(f"✅ [{datetime.now().strftime('%H:%M:%S')}] Cycle complete. Total sent: {total_in_cycle}")

#     except Exception as e:
#         print(f"❌ Error during sync: {e}")

#     # =========================
#     # 4. SLEEP INTERVAL
#     # =========================
#     print("😴 Sleeping for 1 hour until next maintenance check...")
#     time.sleep(3600)

import requests, json, time
from kafka import KafkaProducer
from datetime import datetime, timedelta, timezone

# =========================
# 2. CONFIGURATION & KAFKA SETUP
# =========================
APP_TOKEN = "gdLWTLhefvaSPLJI2AV4lTv4m"
KAFKA_BROKER = "course-kafka:9093"
TOPIC_NAME = "nyc_crashes_stream"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks='all'
)

print(f"🚀 Crash Maintenance Producer is LIVE. Monitoring NYC for new accidents...")

# =========================
# 3. SINGLE INGESTION CYCLE
# =========================
try:
    # Step A: Define Lookback Window (60 days)
    start_date = (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%d')

    offset = 0
    limit = 1000  # Max records per API call
    total_in_cycle = 0

    print(f"⏳ Starting sync cycle for data since {start_date}...")

    # לולאת ה-Pagination: רצה עד שנגמרים הנתונים לסבב הנוכחי
    while True:
        # Step B: Construct API URL with Offset for Pagination
        url = (
            f"https://data.cityofnewyork.us/resource/h9gi-nx95.json"
            f"?$where=crash_date >= '{start_date}'"
            f"&$limit={limit}&$offset={offset}"
            f"&$order=crash_date DESC, crash_time DESC"
        )

        # Step C: Fetch data from NYC Open Data API
        res = requests.get(url, headers={"X-App-Token": APP_TOKEN}, timeout=120)
        res.raise_for_status()
        data = res.json()

        if not data or not isinstance(data, list):
            break  # Finished fetching all records for this cycle

        # Step D: Process and Send records to Kafka
        for r in data:
            r['ingested_at'] = datetime.now(timezone.utc).isoformat()
            crash_id = str(r.get('collision_id', 'unknown'))
            producer.send(TOPIC_NAME, key=crash_id.encode('utf-8'), value=r)

        producer.flush()
        batch_size = len(data)
        total_in_cycle += batch_size
        print(f"📦 Batch Sent: {batch_size} records | Total so far: {total_in_cycle} | Offset: {offset}")

        # אם קיבלנו פחות מה-limit, סימן שהגענו לסוף הנתונים
        if batch_size < limit:
            break

        offset += limit  # Increment offset for next page
        time.sleep(0.5)  # Short pause to be kind to the API

    print(f"✅ [{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Cycle complete. Total sent: {total_in_cycle}")

except Exception as e:
    print(f"❌ Error during sync: {e}")
    exit(1) # מוודא ש-Airflow יזהה כישלון אם היתה שגיאה

# =========================
# 4. TASK FINISHED
# =========================
# הסקריפט מסתיים כאן, Airflow ימשיך כעת למשימה הבאה ב-DAG (Bronze)
print("🏁 Task finished successfully.")