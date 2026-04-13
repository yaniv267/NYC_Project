import json
import io
import time
import pandas as pd
from kafka import KafkaConsumer
from minio import Minio

# --- הגדרות קפקה ---
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'complaints_Stream'

# --- הגדרות MinIO ---
MINIO_ENDPOINT = 'localhost:9001'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
BUCKET_NAME = 'complaints311'  # הבאקט החדש שביקשת

# אתחול ה-Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='minio-parquet-consumer',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# אתחול חיבור ל-MinIO
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# יצירת הבאקט אם הוא עדיין לא קיים
if not minio_client.bucket_exists(BUCKET_NAME):
    minio_client.make_bucket(BUCKET_NAME)
    print(f"Created MinIO bucket: '{BUCKET_NAME}'")


def consume_and_upload():
    print(f"Listening to topic '{TOPIC_NAME}' and uploading Parquet files to '{BUCKET_NAME}'...")
    batch = []
    BATCH_SIZE = 500  # מספר הרשומות לאיסוף לפני כתיבת הקובץ

    for message in consumer:
        complaint = message.value
        batch.append(complaint)
        print(f"Consumed complaint ID: {complaint.get('unique_key')}")

        # כאשר הבאצ' מתמלא, ממירים ל-Parquet וכותבים ל-MinIO
        if len(batch) >= BATCH_SIZE:
            timestamp = int(time.time())
            object_name = f"complaints_batch_{timestamp}.parquet"

            # יצירת DataFrame מתוך רשימת המילונים
            df = pd.DataFrame(batch)

            # כתיבת ה-DataFrame לזיכרון (BytesIO) בפורמט Parquet
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, engine='pyarrow', index=False)

            # החזרת הסמן לתחילת הקובץ בזיכרון
            parquet_buffer.seek(0)

            # פעולת ההעלאה ל-MinIO
            minio_client.put_object(
                bucket_name=BUCKET_NAME,
                object_name=object_name,
                data=parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type='application/vnd.apache.parquet'
            )
            print(f"--> Uploaded {BATCH_SIZE} records to MinIO as: {object_name} <--")

            # איפוס הרשימה לבאצ' הבא
            batch = []


if __name__ == "__main__":
    consume_and_upload()