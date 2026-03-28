import requests
import json
import boto3
from datetime import datetime, date

# --- הגדרות הפרויקט ---
LAT, LON = 40.7128, -74.0060
START_DATE = "2025-10-01"
END_DATE = date.today().strftime('%Y-%m-%d')

# הגדרות MinIO לתוך Docker
MINIO_CONF = {
    "endpoint_url": "http://minio:9000", 
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin"
}
BUCKET_NAME = "spark"

def ingest_weather_bronze():
    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={LAT}&longitude={LON}&"
        f"start_date={START_DATE}&end_date={END_DATE}&"
        f"hourly=temperature_2m,precipitation,snowfall&"
        f"timezone=America%2FNew_York"
    )

    print(f"📡 Requesting raw history from Open-Meteo...")

    try:
        # 1. שליפת הנתונים
        response = requests.get(url)
        response.raise_for_status()
        raw_data = response.json()

        # 2. הצגת דגימה מהנתונים (השורה שביקשת)
        print("\n🔍 --- RAW DATA PREVIEW (First 5 hours) ---")
        times = raw_data['hourly']['time'][:5]
        temps = raw_data['hourly']['temperature_2m'][:5]
        
        for t, temp in zip(times, temps):
            print(f"Time: {t} | Temp: {temp}°C")
        print("-------------------------------------------\n")

        # 3. שמירה ל-MinIO
        s3 = boto3.client('s3', **MINIO_CONF)
        
        # וודא באקט קיים
        try:
            s3.head_bucket(Bucket=BUCKET_NAME)
        except:
            s3.create_bucket(Bucket=BUCKET_NAME)

        file_key = f"data/bronze/weather/nyc_historical_raw_{datetime.now().strftime('%Y%m%d')}.json"

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_key,
            Body=json.dumps(raw_data, indent=4)
        )

        print(f"✅ Success! Raw JSON saved to MinIO as: {file_key}")

    except Exception as e:
        print(f"❌ Ingestion failed: {e}")

if __name__ == "__main__":
    ingest_weather_bronze()