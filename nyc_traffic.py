import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime

# ==========================================
# CONFIGURATION
# ==========================================
# Querying for the most recent speed data across all links
# Using $order to get the latest timestamps first
API_ENDPOINT = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json?$limit=2000&$order=data_as_of DESC"
APP_TOKEN = "gdLWTLhefvaSPLJI2AV4lTv4m"
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "nyc_traffic_"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_latest_road_status():
    """
    Fetches the most recent reported speed for every road segment (Link).
    """
    headers = {"X-App-Token": APP_TOKEN}
    try:
        print(f"📡 [{datetime.now().strftime('%H:%M:%S')}] Fetching latest status for all New York roads...")
        response = requests.get(API_ENDPOINT, headers=headers, timeout=45)
        response.raise_for_status()
        data = response.json()

        if not data:
            print("⚪ No data received from the sensors.")
            return

        # We use a dictionary to keep only the absolute latest record for each unique Link ID
        latest_updates = {}
        for record in data:
            link_id = record.get('link_id')
            if link_id not in latest_updates:
                latest_updates[link_id] = record

        # Send the unique latest records to Kafka
        for link_id, record in latest_updates.items():
            producer.send(TOPIC_NAME, value=record)

        producer.flush()
        print(f"✅ Success! Sent latest status for {len(latest_updates)} unique road segments.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    while True:
        fetch_latest_road_status()
        time.sleep(60) # Poll every minute for updates