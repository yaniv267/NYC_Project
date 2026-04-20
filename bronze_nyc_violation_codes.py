
import boto3, json, requests
# =========================
# 2. CONFIGURATION & S3 SETTINGS
# =========================
URL = "https://data.cityofnewyork.us/resource/ncbg-6agr.json"
S3_CONF = {
    "endpoint_url": "http://minio:9000",
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin",
    "region_name": "us-east-1"
}

# =========================
# 3. INGESTION FUNCTION
# =========================

def ingest():
    # Step A: Download raw data from API
    print("📥 Downloading codes...")
    data = requests.get(URL).json()
    
   # Step B: Initialize S3 Client and Upload to MinIO
    s3 = boto3.client('s3', **S3_CONF)
    
    s3.put_object(
        Bucket="spark",
        Key="bronze/violation_codes.json",
        Body=json.dumps(data),
        ContentType='application/json'
    )
    print(f"✅ Success! {len(data)} codes saved to MinIO.")

if __name__ == "__main__":
    ingest()