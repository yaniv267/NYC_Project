# import boto3
# import json
# import requests

# # --- הגדרות ---
# # הלינק הרשמי למילון הקודים של ניו יורק
# URL = "https://data.cityofnewyork.us/resource/ncbg-6agr.json"

# # הגדרות MinIO (חיבור פנימי בתוך דוקר)
# MINIO_URL = "http://minio:9000" 
# ACCESS_KEY = "minioadmin"
# SECRET_KEY = "minioadmin"
# BUCKET = "spark"
# KEY = "reference/violation_codes.json"

# def main():
#     try:
#         # 1. הורדת הנתונים מהאינטרנט
#         print(f"🌐 מוריד נתונים מ-API...")
#         res = requests.get(URL, timeout=30)
#         res.raise_for_status()
#         data = res.json()
#         print(f"✅ הצלחתי להוריד {len(data)} קודים.")

#         # 2. התחברות ל-MinIO
#         # אנחנו משתמשים ב-9000 כי הקוד רץ בתוך הרשת של הדוקר
#         print(f"🔌 מתחבר ל-MinIO בכתובת: {MINIO_URL}...")
#         s3 = boto3.client('s3',
#             endpoint_url=MINIO_URL,
#             aws_access_key_id=ACCESS_KEY,
#             aws_secret_access_key=SECRET_KEY,
#             region_name='us-east-1'
#         )

#         # 3. העלאת ה-JSON
#         print(f"📤 מעלה קובץ ל-MinIO בנתיב: {BUCKET}/{KEY}...")
#         s3.put_object(
#             Bucket=BUCKET,
#             Key=KEY,
#             Body=json.dumps(data),
#             ContentType='application/json'
#         )

#         print("-" * 30)
#         print("🚀 סיימנו! הקובץ נשמר בהצלחה.")
#         print(f"עכשיו רענן את הדפדפן בפורט 9002 ותראה את תיקיית reference.")
#         print("-" * 30)

#     except Exception as e:
#         print(f"❌ אופס, משהו השתבש: {e}")

# if __name__ == "__main__":
#     main()

import boto3, json, requests

# הגדרות
URL = "https://data.cityofnewyork.us/resource/ncbg-6agr.json"
S3_CONF = {
    "endpoint_url": "http://minio:9000",
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin",
    "region_name": "us-east-1"
}

def ingest():
    # 1. הורדה מה-API
    print("📥 Downloading codes...")
    data = requests.get(URL).json()
    
    # 2. חיבור והעלאה ל-MinIO
    s3 = boto3.client('s3', **S3_CONF)
    
    s3.put_object(
        Bucket="spark",
        Key="reference/violation_codes.json",
        Body=json.dumps(data),
        ContentType='application/json'
    )
    print(f"✅ Success! {len(data)} codes saved to MinIO.")

if __name__ == "__main__":
    ingest()