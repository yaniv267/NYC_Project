import io
import json
import pandas as pd
from minio import Minio
from sqlalchemy import create_engine

# --- הגדרות MINIO ---
MINIO_ENDPOINT = '127.0.0.1:9001'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
BUCKET_NAME = 'complaints311'

# --- הגדרות PostgreSQL ---
PG_USER = 'postgres'
PG_PASSWORD = 'postgres'
PG_HOST = 'localhost'
PG_PORT = '5432'
PG_DB = 'airflow'
TABLE_NAME = 'nyc_complaints_raw'


def load_data_to_postgres():
    minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    db_url = f'postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}'
    engine = create_engine(db_url)

    print(f"Connecting to MinIO bucket '{BUCKET_NAME}'...")
    objects = minio_client.list_objects(BUCKET_NAME)

    # רשימה שתאגור את כל הנתונים מכל הקבצים
    all_dataframes = []

    for obj in objects:
        file_name = obj.object_name
        if not file_name.endswith('.parquet'):
            continue

        print(f"Reading file: {file_name} from MinIO...")
        response = minio_client.get_object(BUCKET_NAME, file_name)
        file_data = io.BytesIO(response.read())
        df = pd.read_parquet(file_data)

        # --- התיקון לשמירת עמודת location כ-JSON ---
        def parse_location(loc):
            if isinstance(loc, dict):
                if 'coordinates' in loc and hasattr(loc['coordinates'], 'tolist'):
                    loc['coordinates'] = loc['coordinates'].tolist()
                return json.dumps(loc)
            return loc

        if 'location' in df.columns:
            df['location'] = df['location'].apply(parse_location)

        for col in df.columns:
            df[col] = df[col].apply(lambda x: str(x) if isinstance(x, (dict, list, tuple)) else x)
        # --- סיום התיקון ---

        # מוסיפים את ה-DataFrame של הקובץ הנוכחי לרשימה הכללית
        all_dataframes.append(df)
        response.close()

    # השלב המכריע: אם מצאנו קבצים, נאחד אותם ונטען יחד
    if all_dataframes:
        print("\nCombining all datafiles into a single master DataFrame...")
        # פעולת ה-concat תשלב את כל העמודות, ותשים NULL במקומות שבהם השדה לא היה קיים
        master_df = pd.concat(all_dataframes, ignore_index=True)

        print(f"Loading a total of {len(master_df)} rows into PostgreSQL table '{TABLE_NAME}'...")
        # נשתמש ב-replace כדי לדרוס את הטבלה השבורה מההרצה הקודמת וליצור טבלה מלאה
        master_df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
        print("Success! All data loaded perfectly.")
    else:
        print("No Parquet files found in the bucket.")


if __name__ == "__main__":
    load_data_to_postgres()