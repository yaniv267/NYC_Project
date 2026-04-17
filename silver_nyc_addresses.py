from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from nyc_schema import silver_address_schema 
# 1. אתחול הסשן
spark = SparkSession.builder \
    .appName("silver_nyc_addresses") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
# השתקת הודעות מערכת לא חשובות כדי שהטרמינל יהיה נקי
spark.sparkContext.setLogLevel("WARN")

# ==========================================
# 2. קריאת הנתונים הגולמיים (Bronze)
# ==========================================
print("📥 Loading raw data from Bronze layer...")

# קריאת כל 967,000 הרשומות ששמרנו קודם בפורמט Parquet
bronze_df = spark.read.parquet("s3a://spark/bronze/addresses/")

# ==========================================
# 3. עיבוד וניקוי הנתונים (Silver Transformation)
# ==========================================
print("🧹 Cleaning data and extracting coordinates...")

silver_df = (
    bronze_df
    
    # סינון שורות פגומות: אנחנו שומרים רק שורות שיש בהן מיקום גיאוגרפי ושם רחוב מלא
    .filter(F.col("the_geom").isNotNull() & F.col("Full Street Name").isNotNull())
    
    # חילוץ קו אורך (Longitude):
    # העמודה המקורית נראית ככה: POINT (-73.922 40.702)
    # אנחנו משתמשים בביטוי רגולרי (Regex) כדי לשלוף את המספר הראשון ולהמיר אותו לעשרוני (double)
    .withColumn("longitude", F.regexp_extract(F.col("the_geom"), r"POINT \(([^ ]+) ([^ ]+)\)", 1).cast("double"))
    
    # חילוץ קו רוחב (Latitude):
    # שולפים את המספר השני מתוך הסוגריים וממירים לעשרוני
    .withColumn("latitude", F.regexp_extract(F.col("the_geom"), r"POINT \(([^ ]+) ([^ ]+)\)", 2).cast("double"))
    
    # בחירת העמודות שנצטרך להמשך ושינוי השמות שלהן לאנגלית תקנית וללא רווחים
    .select(
        # מזהה הכתובת מומר למחרוזת
        F.col("Address Point ID").cast("string").alias("address_id"),
        
        # ניקוי רווחים מיותרים בתחילת ובסוף שם הרחוב
        F.trim(F.col("Full Street Name")).alias("street_name"),
        
        # קוד הרובע מומר למחרוזת כדי שיתאים לטבלת דוחות החניה בהמשך
        F.col("Borough Code").cast("string").alias("borough_code"),
        
        # מיקוד מומר למחרוזת
        F.col("ZIPCODE").cast("string").alias("zip_code"),
        
        # העמודות החדשות שיצרנו
        "longitude",
        "latitude"
    )
    
    # הסרת כפילויות: מוודאים שכל מזהה כתובת מופיע פעם אחת בלבד
    .dropDuplicates(["address_id"])
)


# 4. אכיפת סכימה סופית (The Data Contract)
final_columns = [field.name for field in silver_address_schema]
df_silver_final = silver_df.select(*final_columns)
# ==========================================
# 4. שמירת התוצאות (Write to Silver)
# ==========================================
print("💾 Saving cleaned data to Silver layer...")

# כתיבת הנתונים הנקיים בחזרה ל-MinIO בתיקיית הסילבר, תוך דריסת נתונים ישנים אם קיימים
df_silver_final.write.mode("overwrite").parquet("s3a://spark/silver/addresses/")

# ==========================================
# 5. בדיקה והצגת התוצאות (Validation)
# ==========================================
print("\n" + "="*50)
print("📊 Silver Layer Results")
print("="*50)

# ספירת כמות השורות הסופית שעברה את הסינון
final_count = df_silver_final.count()
print(f"Total processed records: {final_count:,}")

# הדפסת מבנה הנתונים החדש והנקי
print("\n--- Final Silver Schema: ---")
df_silver_final.printSchema()

# הצגת 10 השורות הראשונות כדי לראות את הנתונים בעין
print("\n--- Top 10 rows: ---")
df_silver_final.show(10, truncate=False)
print("="*50)

# סגירת סביבת העבודה בסיום התהליך
spark.stop()