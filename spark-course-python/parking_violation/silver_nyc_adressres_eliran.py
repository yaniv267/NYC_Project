# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, upper, trim, concat_ws
# from nyc_schema import silver_address_schema
# from pyspark.sql.functions import col, upper, trim, concat_ws, from_json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from nyc_schema_eliran import silver_address_schema 

spark = SparkSession.builder \
    .appName("silver_nyc_addresses") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# input_path = "s3a://spark/bronze/addresses_eliran/"
# output_path = "s3a://spark/silver/addresses_eliran/"

bronze_df = spark.read.parquet("s3a://spark/bronze/addresses/")            


print(f"🧹 Refining Bronze data to Silver...")

# השתקת הודעות מערכת לא חשובות כדי שהטרמינל יהיה נקי
spark.sparkContext.setLogLevel("WARN")

# 1. קריאת כל ה-JSONs מהברונז בבת אחת
#df_raw = spark.read.json(input_path)
# df_raw = spark.read.parquet(input_path)

# # 2. עיבוד והתאמה לסכימה
# df_silver = df_raw.select(
#     col("addresspointid").cast("string").alias("address_id"),
#     upper(trim(col("full_street_name"))).alias("street_name_clean"),
#     upper(trim(concat_ws(" ", col("house_number"), col("full_street_name")))).alias("full_address"),
#     col("zipcode").cast("string").alias("zip_code"),
#     # חילוץ קואורדינטות מהמבנה הגיאומטרי
#     #col("the_geom.coordinates")[0].cast("double").alias("longitude"),
#     #col("the_geom.coordinates")[1].cast("double").alias("latitude")
#     from_json(col("the_geom.coordinates"), "array<double>")[0].alias("longitude"),
#     from_json(col("the_geom.coordinates"), "array<double>")[1].alias("latitude")
# )


df_silver = (
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


final_columns = [field.name for field in silver_address_schema]
df_silver_final = df_silver.select(*final_columns)
# 3. שמירה כ-Parquet (מהיר פי 10 ל-Join ב-Gold)

print("💾 Saving cleaned data to Silver layer...")

# כתיבת הנתונים הנקיים בחזרה ל-MinIO בתיקיית הסילבר, תוך דריסת נתונים ישנים אם קיימים
df_silver_final.write.mode("overwrite").parquet("s3a://spark/silver/addresses/")

# df_silver.write.mode("overwrite").parquet(output_path)
# print(f"✅ Silver Layer Ready at {output_path}")



print("💾 Saving cleaned data to Silver layer...")


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