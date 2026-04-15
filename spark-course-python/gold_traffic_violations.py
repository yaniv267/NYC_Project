import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-memory 4g --executor-memory 4g pyspark-shell"
# 1. אתחול ספארק
spark = (SparkSession.builder \
    .appName("nyc_gold_traffic_violations") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.13.2") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate())


def normalize_street_logic(c):
    c = F.upper(F.trim(c))
    c = F.regexp_replace(c, r"^(NB|SB|EB|WB|N/B|S/B|E/B|W/B)\s+", "")
    c = F.regexp_replace(c, r"(\d+)(ST|ND|RD|TH)", r"$1")
    c = F.regexp_replace(c, r"\bAVENUE\b", "AVE")
    c = F.regexp_replace(c, r"\bSTREET\b", "ST")
    c = F.regexp_replace(c, r"\bSTR\b", "ST")
    c = F.regexp_replace(c, r"\bBOULEVARD\b|\bBLV\b", "BLVD")
    c = F.regexp_replace(c, r"\bPARKWAY\b", "PKWY")
    c = F.regexp_replace(c, r"\s+", " ") 
    return c

print("🚀 Starting Unified Gold Pipeline...")


#silver_df = spark.read.parquet("s3a://spark/silver/nyc_traffic_violations/").cache()
silver_df = spark.read.parquet("s3a://spark/silver/nyc_parking_violation/")
addresses_df = spark.read.parquet("s3a://spark/silver/addresses/")


lookup_codes = spark.read.parquet("s3a://spark/silver/violation_codes/") \
    .withColumn("violation_code", F.col("violation_code").cast("int"))

# 4. הכנת שכבת המפות (Address Master)
address_master = (addresses_df
    .withColumn("norm_addr", normalize_street_logic(F.col("street_name_clean")))
    .groupBy("norm_addr")
    .agg(F.avg("latitude").alias("lat"), F.avg("longitude").alias("lon"))
).cache()

# 5. יצירת טבלת ה-Gold המאוחדת
camera_codes = [7, 21, 36, 38]

gold_final = (silver_df
    # 1. הכנת הנתונים לפני הקיבוץ
    .withColumn("street_clean", F.split(F.col("street_name"), "@")[0])
    .withColumn("norm_viol", normalize_street_logic(F.col("street_clean")))
    .withColumn("issue_date_tmp", F.to_date(F.col("issue_date"))) # שיניתי שם זמני למניעת בלבול
    
    # 2. אגרגציה - כאן נוצרים start_date ו-end_date
    .groupBy("street_name", "norm_viol", "violation_code", "violation_county")
    .agg(
        F.count("summons_number").alias("tickets_count"),
        F.min("issue_date_tmp").alias("start_date"), 
        F.max("issue_date_tmp").alias("end_date")
    )
    
    # 3. יצירת יום בשבוע מתוך התאריך האחרון שנמצא
    .withColumn("issue_day_name", F.date_format(F.col("end_date"), "EEEE"))
    
    # 4. Joins (Lookup וקואורדינטות)
    .join(F.broadcast(lookup_codes), on="violation_code", how="left")
    .join(F.broadcast(address_master), F.col("norm_viol") == F.col("norm_addr"), "left")
    
    # 5. בחירה סופית - שים לב לשינוי ב-issue_date
    .select(
        F.col("street_name"),
        F.col("violation_code"),
        F.coalesce(F.col("violation_description"), F.lit("Unknown Violation")).alias("violation_desc"),
        F.col("tickets_count"),
        F.col("start_date"),
        F.col("end_date"),
        # כאן התיקון: אנחנו מציגים את ה-end_date בתור ה-issue_date הסופי
        F.col("end_date").alias("issue_date"),
        F.col("issue_day_name"),
        F.col("lat").cast("double"),
        F.col("lon").cast("double"),
        F.concat_ws(",", F.col("lat").cast("string"), F.col("lon").cast("string")).alias("location"), # for ELK - lat,lon
        F.when(F.col("violation_code").isin(camera_codes), "Camera").otherwise("Parking").alias("category"),
        F.when(F.col("tickets_count") > 1000, "🔴 High Risk")
         .when(F.col("tickets_count") > 500, "🟠 Medium Risk")
         .otherwise("🟢 Low Risk").alias("risk_level"),
        F.when(F.col("violation_county").isin("K", "Kings"), "Brooklyn")
         .when(F.col("violation_county").isin("Q", "Queens"), "Queens")
         .when(F.col("violation_county").isin("NY", "Manhattan"), "Manhattan")
         .when(F.col("violation_county").isin("BX", "Bronx"), "Bronx")
         .otherwise("Other").alias("borough"),
        F.current_timestamp().alias("last_updated")
    )
)
# 7. בדיקת תוצאות
gold_final = gold_final.coalesce(4).cache()

final_count = gold_final.count()
print(f"✅ Created Gold layer with {final_count} rows.")
gold_final.orderBy(F.col("tickets_count").desc()).show(10, truncate=False)

# 8. שמירה כפולה: גם למינאו (גולד) וגם לפוסטגרס (בוט)
if final_count > 0:
    # שמירה למינאו כפורמט Parquet
    print("💾 Saving Gold Parquet to MinIO...")
    gold_final.write.mode("overwrite").parquet("s3a://spark/gold/traffic_violations/")
    
    # שמירה לפוסטגרס
    jdbc_url = "jdbc:postgresql://postgres:5432/nyc_data"
    db_props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}
    
    try:
        print("💎 Syncing to Postgres...")
        gold_final.write.jdbc(url=jdbc_url, table="gold_traffic_violations", mode="overwrite", properties=db_props)
        print("🚀 DONE! Everything is ready.")
    except Exception as e:
        print(f"❌ Postgres Error: {e}")

    try:
        print("💎 Syncing to Elasticsearch...")
        (gold_final.filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
            .write
            .format("org.elasticsearch.spark.sql")
            .option("es.nodes", "elasticsearch") 
            .option("es.port", "9200")
            .option("es.nodes.wan.only", "true")
            .option("es.resource", "nyc_parking_gold") 
                # הגדרות למניעת חנק CPU:
            .option("es.batch.size.entries", "500") # שולח 500 שורות בכל פעם במקום אלפים
            .option("es.http.timeout", "1m")        # נותן לאלסטיק דקה להגיב
            .option("es.http.retries", "3")         # מנסה שוב אם יש עומס
            .mode("append") # שינינו ל-append כי האינדקס כבר קיים!
            .save())
        print("✅ Success! Data is in Elasticsearch.")
    except Exception as e:
            print(f"❌ Elasticsearch Write Error: {e}")

spark.stop()