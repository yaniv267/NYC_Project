from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, to_date, trim, lit,year, month, hour, date_format, to_timestamp, when, regexp_extract
from nyc_schema import silver_traffic_violations

# 1. יצירת Spark Session
spark = SparkSession.builder \
    .appName("NYC_Parking_Silver_Optimized") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 2. קריאה מה-Bronze
df_bronze = spark.read.parquet("s3a://spark/bronze/nyc_traffic_violations/")

# 3. תהליך הניקוי וההכנה לחיבור (Join Optimization)
df_cleaned = (
    df_bronze
    # א. יצירת עמודת borough_code - התיקון הקריטי ביותר ל-Join!
    # אנחנו מתרגמים את הקודים מהקפקא (NY, K, Q...) למספרים שיש במיליון הכתובות (1, 3, 4...)
    .withColumn("borough_code", 
        when(trim(upper(col("violation_county"))).isin("NY", "MN", "NEW Y", "MANHATTAN"), "1")
        .when(trim(upper(col("violation_county"))).isin("BX", "BRONX"), "2")
        .when(trim(upper(col("violation_county"))).isin("K", "BK", "KINGS", "BKLYN", "BROOKLYN"), "3")
        .when(trim(upper(col("violation_county"))).isin("Q", "QN", "QUEENS", "QU"), "4")
        .when(trim(upper(col("violation_county"))).isin("R", "ST", "RICHMOND", "STATEN ISLAND"), "5")
        .otherwise(None)
    )

    # ב. חילוץ שעה אמיתית מתוך עמודת violation_time (למשל "0409A")
    # התאריך המקורי issue_date לרוב לא מכיל שעה, לכן extract מהזמן של העבירה
    .withColumn("hour", regexp_extract(col("violation_time"), r"^(\d{2})", 1).cast("int"))

    # ג. ניקוי שם הרחוב - הסרת רווחים והפיכה לאותיות גדולות
    .withColumn("street_name", trim(upper(col("street_name"))))
    
    # ד. טיפול בתאריכים
    .withColumn("issue_timestamp", to_timestamp(col("issue_date")))
    .withColumn("issue_date", to_date(col("issue_timestamp")))
    .withColumn("day_of_week", date_format(col("issue_timestamp"), "E"))

    # ה. מניעת כפילויות
    .dropDuplicates(["summons_number", "issue_date"])

    # ו. Data Quality בסיסי
    .filter(col("borough_code").isNotNull()) # אם אין רובע, ה-Join בזהב בטוח ייכשל
    .filter(col("street_name").isNotNull())
)

# 4. הוספת עמודות תאריך לחלוקה (Partitions)
df_cleaned = df_cleaned.withColumn("year", year(col("issue_date"))) \
                       .withColumn("month", month(col("issue_date")))

# 5. התאמה לסכימה (Data Contract)
# שים לב: וודא ש-borough_code ו-hour קיימים ב-silver_traffic_violations ב-nyc_schema.py
existing_columns = df_cleaned.columns
final_selection = []

for field in silver_traffic_violations:
    if field.name in existing_columns:
        final_selection.append(col(field.name))
    else:
        # אם חסר שדה, נשים Null (או נתקן את הסכימה ב-nyc_schema.py)
        final_selection.append(lit(None).cast(field.dataType).alias(field.name))

df_silver_final = df_cleaned.select(*final_selection)

# 6. כתיבה ל-Silver
print("💾 Saving to Silver layer...")

# 6. כתיבה ל-Silver
print("💾 Saving to Silver layer...")

df_silver_final.write \
    .format("parquet") \
    .partitionBy("year", "month") \
    .mode("append") \
    .save("s3a://spark/silver/nyc_traffic_violations/")

print("✅ Silver Process Complete!")