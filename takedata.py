from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, max as spark_max, min as spark_min, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. יצירת Spark Session
spark = SparkSession.builder \
    .appName("NYC_Validation_Check") \
    .config("spark.jars.packages",
     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()


# 2. סכימה מלאה לכל השדות
schema = StructType([
    StructField("summons_number", StringType(), True),
    StructField("issue_date", StringType(), True),
    StructField("violation_code", StringType(), True),
    StructField("violation_description", StringType(), True),
    StructField("fine_amount", StringType(), True),
    StructField("registration_state", StringType(), True),
    StructField("vehicle_make", StringType(), True),
    StructField("street_name", StringType(), True),
    StructField("plate_id", StringType(), True),
    StructField("state", StringType(), True),
    StructField("vehicle_body_type", StringType(), True),
        # שדות נוספים מה-JSON שלך
    StructField("plate_type", StringType(), True),
    StructField("issuing_agency", StringType(), True),
    StructField("street_code1", StringType(), True),
    StructField("street_code2", StringType(), True),
    StructField("street_code3", StringType(), True),
    StructField("vehicle_expiration_date", StringType(), True),
    StructField("violation_location", StringType(), True),
    StructField("violation_precinct", StringType(), True),
    StructField("issuer_precinct", StringType(), True),
    StructField("issuer_code", StringType(), True),
    StructField("issuer_command", StringType(), True),
    StructField("issuer_squad", StringType(), True),
    StructField("violation_time", StringType(), True),
    StructField("violation_county", StringType(), True),
    StructField("violation_in_front_of_or_opposite", StringType(), True),
    StructField("house_number", StringType(), True),
    StructField("date_first_observed", StringType(), True),
    StructField("law_section", StringType(), True),
    StructField("sub_division", StringType(), True),
    StructField("days_parking_in_effect", StringType(), True),
    StructField("from_hours_in_effect", StringType(), True),
    StructField("to_hours_in_effect", StringType(), True),
    StructField("unregistered_vehicle", IntegerType(), True),
    StructField("vehicle_year", IntegerType(), True),
    StructField("meter_number", StringType(), True),
    StructField("feet_from_curb", IntegerType(), True)
])

# 3. קריאה מקפקא (Batch) – או אם יש לך קובץ JSON, החלף כאן
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9093") \
    .option("subscribe", "nyc_parking_data_last_date") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. המרת value מ־bytes ל־string והחלת סכימה
parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
              .select(from_json(col("json_str"), schema).alias("data")) \
              .select("data.*")

# 5. Sanity Check – מינימום, מקסימום תאריך וספירת רשומות
validation_df = parsed_df.agg(
    spark_min(col("issue_date")).alias("Min_Date"),
    spark_max(col("issue_date")).alias("Max_Date"),
    count("*").alias("Total_Count")
)

# הצגת Sanity Check
validation_df.show(truncate=False)

# 6️⃣ בדיקת כפילויות לפי summons_number
duplicates_df = parsed_df.groupBy("summons_number").count().filter(col("count") > 1)

num_duplicates = duplicates_df.count()
print(f"\n⚠ סה\"כ כפילויות ב-summons_number: {num_duplicates}")

if num_duplicates > 0:
    print("📌 דוגמאות לכפילויות:")
    duplicates_df.show(10, truncate=False)  # מציג 10 דוגמאות בלבד


output_path = "s3a://spark/nyc_parking_raw.parquet"
# 7️⃣ עצירת Spark
spark.stop()
