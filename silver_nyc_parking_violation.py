from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, to_date, trim, lit, year, month, add_months, current_date
from nyc_schema import silver_parking_schema
# 1. יצירת Spark Session
spark = SparkSession.builder \
.appName("NYC_Parking_Silver") \
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
df_bronze = spark.read.parquet("s3a://spark/bronze/nyc_parking_violation/")

# 3. תהליך הניקוי + ניהול היסטוריה
# נגדיר תאריך סף (למשל: נשמור רק נתונים מ-24 החודשים האחרונים)
retention_limit = add_months(current_date(), -24)

df_cleaned = (df_bronze
    .dropDuplicates(["summons_number"]) # מניעת כפילויות
    .withColumn("issue_date", to_date(col("issue_date"), "yyyy-MM-dd"))
    # סינון היסטוריה: רק מה שחדש מתאריך הסף
    .filter(col("issue_date") >= retention_limit)
    .withColumn("street_name", trim(upper(col("street_name"))))
    .withColumn("violation_code", col("violation_code").cast("int"))
    # הוספת עמודות לפרטישנים חכמים (שנה וחודש)
    .withColumn("year", year(col("issue_date")))
    .withColumn("month", month(col("issue_date")))
    .filter(col("street_name").isNotNull() & col("issue_date").isNotNull())
)

# 4. התאמה לסכימה (הלוגיקה החכמה שלך)
existing_columns = df_cleaned.columns
final_selection = []
for field in silver_parking_schema:
    if field.name in existing_columns:
        final_selection.append(col(field.name))
    else:
        final_selection.append(lit(None).cast(field.dataType).alias(field.name))

# הוספת עמודות הפרטישן לבחירה הסופית
final_selection.extend([col("year"), col("month")])
df_silver_final = df_cleaned.select(*final_selection)

# 5. כתיבה חכמה ל-Silver
print("💾 Saving to Silver layer with Year/Month partitioning...")
(df_silver_final.write 
    .mode("overwrite") # כרגע overwrite, אבל בזכות ה-filter בצעד 3, היסטוריה ישנה תתנקה
    .partitionBy("year", "month") 
    .parquet("s3a://spark/silver/nyc_parking_violation/"))

print("✅ Silver Process Complete! Old history (>24 months) was filtered out.")