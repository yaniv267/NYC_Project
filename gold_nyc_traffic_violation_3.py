
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# spark = SparkSession.builder \
#     .appName("gold_nyc_dashboard_final") \
#     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.5.0") \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#     .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .config("spark.sql.shuffle.partitions", "200") \
#     .getOrCreate()

# # print("\n" + "="*50)
# # print("🔍 בדיקת סכמת כתובות (Addresses Silver)")
# # print("="*50)
# # addresses_df = spark.read.parquet("s3a://spark/silver/addresses/")
# # addresses_df.printSchema()

# # print("\n" + "="*50)
# # print("🔍 בדיקת סכמת דוחות (Tickets Silver)")
# # print("="*50)
# # tickets_df = spark.read.parquet("s3a://spark/silver/nyc_traffic_violations/")
# # tickets_df.printSchema()

# # הגדרות Postgres

# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from nyc_schema import silver_violation_codes_schema,gold_traffic_violations_schema
# # ==========================================
# # 1. אתחול Spark Session עם תמיכה ב-S3 ו-Postgres
# # ==========================================
# spark = SparkSession.builder \
#     .appName("gold_nyc_traffic_violation_final_fixed") \
#     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.5.0") \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#     .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .config("spark.sql.shuffle.partitions", "200") \
#     .getOrCreate()

# # 2. מנוע הנרמול - הלב של המערכת
# def normalize_street_expert(col_name):
#     # הופך הכל לאותיות גדולות, מוריד רווחים מיותרים וסימנים
#     c = F.upper(F.trim(F.col(col_name)))
    
#     # שלב א': ניקוי צמתים וקידומות כיוון (WB, EB, NB, SB)
#     c = F.regexp_replace(c, r"^(WB|EB|NB|SB|W\s|E\s|N\s|S\s|WEST\s|EAST\s|NORTH\s|SOUTH\s)", "")
#     c = F.regexp_replace(c, r"\s*@.*$", "") # מסיר כל מה שאחרי @ (צמתים)
    
#     # שלב ב': קיצור סיומות לפורמט אחיד (המכנה המשותף הנמוך ביותר)
#     # אנחנו הופכים הכל לקיצורים קצרים (ST, AVE, RD) כי זה הכי נפוץ בדוחות
#     replacements = {
#         r"\bAVENUE\b": "AVE", r"\bSTREET\b": "ST", r"\bROAD\b": "RD",
#         r"\bPLACE\b": "PL", r"\bBOULEVARD\b": "BLVD", r"\bPARKWAY\b": "PKWY",
#         r"\bEXPRESSWAY\b": "EXPY", r"\bTERRACE\b": "TER", r"\bCOURT\b": "CT"
#     }
#     for full, short in replacements.items():
#         c = F.regexp_replace(c, full, short)
        
#     # שלב ג': טיפול במספרים (3RD -> 3, 1ST -> 1)
#     c = F.regexp_replace(c, r"(\d+)(ST|ND|RD|TH)", r"$1")
    
#     # שלב ד': ניקוי סופי של תווים שהם לא אותיות או מספרים
#     c = F.regexp_replace(c, r"[^A-Z0-9\s]", "")
#     c = F.regexp_replace(c, r"\s+", " ") # צמצום רווחים כפולים לרווח אחד
    
#     return F.trim(c)

# # 3. טעינת נתונים
# addresses_raw = spark.read.parquet("s3a://spark/silver/addresses/")
# tickets_raw = spark.read.parquet("s3a://spark/silver/nyc_traffic_violations/")
# codes_df = spark.read.schema(silver_violation_codes_schema).parquet("s3a://spark/silver/violation_codes")

# # 4. הכנת שכבת הכתובות (נרמול המאגר)
# print("🏗️ Processing Address Reference Map...")
# addresses_clean = addresses_raw.withColumn("street_key", normalize_street_expert("street_name")) \
#     .filter(F.col("street_key").isNotNull()) \
#     .groupBy("street_key", "borough_code") \
#     .agg(
#         F.avg("latitude").alias("lat"), 
#         F.avg("longitude").alias("lon")
#     )

# # 5. הכנת הדוחות (נרמול נתוני אמת)
# print("🏗️ Processing Traffic Tickets...")
# tickets_prepared = tickets_raw.withColumn("borough_code", 
#     F.when(F.col("violation_county").isin("NY", "MN", "NEW Y", "MANHATTAN"), "1")
#      .when(F.col("violation_county").isin("BX", "BRONX"), "2")
#      .when(F.col("violation_county").isin("K", "BK", "KINGS", "BROOKLYN"), "3")
#      .when(F.col("violation_county").isin("Q", "QN", "QUEENS"), "4")
#      .when(F.col("violation_county").isin("R", "ST", "RICHMOND"), "5").otherwise(None)
# ).withColumn("street_key", normalize_street_expert("street_name")) \
#  .filter(F.col("borough_code").isNotNull())

# # 6. ביצוע ה-JOIN וחישוב המטריקה
# print("🔗 Executing High-Precision Join...")
# gold_base = tickets_prepared.join(addresses_clean, ["street_key", "borough_code"], "inner")

# # חישוב אחוזי התאמה
# total_tickets = tickets_prepared.count()
# matched_tickets = gold_base.count()
# match_rate = (matched_tickets / total_tickets) * 100

# print("\n" + "="*50)
# print(f"📊 EXPERT ANALYSIS COMPLETE")
# print(f"🎯 MATCH RATE: {match_rate:.2f}%")
# print(f"✅ Records successfully mapped: {matched_tickets:,} / {total_tickets:,}")
# print("="*50 + "\n")

# # 7. הכנה סופית וכתיבה לפוסטגרס
# gold_final = gold_base.groupBy(
#     "street_name", "violation_code", "borough_code", "lat", "lon", "hour"
# ).agg(
#     F.count("summons_number").alias("tickets_count"),
#     F.max("issue_date").alias("issue_date")
# ).select(
#     "street_name", "violation_code", "tickets_count", "issue_date", "lat", "lon", "hour",
#     F.when(F.col("violation_code").cast("int") < 20, "Parking").otherwise("Moving").alias("category"),
#     F.current_timestamp().alias("last_updated")
# )

# jdbc_url = "jdbc:postgresql://postgres:5432/nyc_data"
# db_props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

# print("💾 Exporting to PostgreSQL...")
# gold_final.write.jdbc(url=jdbc_url, table="gold_traffic_violations", mode="overwrite", properties=db_props)

# print("🏁 Process finished successfully.")
# spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
# ייבוא הסכמות מהקובץ המרכזי שלך
from nyc_schema import silver_violation_codes_schema, gold_traffic_violations_schema

# ==========================================
# 1. אתחול Spark Session
# ==========================================
spark = SparkSession.builder \
    .appName("NYC_Gold_Production_Pipeline") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.5.0") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================
# 2. מנוע הנרמול (מפתח ה-Join)
# ==========================================

def normalize_street_expert(col_name):
    # הופך הכל לאותיות גדולות, מוריד רווחים מיותרים וסימנים
    c = F.upper(F.trim(F.col(col_name)))
    
    # שלב א': ניקוי צמתים וקידומות כיוון (WB, EB, NB, SB)
    c = F.regexp_replace(c, r"^(WB|EB|NB|SB|W\s|E\s|N\s|S\s|WEST\s|EAST\s|NORTH\s|SOUTH\s)", "")
    c = F.regexp_replace(c, r"\s*@.*$", "") # מסיר כל מה שאחרי @ (צמתים)
    
    # שלב ב': קיצור סיומות לפורמט אחיד (המכנה המשותף הנמוך ביותר)
    # אנחנו הופכים הכל לקיצורים קצרים (ST, AVE, RD) כי זה הכי נפוץ בדוחות
    replacements = {
        r"\bAVENUE\b": "AVE", r"\bSTREET\b": "ST", r"\bROAD\b": "RD",
        r"\bPLACE\b": "PL", r"\bBOULEVARD\b": "BLVD", r"\bPARKWAY\b": "PKWY",
        r"\bEXPRESSWAY\b": "EXPY", r"\bTERRACE\b": "TER", r"\bCOURT\b": "CT"
    }
    for full, short in replacements.items():
        c = F.regexp_replace(c, full, short)
        
    # שלב ג': טיפול במספרים (3RD -> 3, 1ST -> 1)
    c = F.regexp_replace(c, r"(\d+)(ST|ND|RD|TH)", r"$1")
    
    # שלב ד': ניקוי סופי של תווים שהם לא אותיות או מספרים
    c = F.regexp_replace(c, r"[^A-Z0-9\s]", "")
    c = F.regexp_replace(c, r"\s+", " ") # צמצום רווחים כפולים לרווח אחד
    
    return F.trim(c)

# ==========================================
# 3. טעינת שכבות הסילבר
# ==========================================
print("📥 Loading Silver layers from MinIO...")
addresses_df = spark.read.parquet("s3a://spark/silver/addresses/")
tickets_df = spark.read.parquet("s3a://spark/silver/nyc_traffic_violations/")
codes_df = spark.read.schema(silver_violation_codes_schema).parquet("s3a://spark/silver/violation_codes")

# ==========================================
# 4. הכנת נתוני ייחוס (Dimensions)
# ==========================================
# נרמול כתובות וצמצום לנקודה ממוצעת אחת לרחוב/רובע
addresses_clean = addresses_df.withColumn("street_key", normalize_street_expert("street_name")) \
    .groupBy("street_key", "borough_code").agg(
        F.avg("latitude").alias("lat"), 
        F.avg("longitude").alias("lon")
    )

# הכנת קודי עבירה (המרה ל-Int ל-Join מהיר)
codes_clean = codes_df.withColumn("violation_code_int", F.col("violation_code").cast("integer")) \
    .select("violation_code_int", "violation_description")

codes_clean = codes_df.withColumn("violation_code_int", F.col("violation_code").cast("integer")) \
    .select(
        "violation_code_int", 
        "violation_description", 
        "manhattan_96th_st_below",  # הבאת קנס מנהטן
        "all_other_areas"           # הבאת קנס שאר האזורים
    )
    
# ==========================================
# 5. הכנת נתוני עובדות (Facts) וזיהוי מצלמות
# ==========================================
tickets_prepared = tickets_df.withColumn("borough_code", 
    F.when(F.col("violation_county").isin("NY", "MN", "NEW Y"), "1")
     .when(F.col("violation_county").isin("BX", "BRONX"), "2")
     .when(F.col("violation_county").isin("K", "BK", "KINGS"), "3")
     .when(F.col("violation_county").isin("Q", "QN", "QUEENS"), "4")
     .when(F.col("violation_county").isin("R", "ST", "RICHM"), "5").otherwise(None)
).withColumn("street_key", normalize_street_expert("street_name")) \
 .withColumn("violation_code_int", F.col("violation_code").cast("integer")) \
 .withColumn("is_camera", F.when(F.col("violation_code_int").isin(7, 12, 36), "Yes").otherwise("No")) \
 .drop("violation_description") # <=== התיקון: מחיקת העמודה הכפולה לפני ה-Join

# ==========================================
# 6. הצלבת נתונים וחישוב Match Rate
# ==========================================
print("🔗 Joining datasets and analyzing quality...")

# Left Join לחישוב אחוזים
match_check = tickets_prepared.join(addresses_clean, ["street_key", "borough_code"], "left")
total_count = tickets_prepared.count()
matched_count = match_check.filter(F.col("lat").isNotNull()).count()

print("\n" + "="*50)
print(f"📊 DATA QUALITY REPORT")
print(f"🎯 FINAL MATCH RATE: {(matched_count/total_count)*100:.2f}%")
print(f"✅ Records mapped: {matched_count:,} / {total_count:,}")
print("="*50 + "\n")

# Inner Join ליצירת שכבת הזהב (רק מה שהתאים גיאוגרפית)
gold_base = tickets_prepared.join(addresses_clean, ["street_key", "borough_code"], "left") \
                            .join(codes_clean, ["violation_code_int"], "left")
# צעד א': קיבוץ בסיסי וספירת הדוחות
gold_agg = gold_base.groupBy(
    "street_name", "violation_code", "violation_description", "borough_code", "lat", "lon", "hour", "is_camera"
).agg(
    F.count("summons_number").alias("tickets_count"),
    F.min("issue_date").alias("start_date"),
    F.max("issue_date").alias("end_date"),
    F.max("issue_timestamp").alias("last_violation_ts"),
    F.sum(F.when(F.col("borough_code") == "1", F.col("manhattan_96th_st_below")).otherwise(F.col("all_other_areas"))).alias("total_fines")
)

# צעד ב': הגדרת "חלון" לחישוב הממוצע העירוני של דוחות באותו רחוב
# (אפשר גם לעשות partitionBy("borough_code") אם תרצה ממוצע רובעי)
street_window = Window.partitionBy("street_name")

# צעד ג': יצירת השדות הסופיים (כולל השוואה לממוצע)
gold_final = gold_agg.withColumn(
    "avg_tickets_for_street", F.avg("tickets_count").over(street_window)
).select(
    "street_name",
    F.col("violation_code").cast("integer").alias("violation_code"),
    F.coalesce(F.col("violation_description"), F.lit("Unknown Violation")).alias("violation_desc"),
    F.col("tickets_count").cast("long"),
    "total_fines",
    "start_date",
    "end_date",
    F.col("end_date").alias("issue_date"),
    F.date_format(F.col("end_date"), "EEEE").alias("issue_day_name"),
    "lat", "lon",
    F.when(F.col("violation_code").cast("int") < 20, "Parking").otherwise("Moving").alias("category"),
    
    # הלוגיקה העסקית שלך: סיכון גבוה = כמות הדוחות גדולה מפי 1.5 מהממוצע ברחוב!
    F.when(F.col("tickets_count") > (1.5 * F.col("avg_tickets_for_street")), "High Risk")
     .otherwise("Low Risk").alias("risk_level"),
    
    F.when(F.col("borough_code") == "1", "Manhattan")
     .when(F.col("borough_code") == "2", "Bronx")
     .when(F.col("borough_code") == "3", "Brooklyn")
     .when(F.col("borough_code") == "4", "Queens")
     .when(F.col("borough_code") == "5", "Staten Island").alias("borough"),
    F.current_timestamp().alias("last_updated"),
    "hour",
    "last_violation_ts",
    "is_camera",
    # חלוקה לזמנים ביום
    F.when((F.col("hour") >= 6) & (F.col("hour") < 12), "Morning")
     .when((F.col("hour") >= 12) & (F.col("hour") < 17), "Afternoon")
     .when((F.col("hour") >= 17) & (F.col("hour") < 21), "Evening")
     .otherwise("Night").alias("time_of_day")
)
# ==========================================
# 8. אכיפת סכימה וכתיבה ל-PostgreSQL
# ==========================================
# בחירת העמודות לפי הסדר והטיפוסים המדויקים בסכימת הזהב
gold_to_export = gold_final.select(*[F.col(field.name) for field in gold_traffic_violations_schema])

jdbc_url = "jdbc:postgresql://postgres:5432/nyc_data"
db_props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

print("💾 Saving Enriched Gold Layer to PostgreSQL...")
gold_to_export.write.jdbc(url=jdbc_url, table="gold_traffic_violations", mode="overwrite", properties=db_props)

print("🏁 Production Pipeline finished successfully.")
spark.stop()