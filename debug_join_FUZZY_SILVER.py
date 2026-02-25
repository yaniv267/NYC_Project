from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, trim, regexp_replace, regexp_extract, abs, row_number, lit
from pyspark.sql.window import Window
# 1. אתחול Spark

spark = SparkSession.builder \
    .appName("NYC_Parking_Gold_Join") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 2. טעינת נתונים (Bronze Layer)
df_kafka_raw = spark.read.parquet("s3a://spark/nyc_parking_raw.parquet")
df_addresses_raw = spark.read.parquet("s3a://spark/bronze/nyc_addresses_parquet")

# 3. פונקציות נרמול (v6)
def super_normalize_v6(column):
    if column is None: return None
    c = upper(trim(column))
    c = regexp_replace(c, " @.*$", "")
    c = regexp_replace(c, "^(NB|SB|EB|WB|N/S OF|S/S OF|E/S OF|W/S OF|R/O|C/O|OPP|FRONT OF|REAR OF) ", "")
    c = regexp_replace(c, " STREET$| STRE$| STR$", " ST")
    c = regexp_replace(c, " AVENUE$| AVEN$| AVE.$", " AVE")
    c = regexp_replace(c, " PARKWAY$| PKWY$| PKY$| PK$", " PKWY")
    c = regexp_replace(c, " BOULEVARD$| BOULEV$| BLV$", " BLVD")
    c = regexp_replace(c, "\\bEAST\\b", "E")
    c = regexp_replace(c, "\\bWEST\\b", "W")
    c = regexp_replace(c, "(\\d+)(ST|ND|RD|TH)", "$1")
    c = regexp_replace(c, "[\\.\\'\\/\\#]", "")
    return trim(regexp_replace(c, " +", " "))

def normalize_house_number(column):
    c = regexp_replace(trim(column), "[^0-9]", "")
    return regexp_replace(c, "^0+", "")

# 4. הכנת Silver - כתובות
df_addr_silver = df_addresses_raw.select(
    super_normalize_v6(col("full_street_name")).alias("st_name"),
    normalize_house_number(col("house_number")).alias("h_num_addr"),
    when(col("boroughcode") == 1, "MANHATTAN").when(col("boroughcode") == 2, "BRONX")
    .when(col("boroughcode") == 3, "BROOKLYN").when(col("boroughcode") == 4, "QUEENS")
    .when(col("boroughcode") == 5, "STATEN ISLAND").alias("boro_name"),
    regexp_extract(col("the_geom"), r"POINT \(([-\d\.]+) [-\d\.]+\)", 1).cast("double").alias("longitude"),
    regexp_extract(col("the_geom"), r"POINT \([-\d\.]+ ([-\d\.]+)\)", 1).cast("double").alias("latitude")
).distinct().cache()

# 5. הכנת Silver - חניה (שימור כל הנתונים)
# אנחנו משתמשים בעמודות המקוריות ובונים עליהן נרמול לחיבור
df_park_silver = df_kafka_raw.withColumn("st_name_norm", super_normalize_v6(col("street_name"))) \
    .withColumn("h_num_norm", normalize_house_number(col("house_number"))) \
    .withColumn("boro_norm", 
        when(upper(col("violation_county")).isin("K", "BK", "KINGS"), "BROOKLYN")
        .when(upper(col("violation_county")).isin("NY", "MN", "NEW YORK"), "MANHATTAN")
        .when(upper(col("violation_county")).isin("BX", "BRONX"), "BRONX")
        .when(upper(col("violation_county")).isin("Q", "QN", "QUEENS"), "QUEENS")
        .when(upper(col("violation_county")).isin("R", "ST"), "STATEN ISLAND")
        .otherwise("UNKNOWN")) \
    .withColumn("issuer_type", 
        when(col("violation_code").isin("7", "36", "5", "12"), "CAMERA").otherwise("OFFICER"))

# ---------------------------------------------------------
# שלב א': Exact Match (Left Join)
# ---------------------------------------------------------
df_step1 = df_park_silver.join(
    df_addr_silver,
    (df_park_silver.st_name_norm == df_addr_silver.st_name) & 
    (df_park_silver.h_num_norm == df_addr_silver.h_num_addr) & 
    (df_park_silver.boro_norm == df_addr_silver.boro_name),
    how="left"
).select(df_park_silver["*"], "longitude", "latitude") \
 .withColumn("match_type", when(col("longitude").isNotNull(), "EXACT").otherwise("NONE"))

# ---------------------------------------------------------
# שלב ב': Fuzzy Join (רק לשאריות)
# ---------------------------------------------------------
df_still_none = df_step1.filter(col("match_type") == "NONE").drop("longitude", "latitude", "match_type")
df_already_matched = df_step1.filter(col("match_type") == "EXACT")

df_fuzzy_logic = df_still_none.join(
    df_addr_silver,
    (df_still_none.st_name_norm == df_addr_silver.st_name) & 
    (df_still_none.boro_norm == df_addr_silver.boro_name),
    how="inner"
).withColumn("house_dist", abs(col("h_num_norm").cast("int") - col("h_num_addr").cast("int"))) \
 .filter((col("house_dist") <= 2) & (col("h_num_norm") % 2 == col("h_num_addr") % 2))

window_spec = Window.partitionBy("summons_number").orderBy("house_dist")

df_fuzzy_final = df_fuzzy_logic.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .withColumn("match_type", lit("FUZZY")) \
    .select(*df_still_none.columns, "longitude", "latitude", "match_type")

# ---------------------------------------------------------
# שלב ג': איחוד סופי (10,000 שורות)
# ---------------------------------------------------------
df_completely_failed = df_step1.filter(col("match_type") == "NONE").join(
    df_fuzzy_final.select("summons_number"), on="summons_number", how="left_anti"
)

df_gold_final = df_already_matched.unionByName(df_fuzzy_final) \
                                  .unionByName(df_completely_failed)

df_gold_final.select(
    "street_name", "st_name_norm", 
    "house_number", "h_num_norm", 
    "match_type"
).distinct().show(20, truncate=False)

# פלט לסיכום ואבחון
print("\n" + "="*60)
print(f"TOTAL ROWS IN GOLD: {df_gold_final.count()}") # וידוא 10,000
print("MATCH TYPE STATISTICS:")
df_gold_final.groupBy("match_type").count().orderBy("count", ascending=False).show()
print("="*60)

# הצגת דגימה של ה-NONE לאבחון
print("\nSAMPLE OF FAILED MATCHES (NONE):")
df_gold_final.filter(col("match_type") == "NONE") \
    .select("street_name", "house_number", "violation_county", "issuer_type") \
    .distinct() \
    .show(10, truncate=False)
    
spark.stop()