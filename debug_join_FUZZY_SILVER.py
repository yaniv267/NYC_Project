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
raw = spark.read.parquet("s3a://spark/bronze/nyc_addresses_parquet")

# 3. פונקציות נרמול משופרות (v7)
def super_normalize_v7(column):
    if column is None: return None
    c = upper(trim(column))
    
    # 1. ניקוי צמתים וסימנים מיוחדים (מנקה הכל אחרי הסימן)
    c = regexp_replace(c, r" (@|&|/| AT | CORNER OF | CONER OF ).*$", "")
    
    # 2. ניקוי קידומות כיוון ותיאורי מיקום (גם עם לוכסנים)
    # הוספתי טיפול ב-N/B, S/B וכו'
    c = regexp_replace(c, r"^(NB|SB|EB|WB|N/B|S/B|E/B|W/B|NORTHBOUND|SOUTHBOUND|EASTBOUND|WESTBOUND) ", "")
    c = regexp_replace(c, r"^(N/S OF|S/S OF|E/S OF|W/S OF|R/O|C/O|OPP|FRONT OF|REAR OF|F/O|B/O) ", "")
    
    # 3. מיפוי שמות רחובות ספציפיים (ה-Troublemakers)
    c = regexp_replace(c, r"\b6 AVE\b|\bSIXTH AVE\b", "AVE OF THE AMERICAS")
    c = regexp_replace(c, r"\b7 AVE\b", "SEVENTH AVE")
    c = regexp_replace(c, r"\b8 AVE\b", "EIGHTH AVE")
    c = regexp_replace(c, r"\bADAMS ST\b", "ADAMS ST") # דוגמה לקיבוע
    
    # 4. סטנדרטיזציה של סיומות (בדיוק כמו ב-v7 אבל עם תוספות)
    c = regexp_replace(c, r"\bEXPRESSWAY\b|\bEXPY\b|\bEXP\b", "EXPY")
    c = regexp_replace(c, r"\bROAD\b|\bRD\b", "RD")
    c = regexp_replace(c, r"\bPLACE\b|\bPL\b", "PL")
    c = regexp_replace(c, r"\bSTREET\b|\bSTRE\b|\bSTR\b|\bST\b", "ST")
    c = regexp_replace(c, r"\bAVENUE\b|\bAVEN\b|\bAVE\b", "AVE")
    c = regexp_replace(c, r"\bPARKWAY\b|\bPKWY\b|\bPKY\b|\bPK\b", "PKWY")
    c = regexp_replace(c, r"\bBOULEVARD\b|\bBOULEV\b|\bBLVD\b|\bBLV\b", "BLVD")
    c = regexp_replace(c, r"\bSAINT\b", "ST") # למשל St Nicholas
    
    # 5. קיצורי כיוונים בסיסיים
    c = regexp_replace(c, r"\bEAST\b", "E")
    c = regexp_replace(c, r"\bWEST\b", "W")
    c = regexp_replace(c, r"\bNORTH\b", "N")
    c = regexp_replace(c, r"\bSOUTH\b", "S")
    
    # 6. הורדת סיומות של מספרים (1ST -> 1)
    c = regexp_replace(c, r"(\d+)(ST|ND|RD|TH)", "$1")
    
    # 7. ניקוי תווים מיוחדים ורווחים כפולים
    c = regexp_replace(c, r"[.\'/#]", "")
    return trim(regexp_replace(c, r" +", " "))


def normalize_house_number(column):
    c = regexp_replace(trim(column), "[^0-9]", "")
    return regexp_replace(c, "^0+", "")

# 4. הכנת Silver - כתובות (תיקון שורה 55)
df_addr_silver = df_addresses_raw.select(
    super_normalize_v7(col("full_street_name")).alias("st_name"),
    normalize_house_number(col("house_number")).alias("h_num_addr"),
    when(col("boroughcode") == 1, "MANHATTAN").when(col("boroughcode") == 2, "BRONX")
    .when(col("boroughcode") == 3, "BROOKLYN").when(col("boroughcode") == 4, "QUEENS")
    .when(col("boroughcode") == 5, "STATEN ISLAND").alias("boro_name"),
    regexp_extract(col("the_geom"), r"POINT \(([-\d\.]+) [-\d\.]+\)", 1).cast("double").alias("longitude"),
    regexp_extract(col("the_geom"), r"POINT \([-\d\.]+ ([-\d\.]+)\)", 1).cast("double").alias("latitude")
).distinct().cache()

# 5. הכנת Silver - חניה
df_park_silver = df_kafka_raw.withColumn("st_name_norm", super_normalize_v7(col("street_name"))) \
    .withColumn("h_num_norm", normalize_house_number(col("house_number"))) \
    .withColumn("boro_norm", 
        when(upper(col("violation_county")).isin("K", "BK", "KINGS"), "BROOKLYN")
        .when(upper(col("violation_county")).isin("NY", "MN", "NEW YORK"), "MANHATTAN")
        .when(upper(col("violation_county")).isin("BX", "BRONX"), "BRONX")
        .when(upper(col("violation_county")).isin("Q", "QN", "QUEENS"), "QUEENS")
        .when(upper(col("violation_county")).isin("R", "ST"), "STATEN ISLAND")
        .otherwise("UNKNOWN")) \
    .withColumn("issuer_type", 
        when(col("violation_code").isin("7", "36", "5", "12"), "CAMERA").otherwise("OFFICER")).cache()

# ---------------------------------------------------------
# שלב א': Exact Match
# ---------------------------------------------------------
df_step1_base = df_park_silver.join(
    df_addr_silver,
    (df_park_silver.st_name_norm == df_addr_silver.st_name) & 
    (df_park_silver.h_num_norm == df_addr_silver.h_num_addr) & 
    (df_park_silver.boro_norm == df_addr_silver.boro_name),
    how="left"
).select(
    df_park_silver["*"], "longitude", "latitude",
    col("h_num_addr"), 
    lit(0).alias("house_dist")
).withColumn("match_type", when(col("longitude").isNotNull(), "EXACT").otherwise("NONE"))

df_already_matched = df_step1_base.filter(col("match_type") == "EXACT")
df_still_none_base = df_step1_base.filter(col("match_type") == "NONE").drop("longitude", "latitude", "match_type", "h_num_addr", "house_dist")

# ---------------------------------------------------------
# שלב ב': Fuzzy Join
# ---------------------------------------------------------
df_fuzzy_logic = df_still_none_base.join(
    df_addr_silver,
    (df_still_none_base.st_name_norm == df_addr_silver.st_name) & 
    (df_still_none_base.boro_norm == df_addr_silver.boro_name),
    how="inner"
).withColumn("house_dist", abs(col("h_num_norm").cast("int") - col("h_num_addr").cast("int"))) \
 .filter((col("house_dist") <= 4) & (col("h_num_norm") % 2 == col("h_num_addr") % 2))

window_spec = Window.partitionBy("summons_number").orderBy("house_dist")

df_fuzzy_final = df_fuzzy_logic.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .withColumn("match_type", lit("FUZZY")) \
    .select(*df_still_none_base.columns, "longitude", "latitude", "h_num_addr", "house_dist", "match_type")

# ---------------------------------------------------------
# שלב ג': שאריות (NONE)
# ---------------------------------------------------------
df_completely_failed = df_still_none_base.join(
    df_fuzzy_final.select("summons_number"), on="summons_number", how="left_anti"
).withColumn("longitude", lit(None).cast("double")) \
 .withColumn("latitude", lit(None).cast("double")) \
 .withColumn("h_num_addr", lit(None).cast("string")) \
 .withColumn("house_dist", lit(None).cast("int")) \
 .withColumn("match_type", lit("NONE"))

# ---------------------------------------------------------
# איחוד סופי וסינון למפה
# ---------------------------------------------------------
df_gold_final = df_already_matched.unionByName(df_fuzzy_final).unionByName(df_completely_failed).cache()

# כאן הוספתי את הסינון שביקשת למפה (רק פקחים ורק מי שיש לו מיקום)
df_map_ready = df_gold_final.filter((col("issuer_type") == "OFFICER") & (col("match_type") != "NONE"))

# פלטים לבדיקה
print(f"\nTOTAL ROWS IN GOLD (ALL DATA): {df_gold_final.count()}")
print(f"ROWS FOR MAP (OFFICER ONLY): {df_map_ready.count()}")
df_gold_final.groupBy("match_type").count().show()

# תצוגה של דוגמאות FUZZY
# df_fuzzy_final.select("street_name", "house_number", "h_num_addr", "house_dist").show(10)

# חישוב אחוזי הדיוק עבור פקחים בלבד
df_officer_stats = df_gold_final.filter(col("issuer_type") == "OFFICER") \
    .groupBy("match_type") \
    .count() \
    .withColumn("percentage", (col("count") / df_gold_final.filter(col("issuer_type") == "OFFICER").count()) * 100)

print("\n" + "="*40)
print("OFFICER-ONLY ACCURACY STATS:")
df_officer_stats.show()
print("="*40)

# 5 הרחובות שגורמים להכי הרבה כישלונות אצל פקחים
print("\n" + "!"*40)
print("TOP 10 TROUBLEMAKER STREETS (OFFICER NONE):")
df_gold_final.filter((col("issuer_type") == "OFFICER") & (col("match_type") == "NONE")) \
    .groupBy("street_name") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(10, truncate=False)
print("!"*40)

spark.stop()