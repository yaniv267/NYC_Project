from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, trim, regexp_replace, regexp_extract

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

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, trim, regexp_replace, regexp_extract, ltrim

spark = SparkSession.builder.appName("NYC_Gold_Final_Optimization").getOrCreate()

# 1. טעינת נתוני ה-Bronze
df_kafka_raw = spark.read.parquet("s3a://spark/nyc_parking_raw.parquet")
df_addresses_raw = spark.read.parquet("s3a://spark/bronze/nyc_addresses_parquet")


def normalize_street_v5(column):
    c = upper(trim(column))
    # א. ניקוי רעשי מצלמות
    c = regexp_replace(c, " @.*$", "")
    c = regexp_replace(c, "^(NB|SB|EB|WB) ", "")
    
    # ב. נרמול סיומות אגרסיבי (כולל PKY מהדגימה)
    c = regexp_replace(c, " STREET$| STRE$| STR$", " ST")
    c = regexp_replace(c, " AVENUE$| AVEN$| AVE.$", " AVE")
    c = regexp_replace(c, " PARKWAY$| PKWY$| PKY$", " PKWY")
    c = regexp_replace(c, " BOULEVARD$| BOULEV$| BLV$", " BLVD")
    c = regexp_replace(c, " ROAD$| ROA$", " RD")
    c = regexp_replace(c, " EXPRESSWAY$| EXPY$| EXP$", " EXPY")
    
    # ג. טיפול בקידומת AVENUE (למשל AVE V בדגימה)
    c = regexp_replace(c, "^AVENUE ", "AVE ")
    
    # ד. נרמול כיוונים והסרת סיומות מספרים (71ST -> 71)
    c = regexp_replace(c, "\\bEAST\\b", "E")
    c = regexp_replace(c, "\\bWEST\\b", "W")
    c = regexp_replace(c, "(\\d+)(ST|ND|RD|TH)", "$1")
    
    # ה. ניקוי תווים מיוחדים ורווחים כפולים
    c = regexp_replace(c, "[\\.\\'\\/\\#]", "")
    return trim(regexp_replace(c, " +", " "))

# בשלב ה-Silver של החניה, נסנן רשומות ללא מספר בית כי הן לא מיועדות ל-JOIN הזה


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, trim, regexp_replace, regexp_extract

# 1. אתחול Spark
spark = SparkSession.builder.appName("NYC_Gold_Final_Master_Success").getOrCreate()

# 2. טעינת נתונים (Bronze Layer)
df_kafka_raw = spark.read.parquet("s3a://spark/nyc_parking_raw.parquet")
df_addresses_raw = spark.read.parquet("s3a://spark/bronze/nyc_addresses_parquet")

# 3. פונקציות נרמול מתקדמות (V6 - הגרסה הכי אגרסיבית)
def super_normalize_v6(column):
    """ ניקוי וסטנדרטיזציה של שמות רחובות משני צדי ה-JOIN """
    if column is None: return None
    c = upper(trim(column))
    
    # א. ניקוי רעשי מצלמות, צמתים וקידומות מיקום
    c = regexp_replace(c, " @.*$", "")
    c = regexp_replace(c, "^(NB|SB|EB|WB|N/S OF|S/S OF|E/S OF|W/S OF|R/O|C/O|OPP|FRONT OF|REAR OF) ", "")
    
    # ב. נרמול סיומות - כולל חלקיות (BOULEV, PKY) שמופיעות בדוחות ידניים
    c = regexp_replace(c, " STREET$| STRE$| STR$", " ST")
    c = regexp_replace(c, " AVENUE$| AVEN$| AVE.$", " AVE")
    c = regexp_replace(c, " PARKWAY$| PKWY$| PKY$| PK$", " PKWY")
    c = regexp_replace(c, " BOULEVARD$| BOULEV$| BLV$", " BLVD")
    c = regexp_replace(c, " ROAD$| ROA$", " RD")
    c = regexp_replace(c, " DRIVE$| DRI$", " DR")
    
    # ג. נרמול כיוונים מלאים לקיצורים
    c = regexp_replace(c, "\\bEAST\\b", "E")
    c = regexp_replace(c, "\\bWEST\\b", "W")
    c = regexp_replace(c, "\\bNORTH\\b", "N")
    c = regexp_replace(c, "\\bSOUTH\\b", "S")
    
    # ד. הצעד הקריטי: הסרת סיומות מספרים (71ST, 82ND -> 71, 82)
    # זה מאפשר התאמה גם אם ב-DCP רשום STREET ובחניה רשום ST
    c = regexp_replace(c, "(\\d+)(ST|ND|RD|TH)", "$1")
    
    # ה. ניקוי תווים מיוחדים ורווחים כפולים
    c = regexp_replace(c, "[\\.\\'\\/\\#]", "")
    return trim(regexp_replace(c, " +", " "))

def normalize_house_number(column):
    """ השארת ספרות בלבד והסרת אפסים מובילים """
    c = regexp_replace(trim(column), "[^0-9]", "")
    return regexp_replace(c, "^0+", "")

# 4. הכנת Silver - כתובות (נרמול אגרסיבי גם כאן!)
df_addr_silver = df_addresses_raw.select(
    super_normalize_v6(col("full_street_name")).alias("st_name"),
    normalize_house_number(col("house_number")).alias("h_num"),
    when(col("boroughcode") == 1, "MANHATTAN")
    .when(col("boroughcode") == 2, "BRONX")
    .when(col("boroughcode") == 3, "BROOKLYN")
    .when(col("boroughcode") == 4, "QUEENS")
    .when(col("boroughcode") == 5, "STATEN ISLAND")
    .alias("boro_name"),
    regexp_extract(col("the_geom"), r"POINT \(([-\d\.]+) [-\d\.]+\)", 1).cast("double").alias("longitude"),
    regexp_extract(col("the_geom"), r"POINT \([-\d\.]+ ([-\d\.]+)\)", 1).cast("double").alias("latitude")
).distinct()

# 5. הכנת Silver - חניה (קפקא) עם מיפוי רובעים וסינון
df_park_silver = df_kafka_raw.select(
    super_normalize_v6(col("street_name")).alias("st_name"),
    normalize_house_number(col("house_number")).alias("h_num"),
    when(upper(col("violation_county")).isin("K", "BK", "KINGS", "BKLYN"), "BROOKLYN")
    .when(upper(col("violation_county")).isin("NY", "MN", "NEW YORK", "MAN"), "MANHATTAN")
    .when(upper(col("violation_county")).isin("BX", "BRONX", "BRON"), "BRONX")
    .when(upper(col("violation_county")).isin("Q", "QN", "QUEENS", "QNS"), "QUEENS")
    .when(upper(col("violation_county")).isin("R", "ST", "RICHMOND"), "STATEN ISLAND")
    .otherwise("UNKNOWN").alias("boro_name"),
    "summons_number",
    # סיווג דוח: מצלמה (7, 36, 5, 12) מול פקח
    when(col("violation_code").isin("7", "36", "5", "12"), "CAMERA")
    .otherwise("OFFICER").alias("issuer_type")
).filter((col("h_num") != "") & (col("h_num").isNotNull()))

# 6. ביצוע ה-JOIN המאוחד (Left Join לאבחון מלא)
df_gold_debug = df_park_silver.join(
    df_addr_silver,
    on=["st_name", "h_num", "boro_name"],
    how="left"
)

# 7. חישוב מדדי הצלחה מפורטים
df_success = df_gold_debug.filter(col("longitude").isNotNull())
df_clean_boro = df_gold_debug.filter(col("boro_name") != "UNKNOWN")

total_all = df_park_silver.count()
matched_all = df_success.count()
total_clean = df_clean_boro.count()
matched_clean = df_clean_boro.filter(col("longitude").isNotNull()).count()

print("\n" + "="*65)
print(f"OVERALL MATCH RATE: {(matched_all/total_all)*100:.2f}%")
print(f"MATCH RATE (Excluding UNKNOWN Boroughs): {(matched_clean/total_clean)*100:.2f}%")
print("="*65)

# 8. הצגת ה"שאריות" לאבחון סופי
print("\nFAILED MATCHES (Known Boroughs Only):")
df_clean_boro.filter(col("longitude").isNull())\
    .select("st_name", "h_num", "boro_name")\
    .distinct().show(20, truncate=False)

# שמירה ל-MinIO
# df_success.write.mode("overwrite").parquet("s3a://spark/gold/nyc_parking_geocoded")

spark.stop()

# def super_normalize_v6(column):
#     """ ניקוי רחובות אגרסיבי למקסום ה-Match Rate """
#     if column is None: return None
#     c = upper(trim(column))
#     # א. ניקוי רעשי מצלמות וצמתים
#     c = regexp_replace(c, " @.*$", "")
#     c = regexp_replace(c, "^(NB|SB|EB|WB|N/S OF|S/S OF|E/S OF|W/S OF) ", "")
#     # ב. נרמול סיומות (כולל חלקיות וקיצורי פארק מהדגימות)
#     c = regexp_replace(c, " STREET$| STRE$| STR$", " ST")
#     c = regexp_replace(c, " AVENUE$| AVEN$| AVE.$", " AVE")
#     c = regexp_replace(c, " PARKWAY$| PKWY$| PKY$| PK$", " PKWY")
#     c = regexp_replace(c, " BOULEVARD$| BOULEV$| BLV$", " BLVD")
#     c = regexp_replace(c, " ROAD$| ROA$", " RD")
#     c = regexp_replace(c, " DRIVE$| DRI$", " DR")
#     # ג. נרמול כיוונים והסרת סיומות מספרים (71ST -> 71)
#     c = regexp_replace(c, "\\bEAST\\b", "E")
#     c = regexp_replace(c, "\\bWEST\\b", "W")
#     c = regexp_replace(c, "(\\d+)(ST|ND|RD|TH)", "$1")
#     # ד. הסרת תווים מיוחדים ורווחים כפולים
#     c = regexp_replace(c, "[\\.\\'\\/\\#]", "")
#     return trim(regexp_replace(c, " +", " "))

# def normalize_house_number(column):
#     """ השארת ספרות בלבד והסרת אפסים מובילים """
#     c = regexp_replace(trim(column), "[^0-9]", "")
#     return regexp_replace(c, "^0+", "")

# # 3. הכנת Silver - כתובות (DCP Metadata)
# df_addr_silver = df_addresses_raw.select(
#     super_normalize_v6(col("full_street_name")).alias("st_name"),
#     normalize_house_number(col("house_number")).alias("h_num"),
#     when(col("boroughcode") == 1, "MANHATTAN")
#     .when(col("boroughcode") == 2, "BRONX")
#     .when(col("boroughcode") == 3, "BROOKLYN")
#     .when(col("boroughcode") == 4, "QUEENS")
#     .when(col("boroughcode") == 5, "STATEN ISLAND")
#     .alias("boro_name"),
#     regexp_extract(col("the_geom"), r"POINT \(([-\d\.]+) [-\d\.]+\)", 1).cast("double").alias("longitude"),
#     regexp_extract(col("the_geom"), r"POINT \([-\d\.]+ ([-\d\.]+)\)", 1).cast("double").alias("latitude")
# ).distinct()

# # 4. הכנת Silver - חניה (קפקא) עם מיפוי רובעים משופר
# df_park_silver = df_kafka_raw.select(
#     super_normalize_v6(col("street_name")).alias("st_name"),
#     normalize_house_number(col("house_number")).alias("h_num"),
#     # מיפוי מורחב למניעת UNKNOWN על בסיס ה-Metadata והתצפיות
#     when(upper(col("violation_county")).isin("K", "BK", "KINGS", "KING", "BKLYN"), "BROOKLYN")
#     .when(upper(col("violation_county")).isin("NY", "MN", "NEW YORK", "MAN", "NYK"), "MANHATTAN")
#     .when(upper(col("violation_county")).isin("BX", "BRONX", "BRON"), "BRONX")
#     .when(upper(col("violation_county")).isin("Q", "QN", "QUEENS", "QNS", "QNS."), "QUEENS")
#     .when(upper(col("violation_county")).isin("R", "ST", "RICHMOND", "RICH", "STATEN"), "STATEN ISLAND")
#     .otherwise("UNKNOWN").alias("boro_name"),
#     "summons_number"
# ).filter((col("h_num") != "") & (col("h_num").isNotNull()))

# # 5. ביצוע ה-JOIN המאוחד (Inner Join לצורך הזהב הסופי)
# df_gold = df_park_silver.join(
#     df_addr_silver,
#     on=["st_name", "h_num", "boro_name"],
#     how="inner"
# )

# # 6. סטטיסטיקה סופית
# total_filtered = df_park_silver.count()
# matched = df_gold.count()
# print("\n" + "="*60)
# print(f"FINAL STAGE A MATCH RATE: {(matched/total_filtered)*100:.2f}%")
# print("="*60)

# # הצגת דגימה מהתוצאות המוצמדות לקואורדינטות
# df_gold.select("summons_number", "st_name", "h_num", "boro_name", "longitude", "latitude").show(10)

# # 7. שמירה ל-MinIO (זה ה-Gold Layer שלך)
# # df_gold.write.mode("overwrite").parquet("s3a://spark/gold/nyc_parking_geocoded")

# spark.stop()