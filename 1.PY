from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    upper,
    trim,
    regexp_replace
)

# ==================================================
# Spark Session
# ==================================================
spark = SparkSession.builder \
    .appName("NYC Join With Address Points") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ==================================================
# Load Kafka Violations
# ==================================================
df_kafka = spark.read.parquet("data/kafka_output")

# ==================================================
# Load NYC Address Points CSV
# ==================================================
df_addresses = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("data/NYC_Address_Points.csv")

# ==================================================
# Clean Kafka Data
# ==================================================
df_kafka_clean = df_kafka \
    .withColumn("house_number_clean",
        upper(trim(col("house_number")))
    ) \
    .withColumn("street_name_clean",
        regexp_replace(
            upper(trim(col("street_name"))),
            "[^A-Z0-9\\- ]",
            ""
        )
    ) \
    .withColumn("borough_clean",
        upper(trim(col("borough")))
    )

# ==================================================
# Clean Address Points Data
# ==================================================
df_addresses_clean = df_addresses \
    .withColumn("house_number_clean",
        upper(trim(col("HOUSE_NUMBER")))
    ) \
    .withColumn("street_name_clean",
        regexp_replace(
            upper(trim(col("STREET_NAME"))),
            "[^A-Z0-9\\- ]",
            ""
        )
    ) \
    .withColumn("borough_clean",
        upper(trim(col("BOROUGH")))
    ) \
    .withColumn("LAT", col("LATITUDE").cast("double")) \
    .withColumn("LON", col("LONGITUDE").cast("double"))

# ==================================================
# Join (House + Street + Borough)
# ==================================================
df_joined = df_kafka_clean.join(
    df_addresses_clean,
    (df_kafka_clean.house_number_clean == df_addresses_clean.house_number_clean) &
    (df_kafka_clean.street_name_clean == df_addresses_clean.street_name_clean) &
    (df_kafka_clean.borough_clean == df_addresses_clean.borough_clean),
    "left"
)

# ==================================================
# Statistics
# ==================================================
total_kafka = df_kafka_clean.count()

matched_rows = df_joined.filter(
    col("LAT").isNotNull() & col("LON").isNotNull()
).count()

match_percent = (matched_rows / total_kafka) * 100 if total_kafka > 0 else 0

print("\n" + "=" * 50)
print("JOIN STATISTICS")
print("=" * 50)
print(f"Total Kafka rows          : {total_kafka}")
print(f"Rows matched to addresses : {matched_rows}")
print(f"Match percentage          : {round(match_percent, 2)}%")

# ==================================================
# Sample Output
# ==================================================
print("\n" + "=" * 50)
print("SAMPLE JOINED DATA")
print("=" * 50)

df_joined.select(
    "summons_number",
    "house_number",
    "street_name",
    "borough",
    "LAT",
    "LON"
).show(20, False)

spark.stop()