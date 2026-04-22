from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, split, min, max, year, month, hour, date_format, dayofmonth, to_date, when
from nyc_schema import silver_traffic_schema

#export PYTHONPATH=$PYTHONPATH:.

spark = (SparkSession.builder
         .appName("NYC_Traffic_Silver_Processing")
         .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
         .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
         .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

input_path = "s3a://spark/bronze/nyc_traffic/"
df_bronze = spark.read.parquet(input_path)

df_silver = (df_bronze
             .filter((col("status") != "-101") & (col("speed").cast("float") > 0))
             .dropDuplicates(["link_id", "data_as_of"])
             .withColumnRenamed("ingested_at", "ingestion_time")
             .withColumn("event_time", to_timestamp(col("data_as_of"), "yyyy-MM-dd'T'HH:mm:ss.000"))
             .withColumn("borough", col("borough"))
             .withColumn("year", year(col("event_time")))
             .withColumn("month", month(col("event_time")))
             .withColumn("day", dayofmonth(col("event_time")))
             .withColumn("hour", hour(col("event_time")))
             .withColumn("day_name", date_format(col("event_time"), "EEEE"))
             .withColumn("date_id", to_date(col("event_time")))
             .withColumn("is_weekend", when(col("day_name").isin("Saturday", "Sunday"), True).otherwise(False))

             .withColumn("speed", col("speed").cast("float"))
             .withColumn("travel_time", col("travel_time").cast("int"))
             .withColumn("first_coords", split(col("link_points"), " ")[0])
             .withColumn("latitude", split(col("first_coords"), ",")[0].cast("double"))
             .withColumn("longitude", split(col("first_coords"), ",")[1].cast("double"))
             )

final_columns = [field.name for field in silver_traffic_schema]
df_silver_final = df_silver.select(*final_columns)

print(f"📊 Total observations in Silver Layer: {df_silver_final.count()}")
df_silver_final.select(min("event_time"), max("event_time")).show()

output_path = "s3a://spark/silver/nyc_traffic/"
(df_silver_final.write
 .mode("overwrite")

 .partitionBy("year", "month", "day")
 .parquet(output_path))

print(f"✨ Silver layer for Traffic complete! Saved to: {output_path}")
df_silver_final.show(5, truncate=False)