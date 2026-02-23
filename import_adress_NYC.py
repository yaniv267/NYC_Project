import os
import geopandas as gpd
import pandas as pd
from pyspark.sql import SparkSession

# =========================
# הגדרות S3
# =========================
s3_path = "s3a://spark/data/dims/"

# =========================
# יצירת SparkSession עם יותר זיכרון
# =========================
spark = SparkSession.builder \
    .appName("NYC_Address_Join") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.jars.packages",
     "org.apache.hadoop:hadoop-aws:3.3.4,"
     "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .getOrCreate()

# =========================
# נתיב לתיקייה עם קבצי SHP
# =========================
data_dir = "/home/developer/projects/spark-course-python/spark-kafka-project/NYC_OpenData_Project/Address_Point_20250505"
shp_files = [f for f in os.listdir(data_dir) if f.endswith(".shp")]

# =========================
# פונקציה לטעינת batch וחילוץ LAT/LON
# =========================
def process_shp_file(shp_file):
    path = os.path.join(data_dir, shp_file)
    gdf = gpd.read_file(path)
    
    # חילוץ LAT/LON
    gdf['LAT'] = gdf.geometry.y
    gdf['LON'] = gdf.geometry.x
    
    # הסרת העמודה geometry
    pdf = gdf.drop(columns='geometry')
    
    # הסרת כפילויות
    pdf = pdf.drop_duplicates(subset='ADDRESS_ID')
    
    return pdf

# =========================
# קריאה והמרה ל-Spark DataFrame ב-batch
# =========================
spark_dfs = []
for shp in shp_files:
    try:
        pdf = process_shp_file(shp)
        sdf = spark.createDataFrame(pdf)
        spark_dfs.append(sdf)
        print(f"Processed {shp} ({len(pdf)} rows)")
    except Exception as e:
        print(f"Failed to process {shp}: {e}")

# =========================
# איחוד כל ה-Spark DataFrames
# =========================
if spark_dfs:
    full_spark_df = spark_dfs[0]
    for sdf in spark_dfs[1:]:
        full_spark_df = full_spark_df.unionByName(sdf)
    
    # אופציונלי: cache אם נצטרך להשתמש שוב ושוב
    full_spark_df.cache()
    
    # הצגת 5 שורות לדוגמה
    full_spark_df.show(5)
# המרה של עמודות התאריך למחרוזת כדי למנוע בעיות בכתיבה
for col_name in ['CREATED', 'MODIFIED']:
    full_spark_df = full_spark_df.withColumn(col_name, full_spark_df[col_name].cast("string"))
    # שמירה ל-S3 כ-Parquet
    full_spark_df.repartition(50).write.mode("overwrite").parquet(s3_path + "dim_address")
    print("Saved to S3:", s3_path + "dim_address/")
else:
    print("No data to process.")