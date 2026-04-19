from pyspark.sql import SparkSession

# 1. יצירת סשן (רזה, רק לצורך קריאה מ-MinIO)
spark = (SparkSession.builder 
    .appName("Check_Silver_Addresses_Data") 
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") 
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") 
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") 
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") 
    .config("spark.hadoop.fs.s3a.path.style.access", "true") 
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# נתיב שכבת הסילבר של הכתובות ב-MinIO
silver_path = "s3a://spark/silver/addresses/"

try:
    print(f"📖 Reading data from MinIO path: {silver_path}...")
    
    # 2. קריאת קבצי ה-Parquet מ-MinIO
    df_addresses = spark.read.parquet(silver_path)
    
    # הצגת כמות רשומות כוללת
    total_records = df_addresses.count()
    print("-" * 40)
    print(f"📊 Total Records in Silver Addresses Layer: {total_records}")
    print("-" * 40)

    # הצגת מדגם קטן
    print("🔍 Sample Data (Top 5 rows):")
    df_addresses.show(5, truncate=False)
    
except Exception as e:
    print(f"❌ Error reading data from MinIO: {e}")

finally:
    spark.stop()