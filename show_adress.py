from pyspark.sql import SparkSession

# 1. אתחול Spark Session לצורך קריאה בלבד
spark = SparkSession.builder \
    .appName("view_dim_adress") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 2. נתיב הקובץ ב-MinIO
path = "s3a://spark/data/dims/dim_adress"

print(f"--- טוען נתונים מ-MinIO לצורך תצוגה ---")

try:
    # קריאת קבצי ה-Parquet
    df = spark.read.parquet(path)
    df.printSchema()
    # הצגת 20 שורות ראשונות
    # truncate=False מאפשר לראות את כל תוכן הכתובת בלי שהיא תיחתך
    df.show(20, truncate=False)
    
    # הדפסת כמות השורות הכוללת שיש כרגע בקובץ
    print(f"סה''כ שורות בטבלה: {df.count():,}")

except Exception as e:
    print(f"❌ שגיאה: לא הצלחתי לקרוא את הנתונים. וודא שהנתיב קיים ושיש בו קבצים. שגיאה: {e}")

spark.stop()