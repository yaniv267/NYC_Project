from pyspark.sql import SparkSession

KAFKA_BOOTSTRAP_SERVERS = "course-kafka:9093"
KAFKA_TOPIC = "nyc_traffic_violations_bronze"

spark = SparkSession.builder \
    .appName("Kafka_Batch_Test") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"🔄 Attempting to connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS} and read topic '{KAFKA_TOPIC}'...")

try:
    df_test = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()
        
    print("✅ Successfully connected! Here is the data structure:")
    df_test.printSchema()
    
    print("📊 Showing top 5 rows:")
    df_test.selectExpr("CAST(value AS STRING)").show(5, truncate=False)
    
except Exception as e:
    print(f"❌ Failed to read from Kafka. Error:\n{e}")
finally:
    spark.stop()