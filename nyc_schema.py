from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType , DoubleType, ArrayType, IntegerType


parking_schema = StructType([
    StructField("summons_number", StringType(), True),
    StructField("plate_id", StringType(), True),
    StructField("registration_state", StringType(), True),
    StructField("plate_type", StringType(), True),
    StructField("issue_date", TimestampType(), True),
    StructField("violation_code", StringType(), True),
    StructField("vehicle_body_type", StringType(), True),
    StructField("vehicle_make", StringType(), True),
    StructField("issuing_agency", StringType(), True),
    StructField("street_name", StringType(), True),
    StructField("intersecting_street", StringType(), True),
    StructField("violation_precinct", StringType(), True),
    StructField("issuer_precinct", StringType(), True),
    StructField("violation_time", StringType(), True),
    StructField("violation_county", StringType(), True),
    StructField("vehicle_color", StringType(), True),
    StructField("vehicle_year", IntegerType(), True),
    StructField("violation_description", StringType(), True),
    StructField("fiscal_year", IntegerType(), True)
])

address_schema = StructType([
    StructField("address_id", StringType(), True),
    StructField("full_address", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("borough_code", StringType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True)
])

parking_violation_codes_schema = StructType([
    StructField("violation_code", StringType(), True),
    StructField("violation_description", StringType(), True),
    StructField("manhattan_96th_st_below", DoubleType(), True),
    StructField("all_other_areas", DoubleType(), True)
])

weather_bronze_schema = StructType([
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timezone", StringType(), True),
    StructField("hourly", StructType([
        StructField("time", ArrayType(StringType()), True),
        StructField("temperature_2m", ArrayType(DoubleType()), True),
        StructField("precipitation", ArrayType(DoubleType()), True),
        StructField("snowfall", ArrayType(DoubleType()), True)
    ]), True)
])


weather_silver_schema = StructType([
    StructField("weather_hour", StringType(), False), # המפתח ל-Join (למשל 2026-03-28 14:00)
    StructField("temp", DoubleType(), True),
    StructField("rain", DoubleType(), True),
    StructField("snow", DoubleType(), True)
])