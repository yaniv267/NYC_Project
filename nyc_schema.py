from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


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