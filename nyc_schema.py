
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,TimestampType , DoubleType,DateType, LongType,ArrayType, FloatType


bronze_traffic_violations = StructType([
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

silver_traffic_violations = StructType([
    StructField("summons_number", LongType(), True),       # מספר דוח - כעת כמספר ארוך
    StructField("plate_id", StringType(), True),           # לוחית רישוי
    StructField("registration_state", StringType(), True), # מדינה (NY, NJ וכו')
    StructField("issue_date", DateType(), True),           # תאריך - כעת כטיפוס Date אמיתי
    StructField("violation_code", IntegerType(), True),    # קוד עבירה - כמספר שלם
    StructField("violation_description", StringType(), True), # תיאור (קריטי למצלמות)
    StructField("vehicle_body_type", StringType(), True),  # סוג רכב
    StructField("vehicle_make", StringType(), True),       # יצרן רכב
    StructField("street_name", StringType(), True),        # שם הרחוב (קריטי לבוט)
    StructField("house_number", StringType(), True),       # מספר בית
    StructField("violation_county", StringType(), True),   # מחוז (NY, BX, QN וכו')
    StructField("violation_time", StringType(), True)      # זמן העבירה
])

gold_traffic_violations_schema = StructType([
    StructField("street_name", StringType(), True),
    StructField("violation_code", IntegerType(), True),
    StructField("violation_desc", StringType(), True),
    StructField("tickets_count", LongType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("issue_date", DateType(), True),
    StructField("issue_day_name", StringType(), True),
    StructField("lat", DoubleType(), True), # הכי חשוב
    StructField("lon", DoubleType(), True), # הכי חשוב
    StructField("category", StringType(), True),
    StructField("risk_level", StringType(), True),
    StructField("borough", StringType(), True),
    StructField("last_updated", TimestampType(), True)
])

gold_parking_violation_schema = StructType([
    # מפתח העיר: רחוב וקוד עבירה
    StructField("street_name", StringType(), False),
    StructField("violation_code", IntegerType(), False),
    
    # תיאור מילולי (מהמילון שיצרנו ב-Silver Reference)
    StructField("violation_description", StringType(), True),
    
    # כמות הדוחות שחולקו באותו רחוב/עבירה
    StructField("tickets_count", LongType(), False),
    
    # נתונים גיאוגרפיים (דיוק כפול עבור מפות)
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    
    # חותמת זמן לעדכון אחרון
    StructField("last_updated", TimestampType(), True)
])

gold_camera_violation_schema = StructType([
    StructField("street_name", StringType(), False),
    StructField("violation_description", StringType(), True),
    StructField("tickets_count", LongType(), False),
    # הוספת הקואורדינטות - חובה להשתמש ב-DoubleType לדיוק מרבי
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("last_updated", TimestampType(), True)
])

silver_address_schema = StructType([
    StructField("address_id", StringType(), True),
    StructField("full_address", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("borough_code", StringType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True)
])

silver_violation_codes_schema = StructType([
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

traffic_bronze_schema = StructType([
    StructField("id", StringType(), True),
    StructField("speed", StringType(), True),
    StructField("travel_time", StringType(), True),
    StructField("status", StringType(), True),
    StructField("data_as_of", StringType(), True),
    StructField("link_id", StringType(), True),
    StructField("link_points", StringType(), True),
    StructField("encoded_poly_line", StringType(), True),
    StructField("encoded_poly_line_lvls", StringType(), True),
    StructField("owner", StringType(), True),
    StructField("transcom_id", StringType(), True),
    StructField("borough", StringType(), True),
    StructField("link_name", StringType(), True),
    StructField("ingested_at", StringType(), True)
])

traffic_silver_schema = StructType([
    StructField("id", StringType(), True),
    StructField("speed", FloatType(), True),           # הפך למספר עשרוני
    StructField("travel_time", IntegerType(), True),   # הפך למספר שלם
    StructField("status", StringType(), True),
    StructField("event_time", TimestampType(), True),  # זמן האירוע המקורי (נקי)
    StructField("link_id", StringType(), True),
    StructField("borough", StringType(), True),
    StructField("link_name", StringType(), True),
    StructField("latitude", DoubleType(), True),       # חולץ החוצה
    StructField("longitude", DoubleType(), True),      # חולץ החוצה
    StructField("ingestion_time", TimestampType(), True) # מתי נכנס למערכת שלנו
])

location_schema = StructType([
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("human_address", StringType(), True)
])


crashes_bronze_schema = StructType([
    StructField("crash_date", StringType(), True),
    StructField("crash_time", StringType(), True),
    StructField("borough", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("location", location_schema, True), # שדה מקונן (Nested)
    StructField("on_street_name", StringType(), True),
    StructField("off_street_name", StringType(), True),
    StructField("number_of_persons_injured", StringType(), True),
    StructField("number_of_persons_killed", StringType(), True),
    StructField("number_of_pedestrians_injured", StringType(), True),
    StructField("number_of_pedestrians_killed", StringType(), True),
    StructField("number_of_cyclist_injured", StringType(), True),
    StructField("number_of_cyclist_killed", StringType(), True),
    StructField("number_of_motorist_injured", StringType(), True),
    StructField("number_of_motorist_killed", StringType(), True),
    StructField("contributing_factor_vehicle_1", StringType(), True),
    StructField("contributing_factor_vehicle_2", StringType(), True),
    StructField("collision_id", StringType(), True),
    StructField("vehicle_type_code1", StringType(), True),
    StructField("vehicle_type_code2", StringType(), True),
    StructField("ingested_at", StringType(), True)
])

crashes_silver_schema = StructType([
    StructField("collision_id", StringType(), True),
    StructField("crash_timestamp", TimestampType(), True), # איחוד של תאריך ושעה
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("on_street_name", StringType(), True),
    StructField("total_injured", IntegerType(), True),
    StructField("total_killed", IntegerType(), True),
    StructField("pedestrians_injured", IntegerType(), True),
    StructField("cyclist_injured", IntegerType(), True),
    StructField("motorist_injured", IntegerType(), True),
    StructField("contributing_factor", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("ingestion_time", TimestampType(), True)
])