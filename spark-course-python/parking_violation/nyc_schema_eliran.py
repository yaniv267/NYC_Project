from pyspark.sql.types import StructType, StructField, StringType, IntegerType,TimestampType , DoubleType,DateType, LongType,ArrayType, FloatType

bronze_address_schema=StructType([
    StructField("the_geom", StringType(), True),                  # קואורדינטות בפורמט POINT (WKT)
    StructField("BIN", IntegerType(), True),                      # Building Identification Number (מזהה בניין ייחודי בעירייה)
    StructField("ZIPCODE", IntegerType(), True),                  # מיקוד
    StructField("PRE_TYPE", StringType(), True),                  # קידומת סוג (נדיר)
    StructField("POST_TYPE", StringType(), True),                 # סיומת סוג (כמו ST, AVE, BLVD)
    StructField("OBJECTID", IntegerType(), True),                 # מזהה אובייקט במערכת ה-GIS
    StructField("Address Point ID", IntegerType(), True),         # מזהה הכתובת הייחודי (Primary Key)
    StructField("Complex ID", IntegerType(), True),               # מזהה למתחמים גדולים (כמו קמפוסים)
    StructField("House Number", StringType(), True),              # מספר הבית (מחרוזת כי יש מספרים כמו 10A)
    StructField("House Number Suffix", StringType(), True),       # סיומת למספר בית (כמו 1/2)
    StructField("Hyphen Type", StringType(), True),               # סוג מקף במספר הבית (למשל בקווינס)
    StructField("SOS Indicator", IntegerType(), True),            # אינדיקטור פנימי של העירייה
    StructField("Special Condition", StringType(), True),         # תנאים מיוחדים (כמו כתובת וירטואלית)
    StructField("Address Source", IntegerType(), True),           # מקור הכתובת
    StructField("Address Status", IntegerType(), True),           # סטטוס (פעיל, היסטורי וכו')
    StructField("Validation", IntegerType(), True),               # רמת אימות הכתובת
    StructField("Borough Code", IntegerType(), True),             # קוד הרובע (1=מנהטן, 2=ברונקס, 3=ברוקלין, 4=קווינס, 5=סטטן איילנד)
    StructField("Collection Method", StringType(), True),         # איך הכתובת נאספה (GPS, ידני וכו')
    StructField("CREATED_DATE", StringType(), True),              # תאריך יצירת הרשומה
    StructField("MODIFIED_DATE", StringType(), True),             # תאריך עדכון אחרון
    StructField("B7SC_ACTUAL", IntegerType(), True),              # קוד רחוב רשמי (Department of City Planning)
    StructField("B7SC_VANITY", IntegerType(), True),              # קוד רחוב חלופי/מיוחד
    StructField("A4ID", IntegerType(), True),                     # מזהה פנימי נוסף
    StructField("Street Name", StringType(), True),               # שם הרחוב (ללא סיומת)
    StructField("House Number Range", StringType(), True),        # טווח מספרי בתים
    StructField("House Number Range Suffix", StringType(), True), # סיומת לטווח
    StructField("Pre-Modifier", StringType(), True),              # משנה קידומת (כמו UPPER, LOWER)
    StructField("Pre-Directional", StringType(), True),           # כיוון בקידומת (N, S, E, W)
    StructField("Post Directional", StringType(), True),          # כיוון בסיומת
    StructField("Post Modifier", StringType(), True),             # משנה סיומת
    StructField("Full Street Name", StringType(), True)           # השם המלא של הרחוב (העמודה שבה אנחנו משתמשים)
])


silver_address_schema = StructType([
    StructField("address_id", StringType(), True),
    StructField("street_name", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("borough_code", StringType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True)
])

bronze_parking_schema = StructType([
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


silver_parking_schema = StructType([
    StructField("summons_number", LongType(), True),
    StructField("plate_id", StringType(), True),
    StructField("registration_state", StringType(), True),
    StructField("issue_timestamp", TimestampType(), True),
    StructField("issue_date", DateType(), True),
    StructField("violation_code", IntegerType(), True),
    StructField("violation_description", StringType(), True),
    StructField("vehicle_body_type", StringType(), True),
    StructField("vehicle_make", StringType(), True),
    StructField("street_name", StringType(), True),
    StructField("house_number", StringType(), True),
    StructField("violation_county", StringType(), True),
    StructField("violation_time", StringType(), True),
    StructField("hour", IntegerType(), True),
    StructField("day_of_week", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("borough_code", StringType(), True)
])


gold_parking_violation_schema = StructType([
    StructField("street_name", StringType(), True),
    StructField("violation_code", IntegerType(), True),
    StructField("violation_desc", StringType(), True),
    StructField("tickets_count", LongType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("issue_date", DateType(), True),
    StructField("issue_day_name", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("category", StringType(), True),
    StructField("risk_level", StringType(), True),
    StructField("borough", StringType(), True),
    StructField("last_updated", TimestampType(), True),
    StructField("hour", IntegerType(), True),
    StructField("last_violation_ts", TimestampType(), True),
    StructField("is_camera", StringType(), True), # שדה חדש עבור הבוט
    StructField("time_of_day", StringType(), True),
    StructField("total_fines", DoubleType(), True)
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
    StructField("street_name", StringType(), True),
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


bronze_crashes_schema = StructType([
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

silver_crashes_schema = StructType([
    StructField("collision_id", StringType(), True),
    StructField("crash_timestamp", TimestampType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day_of_week", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("on_street_name", StringType(), True),
    StructField("borough", StringType(), True),
    StructField("total_injured", IntegerType(), True),
    StructField("total_killed", IntegerType(), True),
    StructField("pedestrians_injured", IntegerType(), True),
    StructField("cyclist_injured", IntegerType(), True),
    StructField("motorist_injured", IntegerType(), True),
    StructField("contributing_factor", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("ingestion_time", TimestampType(), True)
])


gold_crashes_schema = StructType([
    StructField("street_name", StringType(), True),
    StructField("borough", StringType(), True),
    StructField("total_crashes", LongType(), True),
    StructField("total_injured", LongType(), True),
    StructField("total_killed", LongType(), True),
    StructField("main_cause", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("crash_pct", DoubleType(), True),
    StructField("danger_rank", IntegerType(), True),
    StructField("safety_label", StringType(), True),
    StructField("first_crash_date", TimestampType(), True),
    StructField("last_crash_date", TimestampType(), True),
    StructField("unique_crash_days", LongType(), True),
    StructField("sample_day_of_week", StringType(), True),
    StructField("last_updated", TimestampType(), True)
])