
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType,TimestampType , DoubleType,DateType,BooleanType , LongType,ArrayType, FloatType


# # bronze_traffic_violations = StructType([
# #     StructField("summons_number", StringType(), True),
# #     StructField("plate_id", StringType(), True),
# #     StructField("registration_state", StringType(), True),
# #     StructField("plate_type", StringType(), True),
# #     StructField("issue_date", TimestampType(), True),
# #     StructField("violation_code", StringType(), True),
# #     StructField("vehicle_body_type", StringType(), True),
# #     StructField("vehicle_make", StringType(), True),
# #     StructField("issuing_agency", StringType(), True),
# #     StructField("street_name", StringType(), True),
# #     StructField("intersecting_street", StringType(), True),
# #     StructField("violation_precinct", StringType(), True),
# #     StructField("issuer_precinct", StringType(), True),
# #     StructField("violation_time", StringType(), True),
# #     StructField("violation_county", StringType(), True),
# #     StructField("vehicle_color", StringType(), True),
# #     StructField("vehicle_year", IntegerType(), True),
# #     StructField("violation_description", StringType(), True),
# #     StructField("fiscal_year", IntegerType(), True)
# # ])

# bronze_parking_schema  = StructType([
#     StructField("summons_number", StringType(), True),           # Unique identifier for the summons/ticket
#     StructField("plate_id", StringType(), True),                # License plate identifier of the vehicle
#     StructField("registration_state", StringType(), True),      # State where the vehicle is registered (e.g., NY, NJ)
#     StructField("plate_type", StringType(), True),              # Type of plate (e.g., PAS - Passenger, COM - Commercial)
#     StructField("issue_date", TimestampType(), True),           # Date the violation was issued
#     StructField("violation_code", StringType(), True),          # Numeric code representing the specific violation
#     StructField("vehicle_body_type", StringType(), True),       # Vehicle body style (e.g., SUBN, SEDN, VAN)
#     StructField("vehicle_make", StringType(), True),            # Manufacturer of the vehicle (e.g., FORD, HONDA)
#     StructField("issuing_agency", StringType(), True),          # Agency that issued the ticket (e.g., P - Police, T - Traffic)
#     StructField("street_name", StringType(), True),             # Street name where the violation occurred
#     StructField("intersecting_street", StringType(), True),     # Nearest cross street to the violation location
#     StructField("violation_precinct", StringType(), True),      # Police precinct where the violation took place
#     StructField("issuer_precinct", StringType(), True),         # Precinct of the officer who issued the summons
#     StructField("violation_time", StringType(), True),          # Time of day when the violation occurred
#     StructField("violation_county", StringType(), True),        # County code (e.g., NY, BX, K, Q, R)
#     StructField("vehicle_color", StringType(), True),           # Exterior color of the vehicle
#     StructField("vehicle_year", IntegerType(), True),           # Manufacturing year of the vehicle
#     StructField("violation_description", StringType(), True),   # Text description of the violation
#     StructField("fiscal_year", IntegerType(), True)             # The NYC fiscal year associated with the record
# ])

# silver_parking_violations = StructType([
#     StructField("summons_number", LongType(), True),
#     StructField("plate_id", StringType(), True),
#     StructField("registration_state", StringType(), True),
#     StructField("issue_timestamp", TimestampType(), True),
#     StructField("issue_date", DateType(), True),
#     StructField("violation_code", IntegerType(), True),
#     StructField("violation_description", StringType(), True),
#     StructField("vehicle_body_type", StringType(), True),
#     StructField("vehicle_make", StringType(), True),
#     StructField("street_name", StringType(), True),
#     StructField("house_number", StringType(), True),
#     StructField("violation_county", StringType(), True),
#     StructField("violation_time", StringType(), True),
#     StructField("hour", IntegerType(), True),
#     StructField("day_of_week", StringType(), True),
#     StructField("year", IntegerType(), True),
#     StructField("month", IntegerType(), True),
#     StructField("borough_code", StringType(), True)
# ])


# # gold_traffic_violations_schema = StructType([
# #     StructField("street_name", StringType(), True),
# #     StructField("violation_code", IntegerType(), True),
# #     StructField("violation_desc", StringType(), True),
# #     StructField("tickets_count", LongType(), True),
# #     StructField("start_date", DateType(), True),
# #     StructField("end_date", DateType(), True),
# #     StructField("issue_date", DateType(), True),
# #     StructField("issue_day_name", StringType(), True),
# #     StructField("lat", DoubleType(), True),
# #     StructField("lon", DoubleType(), True),
# #     StructField("category", StringType(), True),
# #     StructField("risk_level", StringType(), True),
# #     StructField("borough", StringType(), True),
# #     StructField("last_updated", TimestampType(), True),
# #     StructField("hour", IntegerType(), True),
# #     StructField("last_violation_ts", TimestampType(), True)
# # ])

# gold_parking_violations_schema = StructType([
#     StructField("street_name", StringType(), True),
#     StructField("violation_code", IntegerType(), True),
#     StructField("violation_desc", StringType(), True),
#     StructField("tickets_count", LongType(), True),
#     StructField("start_date", DateType(), True),
#     StructField("end_date", DateType(), True),
#     StructField("issue_date", DateType(), True),
#     StructField("issue_day_name", StringType(), True),
#     StructField("lat", DoubleType(), True),
#     StructField("lon", DoubleType(), True),
#     StructField("category", StringType(), True),
#     StructField("risk_level", StringType(), True),
#     StructField("borough", StringType(), True),
#     StructField("last_updated", TimestampType(), True),
#     StructField("hour", IntegerType(), True),
#     StructField("last_violation_ts", TimestampType(), True),
#     StructField("is_camera", StringType(), True), # שדה חדש עבור הבוט
#     StructField("time_of_day", StringType(), True),
#     StructField("total_fines", DoubleType(), True)
# ])


# location_schema = StructType([
#     StructField("type", StringType(), True),
#     StructField("coordinates", ArrayType(DoubleType()), True)
# ])

# # bronze_311_schema = StructType([
# #     StructField("unique_key", StringType(), True),
# #     StructField("created_date", StringType(), True), # נשמור כסטרינג בברונז, נהפוך ל-Timestamp בסילבר
# #     StructField("agency", StringType(), True),
# #     StructField("agency_name", StringType(), True),
# #     StructField("complaint_type", StringType(), True),
# #     StructField("descriptor", StringType(), True),
# #     StructField("location_type", StringType(), True),
# #     StructField("incident_zip", StringType(), True),
# #     StructField("incident_address", StringType(), True),
# #     StructField("street_name", StringType(), True),
# #     StructField("cross_street_1", StringType(), True),
# #     StructField("cross_street_2", StringType(), True),
# #     StructField("intersection_street_1", StringType(), True),
# #     StructField("intersection_street_2", StringType(), True),
# #     StructField("address_type", StringType(), True),
# #     StructField("city", StringType(), True),
# #     StructField("landmark", StringType(), True),
# #     StructField("status", StringType(), True),
# #     StructField("community_board", StringType(), True),
# #     StructField("council_district", StringType(), True),
# #     StructField("police_precinct", StringType(), True),
# #     StructField("bbl", StringType(), True),
# #     StructField("borough", StringType(), True),
# #     StructField("x_coordinate_state_plane", StringType(), True),
# #     StructField("y_coordinate_state_plane", StringType(), True),
# #     StructField("open_data_channel_type", StringType(), True),
# #     StructField("park_facility_name", StringType(), True),
# #     StructField("park_borough", StringType(), True),
# #     StructField("latitude", StringType(), True), # מגיע בגרשיים ב-JSON, נמיר ל-Double בסילבר
# #     StructField("longitude", StringType(), True),
# #     StructField("location", location_schema, True) # שימוש בסכמה המקוננת שהגדרנו למעלה
# # ])

# bronze_311_schema = StructType([
#     StructField("unique_key", StringType(), True),                # Unique identifier for each service request
#     StructField("created_date", StringType(), True),              # Raw date string (Convert to Timestamp in Silver)
#     StructField("agency", StringType(), True),                    # Agency acronym (e.g., NYPD, DOT, DSNY)
#     StructField("agency_name", StringType(), True),               # Full name of the responding agency
#     StructField("complaint_type", StringType(), True),            # Category of the complaint (e.g., Noise, Illegal Parking)
#     StructField("descriptor", StringType(), True),                # Specific detail of the complaint type
#     StructField("location_type", StringType(), True),             # Type of location (e.g., Residential, Street, Park)
#     StructField("incident_zip", StringType(), True),              # Incident postal code
#     StructField("incident_address", StringType(), True),          # Full house number and street name
#     StructField("street_name", StringType(), True),               # Name of the street where incident occurred
#     StructField("cross_street_1", StringType(), True),            # First nearest cross street
#     StructField("cross_street_2", StringType(), True),            # Second nearest cross street
#     StructField("intersection_street_1", StringType(), True),     # First street of the intersection (if applicable)
#     StructField("intersection_street_2", StringType(), True),     # Second street of the intersection (if applicable)
#     StructField("address_type", StringType(), True),              # Format of the address (e.g., ADDRESS, INTERSECTION)
#     StructField("city", StringType(), True),                      # City name (typically NYC boroughs)
#     StructField("landmark", StringType(), True),                  # Well-known nearby landmark
#     StructField("status", StringType(), True),                    # Current status of the request (e.g., Open, Closed)
#     StructField("community_board", StringType(), True),           # NYC Community Board identifier
#     StructField("council_district", StringType(), True),          # NYC City Council District
#     StructField("police_precinct", StringType(), True),           # NYPD Precinct responsible for the area
#     StructField("bbl", StringType(), True),                       # Borough, Block, and Lot (Property identifier)
#     StructField("borough", StringType(), True),                    # Name of the borough
#     StructField("x_coordinate_state_plane", StringType(), True),  # X coordinate in NY State Plane format
#     StructField("y_coordinate_state_plane", StringType(), True),  # Y coordinate in NY State Plane format
#     StructField("open_data_channel_type", StringType(), True),    # Submission source (e.g., Web, Phone, Mobile)
#     StructField("park_facility_name", StringType(), True),        # Name of the park/facility if applicable
#     StructField("park_borough", StringType(), True),              # Borough where the park is located
#     StructField("latitude", StringType(), True),                  # Latitude (Convert to Double in Silver)
#     StructField("longitude", StringType(), True),                 # Longitude (Convert to Double in Silver)
#     StructField("location", location_schema, True)                # Nested geometry structure (Schema defined above)
# ])

# silver_311_schema = StructType([
#     StructField("unique_key", StringType(), True),
#     StructField("created_date", TimestampType(), True),
#     StructField("agency", StringType(), True),
#     StructField("complaint_type", StringType(), True),
#     StructField("descriptor", StringType(), True),
#     StructField("city", StringType(), True),
#     StructField("borough", StringType(), True),
#     StructField("latitude", DoubleType(), True),
#     StructField("longitude", DoubleType(), True),
#     StructField("incident_zip", StringType(), True),
#     StructField("year", IntegerType(), True),
#     StructField("month", IntegerType(), True),
#     StructField("hour", IntegerType(), True),
#     StructField("day_name", StringType(), True),
#     StructField("street_name", StringType(), True)
# ])


# gold_311_schema = StructType([
#     StructField("borough", StringType()),
#     StructField("complaint_type", StringType()),
#     StructField("year", IntegerType()),
#     StructField("month", IntegerType()),
#     StructField("day_name", StringType()),
#     StructField("complaint_count", LongType()),
#     StructField("latitude", DoubleType()),
#     StructField("longitude", DoubleType()),
#     StructField("peak_hour", IntegerType()),
#     StructField("last_updated_at", TimestampType()),
#     StructField("first_complaint_date", TimestampType()),
#     StructField("last_complaint_date", TimestampType()),
#     StructField("street_name", StringType(), True)
# ])

# # bronze_address_schema = StructType([
# #     StructField("the_geom", StringType(), True),                  # קואורדינטות בפורמט POINT (WKT)
# #     StructField("BIN", IntegerType(), True),                      # Building Identification Number (מזהה בניין ייחודי בעירייה)
# #     StructField("ZIPCODE", IntegerType(), True),                  # מיקוד
# #     StructField("PRE_TYPE", StringType(), True),                  # קידומת סוג (נדיר)
# #     StructField("POST_TYPE", StringType(), True),                 # סיומת סוג (כמו ST, AVE, BLVD)
# #     StructField("OBJECTID", IntegerType(), True),                 # מזהה אובייקט במערכת ה-GIS
# #     StructField("Address Point ID", IntegerType(), True),         # מזהה הכתובת הייחודי (Primary Key)
# #     StructField("Complex ID", IntegerType(), True),               # מזהה למתחמים גדולים (כמו קמפוסים)
# #     StructField("House Number", StringType(), True),              # מספר הבית (מחרוזת כי יש מספרים כמו 10A)
# #     StructField("House Number Suffix", StringType(), True),       # סיומת למספר בית (כמו 1/2)
# #     StructField("Hyphen Type", StringType(), True),               # סוג מקף במספר הבית (למשל בקווינס)
# #     StructField("SOS Indicator", IntegerType(), True),            # אינדיקטור פנימי של העירייה
# #     StructField("Special Condition", StringType(), True),         # תנאים מיוחדים (כמו כתובת וירטואלית)
# #     StructField("Address Source", IntegerType(), True),           # מקור הכתובת
# #     StructField("Address Status", IntegerType(), True),           # סטטוס (פעיל, היסטורי וכו')
# #     StructField("Validation", IntegerType(), True),               # רמת אימות הכתובת
# #     StructField("Borough Code", IntegerType(), True),             # קוד הרובע (1=מנהטן, 2=ברונקס, 3=ברוקלין, 4=קווינס, 5=סטטן איילנד)
# #     StructField("Collection Method", StringType(), True),         # איך הכתובת נאספה (GPS, ידני וכו')
# #     StructField("CREATED_DATE", StringType(), True),              # תאריך יצירת הרשומה
# #     StructField("MODIFIED_DATE", StringType(), True),             # תאריך עדכון אחרון
# #     StructField("B7SC_ACTUAL", IntegerType(), True),              # קוד רחוב רשמי (Department of City Planning)
# #     StructField("B7SC_VANITY", IntegerType(), True),              # קוד רחוב חלופי/מיוחד
# #     StructField("A4ID", IntegerType(), True),                     # מזהה פנימי נוסף
# #     StructField("Street Name", StringType(), True),               # שם הרחוב (ללא סיומת)
# #     StructField("House Number Range", StringType(), True),        # טווח מספרי בתים
# #     StructField("House Number Range Suffix", StringType(), True), # סיומת לטווח
# #     StructField("Pre-Modifier", StringType(), True),              # משנה קידומת (כמו UPPER, LOWER)
# #     StructField("Pre-Directional", StringType(), True),           # כיוון בקידומת (N, S, E, W)
# #     StructField("Post Directional", StringType(), True),          # כיוון בסיומת
# #     StructField("Post Modifier", StringType(), True),             # משנה סיומת
# #     StructField("Full Street Name", StringType(), True)           # השם המלא של הרחוב (העמודה שבה אנחנו משתמשים)
# # ])

# bronze_address_schema = StructType([
#     StructField("the_geom", StringType(), True),                  # Coordinates in POINT (WKT) format
#     StructField("BIN", IntegerType(), True),                       # Building Identification Number (Unique NYC building ID)
#     StructField("ZIPCODE", IntegerType(), True),                   # Postal code
#     StructField("PRE_TYPE", StringType(), True),                   # Street type prefix (rare)
#     StructField("POST_TYPE", StringType(), True),                  # Street type suffix (e.g., ST, AVE, BLVD)
#     StructField("OBJECTID", IntegerType(), True),                  # GIS system object identifier
#     StructField("Address Point ID", IntegerType(), True),          # Unique address identifier (Primary Key)
#     StructField("Complex ID", IntegerType(), True),                # Identifier for large complexes (e.g., campuses)
#     StructField("House Number", StringType(), True),               # House number (String to support alphanumeric like 10A)
#     StructField("House Number Suffix", StringType(), True),        # House number suffix (e.g., 1/2)
#     StructField("Hyphen Type", StringType(), True),                # House number hyphen type (common in Queens)
#     StructField("SOS Indicator", IntegerType(), True),             # Internal city indicator
#     StructField("Special Condition", StringType(), True),          # Special conditions (e.g., virtual addresses)
#     StructField("Address Source", IntegerType(), True),            # Source of the address data
#     StructField("Address Status", IntegerType(), True),            # Status (Active, Historical, etc.)
#     StructField("Validation", IntegerType(), True),                # Address validation level
#     StructField("Borough Code", IntegerType(), True),              # Borough code (1=MN, 2=BX, 3=BK, 4=QN, 5=SI)
#     StructField("Collection Method", StringType(), True),          # Data collection method (GPS, Manual, etc.)
#     StructField("CREATED_DATE", StringType(), True),               # Record creation date
#     StructField("MODIFIED_DATE", StringType(), True),              # Last modification date
#     StructField("B7SC_ACTUAL", IntegerType(), True),               # Official street code (Dept. of City Planning)
#     StructField("B7SC_VANITY", IntegerType(), True),               # Vanity/Alternative street code
#     StructField("A4ID", IntegerType(), True),                      # Additional internal identifier
#     StructField("Street Name", StringType(), True),                # Street name (without type suffix)
#     StructField("House Number Range", StringType(), True),         # Range of house numbers
#     StructField("House Number Range Suffix", StringType(), True),  # Range suffix
#     StructField("Pre-Modifier", StringType(), True),               # Prefix modifier (e.g., UPPER, LOWER)
#     StructField("Pre-Directional", StringType(), True),            # Prefix directional (N, S, E, W)
#     StructField("Post Directional", StringType(), True),           # Suffix directional (N, S, E, W)
#     StructField("Post Modifier", StringType(), True),              # Suffix modifier
#     StructField("Full Street Name", StringType(), True)            # Full street name (Main column used for analysis)
# ])

# silver_address_schema = StructType([
#     StructField("address_id", StringType(), True),
#     StructField("street_name", StringType(), True),
#     StructField("zip_code", StringType(), True),
#     StructField("borough_code", StringType(), True),
#     StructField("longitude", DoubleType(), True),
#     StructField("latitude", DoubleType(), True)
# ])

# silver_violation_codes_schema = StructType([
#     StructField("violation_code", StringType(), True),
#     StructField("violation_description", StringType(), True),
#     StructField("manhattan_96th_st_below", DoubleType(), True),
#     StructField("all_other_areas", DoubleType(), True)
# ])

# # weather_bronze_schema = StructType([
# #     StructField("latitude", DoubleType(), True),
# #     StructField("longitude", DoubleType(), True),
# #     StructField("timezone", StringType(), True),
# #     StructField("hourly", StructType([
# #     StructField("time", ArrayType(StringType()), True),
# #     StructField("temperature_2m", ArrayType(DoubleType()), True),
# #     StructField("precipitation", ArrayType(DoubleType()), True),
# #     StructField("snowfall", ArrayType(DoubleType()), True)
# #     ]), True)
# # ])


# # weather_silver_schema = StructType([
# #     StructField("weather_hour", StringType(), False), # המפתח ל-Join (למשל 2026-03-28 14:00)
# #     StructField("temp", DoubleType(), True),
# #     StructField("rain", DoubleType(), True),
# #     StructField("snow", DoubleType(), True)
# # ])

# bronze_traffic_schema = StructType([
#     StructField("id", StringType(), True),
#     StructField("speed", StringType(), True),
#     StructField("travel_time", StringType(), True),
#     StructField("status", StringType(), True),
#     StructField("data_as_of", StringType(), True),
#     StructField("link_id", StringType(), True),
#     StructField("link_points", StringType(), True),
#     StructField("encoded_poly_line", StringType(), True),
#     StructField("encoded_poly_line_lvls", StringType(), True),
#     StructField("owner", StringType(), True),
#     StructField("transcom_id", StringType(), True),
#     StructField("borough", StringType(), True),
#     StructField("link_name", StringType(), True),
#     StructField("ingested_at", StringType(), True)
# ])

# silver_traffic_schema = StructType([
#     StructField("link_id", StringType(), False),
#     StructField("link_name", StringType(), True),
#     StructField("borough", StringType(), True), # <--- הוספנו
#     StructField("speed", FloatType(), True),
#     StructField("travel_time", IntegerType(), True),
#     StructField("latitude", DoubleType(), True),
#     StructField("longitude", DoubleType(), True),
#     StructField("event_time", TimestampType(), True),
#     StructField("day", IntegerType(), True),
#     StructField("hour", IntegerType(), True),
#     StructField("day_name", StringType(), True),
#     StructField("date_id", StringType(), True),
#     StructField("is_weekend", BooleanType(), True),
#     StructField("year", IntegerType(), True),
#     StructField("month", IntegerType(), True)
# ])


# gold_traffic_realtime_schema = StructType([
#     StructField("link_id", StringType(), False),
#     StructField("street_name", StringType(), True),
#     StructField("borough", StringType(), True), # <--- קיים
#     StructField("latitude", DoubleType(), True),
#     StructField("longitude", DoubleType(), True),
#     StructField("current_speed", FloatType(), True),
#     StructField("travel_time", IntegerType(), True),
#     StructField("traffic_status", StringType(), True),
#     StructField("event_time", TimestampType(), True),
#     StructField("last_updated", TimestampType(), True)
# ])


# gold_traffic_analytics_schema = StructType([
#     StructField("link_id", StringType(), False),
#     StructField("street_name", StringType(), True),
#     StructField("borough", StringType(), True), # <--- קיים
#     StructField("day_name", StringType(), True),
#     StructField("is_weekend", BooleanType(), True),
#     StructField("avg_speed_daily", FloatType(), True),
#     StructField("peak_congestion_hour", IntegerType(), True),
#     StructField("reliability_score", StringType(), True),
#     StructField("total_readings", IntegerType(), True),
#     StructField("last_updated", TimestampType(), True)
# ])

# bronze_crashes_schema = StructType([
#     StructField("crash_date", StringType(), True),
#     StructField("crash_time", StringType(), True),
#     StructField("borough", StringType(), True),
#     StructField("zip_code", StringType(), True),
#     StructField("latitude", StringType(), True),
#     StructField("longitude", StringType(), True),
#     StructField("location", location_schema, True), # שדה מקונן (Nested)
#     StructField("on_street_name", StringType(), True),
#     StructField("off_street_name", StringType(), True),
#     StructField("number_of_persons_injured", StringType(), True),
#     StructField("number_of_persons_killed", StringType(), True),
#     StructField("number_of_pedestrians_injured", StringType(), True),
#     StructField("number_of_pedestrians_killed", StringType(), True),
#     StructField("number_of_cyclist_injured", StringType(), True),
#     StructField("number_of_cyclist_killed", StringType(), True),
#     StructField("number_of_motorist_injured", StringType(), True),
#     StructField("number_of_motorist_killed", StringType(), True),
#     StructField("contributing_factor_vehicle_1", StringType(), True),
#     StructField("contributing_factor_vehicle_2", StringType(), True),
#     StructField("collision_id", StringType(), True),
#     StructField("vehicle_type_code1", StringType(), True),
#     StructField("vehicle_type_code2", StringType(), True),
#     StructField("ingested_at", StringType(), True)
# ])

# silver_crashes_schema = StructType([
#     StructField("collision_id", StringType(), True),
#     StructField("crash_timestamp", TimestampType(), True),
#     StructField("year", IntegerType(), True),
#     StructField("month", IntegerType(), True),
#     StructField("day_of_week", StringType(), True),
#     StructField("latitude", DoubleType(), True),
#     StructField("longitude", DoubleType(), True),
#     StructField("on_street_name", StringType(), True),
#     StructField("borough", StringType(), True),
#     StructField("total_injured", IntegerType(), True),
#     StructField("total_killed", IntegerType(), True),
#     StructField("pedestrians_injured", IntegerType(), True),
#     StructField("cyclist_injured", IntegerType(), True),
#     StructField("motorist_injured", IntegerType(), True),
#     StructField("contributing_factor", StringType(), True),
#     StructField("vehicle_type", StringType(), True),
#     StructField("ingestion_time", TimestampType(), True)
# ])



# gold_crashes_schema = StructType([
#     StructField("street_name", StringType(), True),
#     StructField("borough", StringType(), True),
#     StructField("total_crashes", LongType(), True),
#     StructField("total_injured", LongType(), True),
#     StructField("total_killed", LongType(), True),
#     StructField("main_cause", StringType(), True),
#     StructField("latitude", DoubleType(), True),
#     StructField("longitude", DoubleType(), True),
#     StructField("crash_pct", DoubleType(), True),
#     StructField("danger_rank", IntegerType(), True),
#     StructField("safety_label", StringType(), True),
#     StructField("first_crash_date", TimestampType(), True),
#     StructField("last_crash_date", TimestampType(), True),
#     StructField("unique_crash_days", LongType(), True),
#     StructField("sample_day_of_week", StringType(), True),
#     StructField("last_updated", TimestampType(), True)
# ])

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType , DoubleType, DateType, BooleanType , LongType, ArrayType, FloatType

# ==========================================
# PARKING VIOLATIONS SCHEMAS
# ==========================================

bronze_parking_schema = StructType([
    StructField("summons_number", StringType(), True),           # Unique identifier for the summons/ticket
    StructField("plate_id", StringType(), True),                # License plate identifier of the vehicle
    StructField("registration_state", StringType(), True),      # State where the vehicle is registered
    StructField("plate_type", StringType(), True),              # Type of plate (e.g., PAS, COM)
    StructField("issue_date", TimestampType(), True),           # Date the violation was issued
    StructField("violation_code", StringType(), True),          # Numeric code representing the specific violation
    StructField("vehicle_body_type", StringType(), True),       # Vehicle body style
    StructField("vehicle_make", StringType(), True),            # Manufacturer of the vehicle
    StructField("issuing_agency", StringType(), True),          # Agency that issued the ticket
    StructField("street_name", StringType(), True),             # Street name where the violation occurred
    StructField("intersecting_street", StringType(), True),     # Nearest cross street
    StructField("violation_precinct", StringType(), True),      # Police precinct of the violation
    StructField("issuer_precinct", StringType(), True),         # Precinct of the issuing officer
    StructField("violation_time", StringType(), True),          # Time of day of the violation
    StructField("violation_county", StringType(), True),        # County code (e.g., NY, BX, K, Q, R)
    StructField("vehicle_color", StringType(), True),           # Exterior color of the vehicle
    StructField("vehicle_year", IntegerType(), True),           # Manufacturing year of the vehicle
    StructField("violation_description", StringType(), True),   # Text description of the violation
    StructField("fiscal_year", IntegerType(), True)             # NYC fiscal year of the record
])

silver_parking_violations = StructType([
    StructField("summons_number", LongType(), True),            # Summon ID cast to Long
    StructField("plate_id", StringType(), True),                # Cleaned plate ID
    StructField("registration_state", StringType(), True),      # Standardized state code
    StructField("issue_timestamp", TimestampType(), True),      # Combined date and time of issue
    StructField("issue_date", DateType(), True),                # Extracted date for partitioning
    StructField("violation_code", IntegerType(), True),         # Violation code cast to Integer
    StructField("violation_description", StringType(), True),   # Cleaned violation description
    StructField("vehicle_body_type", StringType(), True),       # Standardized body type
    StructField("vehicle_make", StringType(), True),            # Standardized vehicle make
    StructField("street_name", StringType(), True),             # Normalized street name
    StructField("house_number", StringType(), True),            # Extracted house number
    StructField("violation_county", StringType(), True),        # Standardized county name
    StructField("violation_time", StringType(), True),          # Formatted violation time
    StructField("hour", IntegerType(), True),                   # Extracted hour (0-23)
    StructField("day_of_week", StringType(), True),             # Name of the day
    StructField("year", IntegerType(), True),                   # Extracted year
    StructField("month", IntegerType(), True),                  # Extracted month
    StructField("borough_code", StringType(), True)             # Borough code mapped from county
])

gold_parking_violations_schema = StructType([
    StructField("street_name", StringType(), True),             # Aggregation key: Street name
    StructField("violation_code", IntegerType(), True),         # Violation code
    StructField("violation_desc", StringType(), True),          # Description
    StructField("tickets_count", LongType(), True),             # Total count of tickets
    StructField("start_date", DateType(), True),                # Earliest date in group
    StructField("end_date", DateType(), True),                  # Latest date in group
    StructField("issue_date", DateType(), True),                # Reference issue date
    StructField("issue_day_name", StringType(), True),          # Weekday name
    StructField("lat", DoubleType(), True),                     # Latitude
    StructField("lon", DoubleType(), True),                     # Longitude
    StructField("category", StringType(), True),                # Violation category
    StructField("risk_level", StringType(), True),              # Calculated safety risk level
    StructField("borough", StringType(), True),                 # Borough name
    StructField("last_updated", TimestampType(), True),         # Record update timestamp
    StructField("hour", IntegerType(), True),                   # Peak hour
    StructField("last_violation_ts", TimestampType(), True),    # Most recent violation timestamp
    StructField("is_camera", StringType(), True),               # Flag for camera-issued violations
    StructField("time_of_day", StringType(), True),             # Time category (e.g., Morning)
    StructField("total_fines", DoubleType(), True)              # Calculated total fine amounts
])

# ==========================================
# 311 SERVICE REQUESTS SCHEMAS
# ==========================================

location_schema = StructType([
    StructField("type", StringType(), True),                    # Geometry type
    StructField("coordinates", ArrayType(DoubleType()), True)   # Longitude and Latitude array
])

bronze_311_schema = StructType([
    StructField("unique_key", StringType(), True),              # Unique identifier for the request
    StructField("created_date", StringType(), True),            # Raw creation date string
    StructField("agency", StringType(), True),                  # Agency acronym
    StructField("agency_name", StringType(), True),             # Full agency name
    StructField("complaint_type", StringType(), True),          # Category of complaint
    StructField("descriptor", StringType(), True),              # Specific complaint detail
    StructField("location_type", StringType(), True),           # Incident location type
    StructField("incident_zip", StringType(), True),            # Incident postal code
    StructField("incident_address", StringType(), True),        # Incident address
    StructField("street_name", StringType(), True),             # Street name
    StructField("cross_street_1", StringType(), True),          # Nearest cross street 1
    StructField("cross_street_2", StringType(), True),          # Nearest cross street 2
    StructField("intersection_street_1", StringType(), True),   # Intersection street 1
    StructField("intersection_street_2", StringType(), True),   # Intersection street 2
    StructField("address_type", StringType(), True),            # Address format
    StructField("city", StringType(), True),                    # City name
    StructField("landmark", StringType(), True),                # Nearby landmark
    StructField("status", StringType(), True),                  # Request status
    StructField("community_board", StringType(), True),         # Community board identifier
    StructField("council_district", StringType(), True),        # Council district
    StructField("police_precinct", StringType(), True),         # Responsible precinct
    StructField("bbl", StringType(), True),                     # Borough, Block, Lot
    StructField("borough", StringType(), True),                 # Borough name
    StructField("x_coordinate_state_plane", StringType(), True),# State plane X coordinate
    StructField("y_coordinate_state_plane", StringType(), True),# State plane Y coordinate
    StructField("open_data_channel_type", StringType(), True),  # Submission source
    StructField("park_facility_name", StringType(), True),      # Park facility name
    StructField("park_borough", StringType(), True),            # Park borough
    StructField("latitude", StringType(), True),                # Latitude string
    StructField("longitude", StringType(), True),               # Longitude string
    StructField("location", location_schema, True)              # Nested location structure
])

silver_311_schema = StructType([
    StructField("unique_key", StringType(), True),              # Request ID
    StructField("created_date", TimestampType(), True),         # Parsed timestamp
    StructField("agency", StringType(), True),                  # Agency code
    StructField("complaint_type", StringType(), True),          # Standardized complaint type
    StructField("descriptor", StringType(), True),              # Normalized descriptor
    StructField("city", StringType(), True),                    # City name
    StructField("borough", StringType(), True),                 # Borough name
    StructField("latitude", DoubleType(), True),                # Latitude cast to Double
    StructField("longitude", DoubleType(), True),               # Longitude cast to Double
    StructField("incident_zip", StringType(), True),            # Standardized Zip
    StructField("year", IntegerType(), True),                   # Partition year
    StructField("month", IntegerType(), True),                  # Partition month
    StructField("hour", IntegerType(), True),                   # Incident hour
    StructField("day_name", StringType(), True),                # Name of day
    StructField("street_name", StringType(), True)              # Street name
])

gold_311_schema = StructType([
    StructField("borough", StringType()),                       # Borough aggregation key
    StructField("complaint_type", StringType()),                # Complaint type key
    StructField("year", IntegerType()),                         # Year
    StructField("month", IntegerType()),                        # Month
    StructField("day_name", StringType()),                      # Weekday
    StructField("complaint_count", LongType()),                 # Total complaints count
    StructField("latitude", DoubleType()),                      # Group latitude
    StructField("longitude", DoubleType()),                     # Group longitude
    StructField("peak_hour", IntegerType()),                    # Hour with highest complaints
    StructField("last_updated_at", TimestampType()),            # Update timestamp
    StructField("first_complaint_date", TimestampType()),       # Group start date
    StructField("last_complaint_date", TimestampType()),        # Group end date
    StructField("street_name", StringType(), True)              # Target street name
])

# ==========================================
# MASTER ADDRESS & VIOLATION REFERENCE
# ==========================================

bronze_address_schema = StructType([
    StructField("the_geom", StringType(), True),                  # Coordinates in POINT (WKT) format
    StructField("BIN", IntegerType(), True),                       # Building Identification Number
    StructField("ZIPCODE", IntegerType(), True),                   # Postal code
    StructField("PRE_TYPE", StringType(), True),                   # Street type prefix
    StructField("POST_TYPE", StringType(), True),                  # Street type suffix
    StructField("OBJECTID", IntegerType(), True),                  # GIS system identifier
    StructField("Address Point ID", IntegerType(), True),          # Unique address identifier
    StructField("Complex ID", IntegerType(), True),                # Complex identifier
    StructField("House Number", StringType(), True),               # House number string
    StructField("House Number Suffix", StringType(), True),        # House number suffix
    StructField("Hyphen Type", StringType(), True),                # Hyphen indicator
    StructField("SOS Indicator", IntegerType(), True),             # Internal city indicator
    StructField("Special Condition", StringType(), True),          # Special conditions
    StructField("Address Source", IntegerType(), True),            # Source identifier
    StructField("Address Status", IntegerType(), True),            # Record status
    StructField("Validation", IntegerType(), True),                # Validation level
    StructField("Borough Code", IntegerType(), True),              # Borough code (1-5)
    StructField("Collection Method", StringType(), True),          # Data collection method
    StructField("CREATED_DATE", StringType(), True),               # Creation date
    StructField("MODIFIED_DATE", StringType(), True),              # Last modification date
    StructField("B7SC_ACTUAL", IntegerType(), True),               # Official street code
    StructField("B7SC_VANITY", IntegerType(), True),               # Vanity street code
    StructField("A4ID", IntegerType(), True),                      # Internal ID
    StructField("Street Name", StringType(), True),                # Street name
    StructField("House Number Range", StringType(), True),         # Range of numbers
    StructField("House Number Range Suffix", StringType(), True),  # Range suffix
    StructField("Pre-Modifier", StringType(), True),               # Prefix modifier
    StructField("Pre-Directional", StringType(), True),            # Prefix direction
    StructField("Post Directional", StringType(), True),           # Suffix direction
    StructField("Post Modifier", StringType(), True),              # Suffix modifier
    StructField("Full Street Name", StringType(), True)            # Full street name
])

silver_address_schema = StructType([
    StructField("address_id", StringType(), True),              # Normalized Address ID
    StructField("street_name", StringType(), True),             # Cleaned street name
    StructField("zip_code", StringType(), True),                # Zip code
    StructField("borough_code", StringType(), True),            # Standardized borough code
    StructField("longitude", DoubleType(), True),               # Extracted longitude
    StructField("latitude", DoubleType(), True)                 # Extracted latitude
])

silver_violation_codes_schema = StructType([
    StructField("violation_code", StringType(), True),          # Violation code
    StructField("violation_description", StringType(), True),   # Fine description
    StructField("manhattan_96th_st_below", DoubleType(), True), # Fine in high-density Manhattan
    StructField("all_other_areas", DoubleType(), True)          # Fine in other areas
])

# ==========================================
# TRAFFIC SPEED SCHEMAS
# ==========================================

bronze_traffic_schema = StructType([
    StructField("id", StringType(), True),                       # Link unique ID
    StructField("speed", StringType(), True),                    # Average speed in MPH
    StructField("travel_time", StringType(), True),              # Travel time in seconds
    StructField("status", StringType(), True),                   # Link operational status
    StructField("data_as_of", StringType(), True),               # Data timestamp
    StructField("link_id", StringType(), True),                  # roadway segment ID
    StructField("link_points", StringType(), True),              # Geometry points
    StructField("encoded_poly_line", StringType(), True),        # Encoded polyline for maps
    StructField("encoded_poly_line_lvls", StringType(), True),   # Polyline levels
    StructField("owner", StringType(), True),                    # Link owner/agency
    StructField("transcom_id", StringType(), True),              # System ID
    StructField("borough", StringType(), True),                  # Borough location
    StructField("link_name", StringType(), True),                # Name of segment
    StructField("ingested_at", StringType(), True)               # Pipeline ingestion time
])

silver_traffic_schema = StructType([
    StructField("link_id", StringType(), False),                # Primary link ID
    StructField("link_name", StringType(), True),               # Road name
    StructField("borough", StringType(), True),                 # Standardized borough
    StructField("speed", FloatType(), True),                    # Speed cast to Float
    StructField("travel_time", IntegerType(), True),            # Travel time cast to Int
    StructField("latitude", DoubleType(), True),                # Segment latitude
    StructField("longitude", DoubleType(), True),               # Segment longitude
    StructField("event_time", TimestampType(), True),           # Event timestamp
    StructField("day", IntegerType(), True),                    # Day of month
    StructField("hour", IntegerType(), True),                   # Hour
    StructField("day_name", StringType(), True),                # Day name
    StructField("date_id", StringType(), True),                 # Unique date ID
    StructField("is_weekend", BooleanType(), True),             # Weekend flag
    StructField("year", IntegerType(), True),                   # Year
    StructField("month", IntegerType(), True)                   # Month
])

gold_traffic_realtime_schema = StructType([
    StructField("link_id", StringType(), False),                # Link ID
    StructField("street_name", StringType(), True),             # Street name
    StructField("borough", StringType(), True),                 # Borough
    StructField("latitude", DoubleType(), True),                # Latitude
    StructField("longitude", DoubleType(), True),               # Longitude
    StructField("current_speed", FloatType(), True),            # Real-time speed
    StructField("travel_time", IntegerType(), True),            # Real-time travel time
    StructField("traffic_status", StringType(), True),          # Congestion status
    StructField("event_time", TimestampType(), True),           # Last data update
    StructField("last_updated", TimestampType(), True)          # Processing time
])

gold_traffic_analytics_schema = StructType([
    StructField("link_id", StringType(), False),                # Link ID
    StructField("street_name", StringType(), True),             # Road name
    StructField("borough", StringType(), True),                 # Borough
    StructField("day_name", StringType(), True),                # Weekday name
    StructField("is_weekend", BooleanType(), True),             # Weekend flag
    StructField("avg_speed_daily", FloatType(), True),          # Historical average speed
    StructField("peak_congestion_hour", IntegerType(), True),   # Worst congestion hour
    StructField("reliability_score", StringType(), True),       # Segment reliability score
    StructField("total_readings", IntegerType(), True),         # Total data samples
    StructField("last_updated", TimestampType(), True)          # Aggregation time
])

# ==========================================
# VEHICLE COLLISIONS SCHEMAS
# ==========================================

bronze_crashes_schema = StructType([
    StructField("crash_date", StringType(), True),               # Raw crash date
    StructField("crash_time", StringType(), True),               # Raw crash time
    StructField("borough", StringType(), True),                  # Borough
    StructField("zip_code", StringType(), True),                 # Zip code
    StructField("latitude", StringType(), True),                 # Latitude string
    StructField("longitude", StringType(), True),                # Longitude string
    StructField("location", location_schema, True),              # Nested geometry
    StructField("on_street_name", StringType(), True),           # Primary street name
    StructField("off_street_name", StringType(), True),          # Intersecting street
    StructField("number_of_persons_injured", StringType(), True),# Persons injured count
    StructField("number_of_persons_killed", StringType(), True), # Persons killed count
    StructField("number_of_pedestrians_injured", StringType(), True),
    StructField("number_of_pedestrians_killed", StringType(), True),
    StructField("number_of_cyclist_injured", StringType(), True),
    StructField("number_of_cyclist_killed", StringType(), True),
    StructField("number_of_motorist_injured", StringType(), True),
    StructField("number_of_motorist_killed", StringType(), True),
    StructField("contributing_factor_vehicle_1", StringType(), True), # Cause vehicle 1
    StructField("contributing_factor_vehicle_2", StringType(), True), # Cause vehicle 2
    StructField("collision_id", StringType(), True),             # Crash ID
    StructField("vehicle_type_code1", StringType(), True),       # Vehicle type 1
    StructField("vehicle_type_code2", StringType(), True),       # Vehicle type 2
    StructField("ingested_at", StringType(), True)                # Pipeline audit time
])

silver_crashes_schema = StructType([
    StructField("collision_id", StringType(), True),             # Crash ID
    StructField("crash_timestamp", TimestampType(), True),       # Parsed timestamp
    StructField("year", IntegerType(), True),                    # Year
    StructField("month", IntegerType(), True),                   # Month
    StructField("day_of_week", StringType(), True),              # Weekday name
    StructField("latitude", DoubleType(), True),                 # Latitude cast to Double
    StructField("longitude", DoubleType(), True),                # Longitude cast to Double
    StructField("on_street_name", StringType(), True),           # Normalized street name
    StructField("borough", StringType(), True),                  # Normalized borough
    StructField("total_injured", IntegerType(), True),           # Combined injury count
    StructField("total_killed", IntegerType(), True),            # Combined fatality count
    StructField("pedestrians_injured", IntegerType(), True),     # Pedestrian injury count
    StructField("cyclist_injured", IntegerType(), True),         # Cyclist injury count
    StructField("motorist_injured", IntegerType(), True),        # Motorist injury count
    StructField("contributing_factor", StringType(), True),      # Standardized cause
    StructField("vehicle_type", StringType(), True),             # Dominant vehicle type
    StructField("ingestion_time", TimestampType(), True)         # Processing time
])

gold_crashes_schema = StructType([
    StructField("street_name", StringType(), True),              # Aggregation key: street
    StructField("borough", StringType(), True),                  # Borough
    StructField("total_crashes", LongType(), True),              # Total count of accidents
    StructField("total_injured", LongType(), True),              # Sum of injuries
    StructField("total_killed", LongType(), True),               # Sum of fatalities
    StructField("main_cause", StringType(), True),               # Top contributing factor
    StructField("latitude", DoubleType(), True),                 # Geometric center latitude
    StructField("longitude", DoubleType(), True),                # Geometric center longitude
    StructField("crash_pct", DoubleType(), True),                # Relative crash percentage
    StructField("danger_rank", IntegerType(), True),             # Safety risk rank
    StructField("safety_label", StringType(), True),             # Qualitative safety label
    StructField("first_crash_date", TimestampType(), True),      # Records start date
    StructField("last_crash_date", TimestampType(), True),       # Records end date
    StructField("unique_crash_days", LongType(), True),          # Count of distinct days with accidents
    StructField("sample_day_of_week", StringType(), True),       # Most common day for crashes
    StructField("last_updated", TimestampType(), True)           # Gold layer update time
])