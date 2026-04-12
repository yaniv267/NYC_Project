# import telebot
# import psycopg2
# from telebot import types

# # --- CONFIGURATION ---
# TOKEN = "7116435990:AAE23VMeB2d0yWR02GBYCaWDSN_Pi1K0uPk"

# DB_CONFIG = {
#     "dbname": "nyc_data",
#     "user": "postgres",
#     "password": "postgres",
#     "host": "postgres",
#     "port": "5432"
# }

# # --- 1. NEW: DATABASE HEALTH CHECK ---
# def check_db_connection():
#     try:
#         conn = psycopg2.connect(**DB_CONFIG)
#         conn.close()
#         print("✅ SUCCESS: Connected to PostgreSQL database!")
#         return True
#     except Exception as e:
#         print(f"❌ CRITICAL ERROR: Could not connect to DB at {DB_CONFIG['host']}.")
#         print(f"   Reason: {e}")
#         return False

# # --- DATABASE LOGIC ---
# def fetch_street_stats(query_text):
#     try:
#         conn = psycopg2.connect(**DB_CONFIG)
#         cur = conn.cursor()
        
#         sql = """
#         SELECT 
#             link_name, 
#             traffic_level, 
#             speed, 
#             safety_score, 
#             total_crashes, 
#             latitude, 
#             longitude,
#             TO_CHAR(data_time, 'YYYY-MM-DD HH24:MI') as update_time
#         FROM gold_traffic_safety_stats 
#         WHERE TRIM(link_name) ILIKE %s
#         ORDER BY data_time DESC
#         LIMIT 1;
#         """
        
#         search_term = f"%{query_text.strip()}%"
#         cur.execute(sql, (search_term,))
#         result = cur.fetchone()
        
#         cur.close()
#         conn.close()
#         return result
#     except Exception as e:
#         print(f"❌ DATABASE ERROR: {e}")
#         return None

# # --- BOT INTERFACE ---
# bot = telebot.TeleBot(TOKEN)

# @bot.message_handler(commands=['start', 'help'])
# def send_welcome(message):
#     welcome_msg = (
#         "🗽 *Welcome to the NYC Traffic & Safety Bot!*\n\n"
#         "Send me a street name (e.g., 'GOW' or 'Broadway') "
#         "to get real-time traffic stats and a location map."
#     )
#     bot.reply_to(message, welcome_msg, parse_mode='Markdown')

# @bot.message_handler(func=lambda message: True)
# def handle_message(message):
#     query = message.text
#     print(f"🔍 LOG: User searching for: {query}")
    
#     stats = fetch_street_stats(query)
    
#     if stats:
#         name, traffic, speed, safety, crashes, lat, lon, update_time = stats
        
#         # --- 2. FIXED: Google Maps URL ---
#         # השתמשנו בכתובת התקנית של גוגל מפות
#         google_maps_url = f"https://www.google.com/maps?q={lat},{lon}"
        
#         response_text = (
#             f"📍 *Street:* {name}\n"
#             f"━━━━━━━━━━━━━━━\n"
#             f"🕒 *Last Update:* {update_time}\n"
#             f"📊 *Traffic Level:* {traffic}\n"
#             f"🏎 *Current Speed:* {speed} mph\n"
#             f"⚠️ *Total Crashes:* {crashes if crashes else 0}\n"
#             f"🛡 *Safety Score:* {safety if safety else 'N/A'}/100\n\n"
#             f"🔗 [Open in Google Maps]({google_maps_url})\n"
#             f"━━━━━━━━━━━━━━━"
#         )
        
#         bot.send_message(message.chat.id, response_text, parse_mode='Markdown')
        
#         if lat and lon:
#             try:
#                 bot.send_location(message.chat.id, latitude=lat, longitude=lon)
#             except Exception as e:
#                 print(f"❌ MAP ERROR: {e}")
#     else:
#         bot.reply_to(message, f"🔍 No data found for '{query}'.\nTry a shorter name or a different street.")

# # --- MAIN EXECUTION ---
# if __name__ == "__main__":
#     # נבדוק חיבור לפני הכל
#     if check_db_connection():
#         print("🚀 NYC Traffic Bot is now LIVE and listening...")
#         bot.polling(none_stop=True)
#     else:
#         print("🛑 Bot stopped due to Database Connection failure.")

import telebot
import psycopg2
from telebot import types

# --- CONFIGURATION ---
TOKEN = "7116435990:AAE23VMeB2d0yWR02GBYCaWDSN_Pi1K0uPk"

# Database Configuration (Using 'postgres' host for Docker network)
DB_CONFIG = {
    "dbname": "nyc_data",
    "user": "postgres",
    "password": "postgres",
    "host": "postgres",
    "port": "5432"
}

# --- 1. DATABASE HEALTH CHECK ---
def check_db_connection():
    """Checks if the bot can connect to the database on startup."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.close()
        print("✅ SUCCESS: Connected to PostgreSQL database!")
        return True
    except Exception as e:
        print(f"❌ CRITICAL ERROR: Could not connect to DB at {DB_CONFIG['host']}.")
        print(f"   Reason: {e}")
        return False

# --- 2. DATABASE LOGIC ---
def fetch_street_stats(query_text):
    """Fetches the latest stats for a given street name from the Gold table."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        sql = """
        SELECT 
            link_name, 
            traffic_level, 
            speed, 
            safety_score, 
            total_crashes, 
            latitude, 
            longitude,
            TO_CHAR(data_time, 'YYYY-MM-DD HH24:MI') as update_time
        FROM gold_traffic_safety_stats 
        WHERE TRIM(link_name) ILIKE %s
        ORDER BY data_time DESC
        LIMIT 1;
        """
        
        search_term = f"%{query_text.strip()}%"
        cur.execute(sql, (search_term,))
        result = cur.fetchone()
        
        cur.close()
        conn.close()
        return result
    except Exception as e:
        print(f"❌ DATABASE ERROR: {e}")
        return None

# --- 3. BOT INTERFACE ---
bot = telebot.TeleBot(TOKEN)

# Welcome message with Keyboard Buttons
@bot.message_handler(commands=['start', 'help'])
@bot.message_handler(func=lambda message: message.text.lower() in ['hi', 'hello', 'hey'])
def send_welcome(message):
    """Greets the user and displays a persistent menu."""
    
    # Create the keyboard markup
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
    btn_search = types.KeyboardButton('🔍 Search a Street')
    btn_help = types.KeyboardButton('❓ How it works?')
    markup.add(btn_search, btn_help)
    
    welcome_text = (
        "👋 *Welcome to NYC Traffic Bot!*\n\n"
        "Please enter the *Street Name* you would like to receive information about.\n\n"
        "💡 _Example: GOW, Broadway, or Richmond_"
    )
    bot.send_message(message.chat.id, welcome_text, parse_mode='Markdown', reply_markup=markup)

# Handle the "How it works" button
@bot.message_handler(func=lambda message: message.text == '❓ How it works?')
def handle_help_button(message):
    help_text = (
        "📖 *How to use this bot:*\n\n"
        "1. Type any NYC street or highway name.\n"
        "2. The bot will search for live speed and traffic data.\n"
        "3. You will receive a summary and a link to Google Maps."
    )
    bot.reply_to(message, help_text, parse_mode='Markdown')

# Handle the "Search" button
@bot.message_handler(func=lambda message: message.text == '🔍 Search a Street')
def handle_search_button(message):
    bot.reply_to(message, "OK! Just type the street name now (e.g., CBE or GOW).")

# Main message handler for searching streets
@bot.message_handler(func=lambda message: True)
def handle_message(message):
    query = message.text
    print(f"🔍 LOG: User searching for: {query}")
    
    stats = fetch_street_stats(query)
    
    if stats:
        name, traffic, speed, safety, crashes, lat, lon, update_time = stats
        
        # Google Maps URL
        google_maps_url = f"https://www.google.com/maps?q={lat},{lon}"
        
        # Build the response text
        response_text = (
            f"📍 *Street:* {name}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🕒 *Last Update:* {update_time}\n"
            f"📊 *Traffic Level:* {traffic}\n"
            f"🏎 *Current Speed:* {speed} mph\n"
            f"⚠️ *Total Crashes:* {crashes if crashes else 0}\n"
            f"🛡 *Safety Score:* {safety if safety else 'N/A'}/100\n\n"
            f"🔗 [Open in Google Maps]({google_maps_url})\n"
            f"━━━━━━━━━━━━━━━"
        )
        
        # Send text ONLY (No send_location)
        bot.send_message(message.chat.id, response_text, parse_mode='Markdown', disable_web_page_preview=False)
        
    else:
        bot.reply_to(message, f"🔍 No data found for '{query}'.\nPlease try a different street name.")

# --- 4. MAIN EXECUTION ---
if __name__ == "__main__":
    if check_db_connection():
        print("🚀 NYC Traffic Bot is now LIVE and listening...")
        bot.polling(none_stop=True)
    else:
        print("🛑 Bot stopped due to Database Connection failure.")