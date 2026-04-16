# import telebot
# import psycopg2
# from telebot import types
# from datetime import datetime

# # --- CONFIGURATION ---
# TOKEN = "7116435990:AAE23VMeB2d0yWR02GBYCaWDSN_Pi1K0uPk"
# bot = telebot.TeleBot(TOKEN)

# DB_CONFIG = {
#     "dbname": "nyc_data", 
#     "user": "postgres", 
#     "password": "postgres",
#     "host": "postgres", 
#     "port": "5432"
# }

# def get_connection():
#     return psycopg2.connect(**DB_CONFIG)

# def get_map_link(street, borough):
#     """Generates an official and reliable Google Maps search link"""
#     clean_street = street.replace(' ', '+')
#     clean_borough = borough.replace(' ', '+') if borough else "NYC"
#     # Official Google Maps Search URL
#     return f"https://www.google.com/maps/search/?api=1&query={clean_street}+{clean_borough}+New+York"

# # --- DATA FETCHING FUNCTIONS ---

# def fetch_311_detailed_report():
#     try:
#         conn = get_connection(); cur = conn.cursor()
#         cur.execute("SELECT MIN(data_time), MAX(data_time), SUM(complaint_count) FROM gold_311_stats")
#         min_d, max_d, city_total = cur.fetchone()

#         cur.execute("""
#             SELECT DISTINCT ON (borough) borough, complaint_type, SUM(complaint_count) as vol, MAX(peak_hour)
#             FROM gold_311_stats
#             GROUP BY borough, complaint_type
#             ORDER BY borough, vol DESC
#         """)
#         borough_issues = cur.fetchall()

#         cur.execute("""
#             WITH RankedStreets AS (
#                 SELECT borough, street_name, complaint_type, SUM(complaint_count) as street_sum,
#                        MAX(peak_hour) as peak_h,
#                        ROW_NUMBER() OVER(PARTITION BY borough ORDER BY SUM(complaint_count) DESC) as rank
#                 FROM gold_311_stats
#                 WHERE street_name NOT IN ('UNKNOWN', '', 'UNSPECIFIED')
#                 GROUP BY borough, street_name, complaint_type
#             )
#             SELECT borough, street_name, complaint_type, street_sum, peak_h
#             FROM RankedStreets WHERE rank <= 2
#             ORDER BY borough, street_sum DESC
#         """)
#         top_streets = cur.fetchall()
#         cur.close(); conn.close()
#         return min_d, max_d, city_total, borough_issues, top_streets
#     except: return None

# def fetch_traffic_safety_report():
#     try:
#         conn = get_connection(); cur = conn.cursor()
#         cur.execute("SELECT MIN(data_time), MAX(data_time) FROM gold_traffic_safety_stats")
#         min_ts, latest_ts = cur.fetchone()
        
#         # Busiest streets with speed and individual update time
#         cur.execute("""
#             SELECT link_name, MAX(borough), AVG(speed), MAX(data_time) 
#             FROM gold_traffic_safety_stats 
#             WHERE data_time >= %s - INTERVAL '1 hour' 
#             GROUP BY link_name ORDER BY 3 ASC LIMIT 3
#         """, (latest_ts,))
#         busiest = cur.fetchall()
        
#         # Dangerous streets with Accident timestamp added
#         cur.execute("""
#             SELECT DISTINCT ON (total_crashes) link_name, borough, total_crashes, total_killed, total_injured, data_time 
#             FROM gold_traffic_safety_stats WHERE total_crashes > 0 
#             ORDER BY total_crashes DESC LIMIT 3
#         """)
#         dangerous = cur.fetchall()
#         cur.close(); conn.close()
#         return min_ts, latest_ts, busiest, dangerous
#     except: return None

# # --- MESSAGE HANDLERS ---

# @bot.message_handler(commands=['start'])
# def send_welcome(message):
#     markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
#     markup.add(types.KeyboardButton('🚨 Traffic & Safety'), types.KeyboardButton('🧹 311 City Pulse'))
#     markup.add(types.KeyboardButton('📸 Camera Summary'), types.KeyboardButton('🚗 Parking Summary'))
#     markup.add(types.KeyboardButton('🔍 Search Street'))
    
#     welcome_text = (
#         "👋 *Welcome to the NYC Smart City Bot!*\n\n"
#         "I am your real-time guide to New York City's data.\n\n"
#         "*What can I do for you?*\n"
#         "• 🚨 *Traffic & Safety:* Live speeds and accident reports.\n"
#         "• 🧹 *311 City Pulse:* Neighborhood complaints and peak hours.\n"
#         "• 📸 *Enforcement:* Camera and Parking ticket summaries.\n\n"
#         "💡 *Pro Tip:* You can also just type any street name (e.g., *Broadway*) to get a full combined report!\n\n"
#         "*Select an option below to start:*"
#     )
#     bot.send_message(message.chat.id, welcome_text, parse_mode='Markdown', reply_markup=markup)

# @bot.message_handler(func=lambda message: True)
# def handle_all_messages(message):
#     text = message.text

#     # --- 311 BUTTON ---
#     if text == '🧹 311 City Pulse':
#         data = fetch_311_detailed_report()
#         if not data:
#             bot.send_message(message.chat.id, "❌ No 311 data available."); return
#         min_d, max_d, total, b_issues, s_issues = data
#         res = "🧹 *311 CITY PULSE - DETAILED REPORT*\n"
#         res += f"📅 *Period:* `{min_d} to {max_d}`\n\n"
#         res += "🏢 *TOP ISSUE PER BOROUGH:*\n"
#         for b in b_issues:
#             peak = f"{int(b[3]):02d}:00" if b[3] is not None else "N/A"
#             res += f"• *{b[0]}:* {b[1]} (Peak: {peak})\n"
#         res += "\n📍 *TOP 2 STREETS PER BOROUGH:*\n"
#         current_b = ""
#         for s in s_issues:
#             street_peak = f"{int(s[4]):02d}:00" if s[4] is not None else "N/A"
#             if s[0] != current_b:
#                 current_b = s[0]; res += f"\n🏙 *{current_b}*\n"
#             res += f" ↳ *{s[1]}* ({s[2]})\n    Count: `{int(s[3])}` | *Peak Hour: {street_peak}*\n    🔗 [Map]({get_map_link(s[1], s[0])})\n"
#         bot.send_message(message.chat.id, res, parse_mode='Markdown', disable_web_page_preview=True)

#     # --- TRAFFIC BUTTON ---
#     elif text == '🚨 Traffic & Safety':
#         data = fetch_traffic_safety_report()
#         if not data:
#             bot.send_message(message.chat.id, "❌ Traffic data unavailable."); return
#         min_ts, ts, busiest, dangerous = data
#         res = f"🏎 *TRAFFIC & SAFETY REPORT*\n📅 `{min_ts.strftime('%d/%m/%Y')} - {ts.strftime('%d/%m/%Y')}`\n\n🔥 *Busiest Streets:*\n"
#         for b in busiest:
#             upd = b[3].strftime('%H:%M') if b[3] else "N/A"
#             res += f"• *{b[0]}*: `{b[2]:.1f} mph` (Updated: {upd})\n"
        
#         res += "\n⚠️ *Dangerous (Accidents):*\n"
#         for d in dangerous:
#             # d[5] is the data_time for the last accident
#             last_acc = d[5].strftime('%d/%m %H:%M') if d[5] else "N/A"
#             res += f"• *{d[0]}* ({d[1]})\n"
#             res += f"  ↳ 💥 `{int(d[2])}` Crashes | Fatalities: `{int(d[3])}`\n"
#             res += f"  ↳ 📅 *Last Accident:* `{last_acc}`\n"
#             res += f"  ↳ 🔗 [View Map]({get_map_link(d[0], d[1])})\n"
#         bot.send_message(message.chat.id, res, parse_mode='Markdown', disable_web_page_preview=True)

#     # --- SEARCH STREET ---
#     elif text == '🔍 Search Street':
#         bot.send_message(message.chat.id, "Please type a street name (e.g., Broadway):")

#     elif text not in ['📸 Camera Summary', '🚗 Parking Summary']:
#         try:
#             conn = get_connection(); cur = conn.cursor()
#             cur.execute("SELECT complaint_type, SUM(complaint_count), MAX(peak_hour), MAX(borough) FROM gold_311_stats WHERE street_name ILIKE %s GROUP BY complaint_type ORDER BY 2 DESC LIMIT 1", (f"%{text}%",))
#             c311 = cur.fetchone()
#             # Added injured/killed and timestamp for street search
#             cur.execute("SELECT speed, total_crashes, borough, data_time, total_injured, total_killed FROM gold_traffic_safety_stats WHERE link_name ILIKE %s ORDER BY data_time DESC LIMIT 1", (f"%{text}%",))
#             traf = cur.fetchone()
            
#             if not c311 and not traf:
#                 bot.send_message(message.chat.id, f"🔍 No data found for '{text}'.")
#             else:
#                 borough = (c311[3] if c311 else traf[2]); res = f"📍 *REPORT: {text.upper()}*\n🏙 Borough: `{borough}`\n🔗 [Open in Google Maps]({get_map_link(text, borough)})\n\n"
#                 if c311:
#                     peak = f"{int(c311[2]):02d}:00" if c311[2] is not None else "N/A"
#                     res += f"🧹 *311:* `{c311[0]}`\n• Total: `{int(c311[1])}` | Peak: `{peak}`\n\n"
#                 if traf:
#                     last_event = traf[3].strftime('%d/%m %H:%M') if traf[3] else "N/A"
#                     res += f"🏎 *Traffic:* `{traf[0]:.1f} mph` (Updated: {last_event})\n"
#                     res += f"• Accidents: `{int(traf[1])}` (🤕{int(traf[4])} / 💀{int(traf[5])})\n"
#                 bot.send_message(message.chat.id, res, parse_mode='Markdown')
#             cur.close(); conn.close()
#         except: pass

# if __name__ == "__main__":
#     print("🚀 NYC Bot is LIVE with Accident Timestamps...")
#     bot.polling(none_stop=True)

import telebot
import psycopg2
from telebot import types
import pytz
from datetime import datetime

# --- CONFIGURATION ---
TOKEN = "7116435990:AAE23VMeB2d0yWR02GBYCaWDSN_Pi1K0uPk"
bot = telebot.TeleBot(TOKEN)

DB_CONFIG = {
    "dbname": "nyc_data", 
    "user": "postgres", 
    "password": "postgres",
    "host": "postgres", 
    "port": "5432"
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_map_link(lat, lon):
    """Generates a Google Maps link based on coordinates"""
    return f"https://www.google.com/maps?q={lat},{lon}"

# --- DATA FETCHING FUNCTIONS ---

def fetch_311_summary():
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute("""
            SELECT borough, complaint_type, complaint_count, intensity_level, hour, latest_incident 
            FROM gold_311_stats 
            ORDER BY latest_incident DESC LIMIT 5
        """)
        data = cur.fetchall()
        cur.close(); conn.close()
        return data
    except: return []

def fetch_safety_summary():
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute("""
            SELECT street_name, borough, total_crashes, safety_label, last_updated 
            FROM gold_crash_stats 
            ORDER BY total_crashes DESC LIMIT 5
        """)
        data = cur.fetchall()
        cur.close(); conn.close()
        return data
    except: return []

# --- MESSAGE HANDLERS ---

@bot.message_handler(commands=['start'])
def send_welcome(message):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton('🚨 Traffic & Safety'), types.KeyboardButton('🧹 311 City Pulse'))
    markup.add(types.KeyboardButton('🔍 Search Street'))
    
    welcome_text = (
        "👋 *Welcome to the NYC Smart City Bot!*\n\n"
        "I provide real-time insights from NYC's data layers.\n\n"
        "• 🚨 *Traffic & Safety:* Accident analysis and danger zones.\n"
        "• 🧹 *311 City Pulse:* Neighborhood complaints and intensity.\n"
        "• 🔍 *Search Street:* Detailed report for any location.\n\n"
        "*Select an option below:*"
    )
    bot.send_message(message.chat.id, welcome_text, parse_mode='Markdown', reply_markup=markup)

# --- 🔍 SEARCH STREET FLOW ---

@bot.message_handler(func=lambda message: message.text == '🔍 Search Street')
def ask_for_street(message):
    msg = bot.send_message(message.chat.id, "📍 *Please enter the desired street name:*", parse_mode='Markdown')
    bot.register_next_step_handler(msg, process_street_search)

def process_street_search(message):
    street_query = message.text.strip()
    try:
        conn = get_connection(); cur = conn.cursor()
        
        # 1. חיפוש נתוני תאונות
        cur.execute("""
            SELECT borough, total_crashes, total_injured, total_killed, main_cause, safety_label, last_updated 
            FROM gold_crash_stats 
            WHERE street_name ILIKE %s LIMIT 1
        """, (f"%{street_query}%",))
        crash_data = cur.fetchone()

        # 2. חיפוש נתוני 311
        # הערה: בגלל שהגולד של 311 הוא לפי רובע, אנחנו מחפשים התאמה לרובע של הרחוב אם נמצא
        target_borough = crash_data[0] if crash_data else "%"
        cur.execute("""
            SELECT complaint_type, complaint_count, intensity_level, hour, latest_incident 
            FROM gold_311_stats 
            WHERE borough ILIKE %s ORDER BY complaint_count DESC LIMIT 1
        """, (target_borough,))
        complaint_data = cur.fetchone()

        if not crash_data and not complaint_data:
            bot.send_message(message.chat.id, f"❌ No data found for *{street_query}*.", parse_mode='Markdown')
            return

        res = f"📍 *STREET REPORT: {street_query.upper()}*\n"
        res += "━━━━━━━━━━━━━━━━━━━━\n\n"

        if crash_data:
            boro, crashes, injured, killed, cause, label, updated = crash_data
            res += f"🚨 *Safety Status:* {label}\n"
            res += f"• Borough: `{boro}`\n"
            res += f"• Total Crashes: `{crashes}`\n"
            res += f"• Injuries: `{injured}` | Fatalities: `{killed}`\n"
            res += f"• Main Cause: `{cause}`\n"
            res += f"🕒 *Sync:* `{updated.strftime('%Y-%m-%d %H:%M')}`\n\n"

        if complaint_data:
            ctype, count, intensity, peak, latest = complaint_data
            res += f"🧹 *311 Pulse:* {intensity}\n"
            res += f"• Top Issue: `{ctype}`\n"
            res += f"• Count: `{count}` reports\n"
            res += f"• Peak Hour: `{peak}:00`\n"
            res += f"🕒 *Last Report:* `{latest.strftime('%H:%M')}`\n"

        res += "\n━━━━━━━━━━━━━━━━━━━━"
        bot.send_message(message.chat.id, res, parse_mode='Markdown')
        
        cur.close(); conn.close()
    except Exception as e:
        bot.send_message(message.chat.id, "❌ Error connecting to data services.")
        print(f"Search Error: {e}")

# --- BUTTON HANDLERS ---

@bot.message_handler(func=lambda message: message.text == '🧹 311 City Pulse')
def handle_311(message):
    data = fetch_311_summary()
    if not data:
        bot.send_message(message.chat.id, "📭 No 311 data found."); return
    
    res = "🧹 *LATEST 311 ACTIVITY*\n\n"
    for row in data:
        res += f"🏢 *{row[0]}* | {row[3]}\n"
        res += f"↳ {row[1]}: `{row[2]}` reports\n"
        res += f"↳ Peak: `{row[4]}:00` | Latest: `{row[5].strftime('%H:%M')}`\n\n"
    bot.send_message(message.chat.id, res, parse_mode='Markdown')

@bot.message_handler(func=lambda message: message.text == '🚨 Traffic & Safety')
def handle_traffic(message):
    data = fetch_safety_summary()
    if not data:
        bot.send_message(message.chat.id, "📭 No safety data found."); return
    
    res = "🚨 *TRAFFIC SAFETY SUMMARY*\n\n"
    for row in data:
        res += f"📍 *{row[0]}* ({row[1]})\n"
        res += f"↳ {row[3]}\n"
        res += f"↳ Crashes: `{row[2]}`\n"
        res += f"🕒 Updated: `{row[4].strftime('%d/%m %H:%M')}`\n\n"
    bot.send_message(message.chat.id, res, parse_mode='Markdown')

if __name__ == "__main__":
    print("🚀 NYC Smart City Bot is LIVE...")
    bot.polling(none_stop=True)