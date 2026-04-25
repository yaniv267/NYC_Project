


import telebot
import psycopg2
from telebot import types
from datetime import datetime

# ==========================================
# 1. INITIAL CONFIGURATION & BOT SETUP
# ==========================================

TOKEN = "7116435990:AAE23VMeB2d0yWR02GBYCaWDSN_Pi1K0uPk"
bot = telebot.TeleBot(TOKEN)

DB_CONFIG = {
    "dbname": "nyc_data", 
    "user": "postgres", 
    "password": "postgres",
    "host": "postgres", 
    "port": "5432"
}

# ==========================================
# 2. DATABASE & UTILITY FUNCTIONS
# ==========================================

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_map_link(lat, lon):
    """Generates a Google Maps link using coordinates."""
    if lat and lon:
        return f"https://www.google.com/maps?q={lat},{lon}"
    return None

def get_time_of_day(hour):
    """המרת שעה לחלק ביום עבור הדו"ח (מתוך הסקריפט שעבד לך)"""
    try:
        h = int(hour)
        if 5 <= h < 12: return "Morning 🌅"
        if 12 <= h < 17: return "Afternoon ☀️"
        if 17 <= h < 21: return "Evening 🌆"
        return "Night 🌙"
    except:
        return "N/A"

# ==========================================
# 3. COMMAND HANDLERS (START & WELCOME)
# ==========================================

@bot.message_handler(commands=['start'])
def send_welcome(message):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton('🚦 Live Traffic'), 
        types.KeyboardButton('🧹 311 City Pulse'), 
        types.KeyboardButton('🎫 Violations Report'), 
        types.KeyboardButton('💥 Crash Report'),
        types.KeyboardButton('🔍 Search Street')
    )
    
    welcome_text = (
        "👋 *Welcome to NYC Smart City Bot!*\n\n"
        "Data-driven insights for New York City.\n\n"
        "• 🚦 *Traffic:* Current speeds & congestion.\n"
        "• 🧹 *311 Pulse:* Neighborhood complaints.\n"
        "• 🎫 *Violations:* Risk analysis (Camera vs. Manual).\n"
        "• 🔍 *Search:* Street-level survival guide.\n"
    )
    bot.send_message(message.chat.id, welcome_text, parse_mode='Markdown', reply_markup=markup)

# ==========================================
# 4. VIOLATIONS ANALYTICS HANDLER
# ==========================================
@bot.message_handler(func=lambda message: message.text == '🎫 Violations Report')
def handle_violations_report(message):
    try:
        conn = get_connection(); cur = conn.cursor()
        query = """
        WITH Stats AS (
            SELECT borough, COUNT(*) as total,
                COUNT(*) FILTER (WHERE is_camera = 'Yes') as cam_count,
                COUNT(*) FILTER (WHERE is_camera = 'No') as manual_count,
                issue_day_name, time_of_day, AVG(lat) as lat, AVG(lon) as lon,
                RANK() OVER (PARTITION BY borough ORDER BY COUNT(*) DESC) as rnk
            FROM gold_traffic_violations
            GROUP BY borough, issue_day_name, time_of_day
        )
        SELECT borough, total, cam_count, manual_count, issue_day_name, time_of_day, (cam_count::float / total * 100), lat, lon
        FROM Stats WHERE rnk = 1 ORDER BY total DESC LIMIT 5
        """
        cur.execute(query)
        rows = cur.fetchall(); cur.close(); conn.close()
        res = "🎫 *NYC VIOLATIONS SUMMARY*\n━━━━━━━━━━━━━━━━━━━━\n\n"
        for row in rows:
            res += f"🏙️ *{row[0]}*\n"
            res += f"↳ Peak: `{row[4]}` | `{row[5]}`\n↳ Tickets: `{row[1]}`\n────────────────────\n"
        bot.send_message(message.chat.id, res, parse_mode='Markdown')
    except:
        bot.send_message(message.chat.id, "❌ Error fetching violations.")

# ==========================================
# 5. CRASH REPORTING HANDLER
# ==========================================
@bot.message_handler(func=lambda message: message.text == '💥 Crash Report')
def handle_crashes_report(message):
    try:
        conn = get_connection(); cur = conn.cursor()
        query = """
        WITH CrashStats AS (
            SELECT borough, SUM(total_crashes) as total_c, SUM(total_injured) as total_i, SUM(total_killed) as total_k, main_cause,
                AVG(latitude) as lat, AVG(longitude) as lon, RANK() OVER (PARTITION BY borough ORDER BY SUM(total_crashes) DESC) as rnk
            FROM gold_crash_stats WHERE borough IS NOT NULL AND borough != '' AND borough != 'None'
            GROUP BY borough, main_cause
        )
        SELECT borough, total_c, total_i, total_k, main_cause, lat, lon
        FROM CrashStats WHERE rnk = 1 ORDER BY total_c DESC LIMIT 5
        """
        cur.execute(query)
        rows = cur.fetchall(); cur.close(); conn.close()
        res = "💥 *NYC CRASHES SUMMARY*\n━━━━━━━━━━━━━━━━━━━━\n\n"
        for row in rows:
            res += f"🏙️ *{row[0]}*\n"
            res += f"↳ Total: `{int(row[1])}` | Injuries: `{int(row[2])}`\n↳ Cause: `{row[4]}`\n────────────────────\n"
        bot.send_message(message.chat.id, res, parse_mode='Markdown')
    except:
        bot.send_message(message.chat.id, "❌ Error fetching crashes.")

# ==========================================
# 6. REAL-TIME TRAFFIC MONITORING
# ==========================================
@bot.message_handler(func=lambda message: message.text == '🚦 Live Traffic')
def handle_live_traffic(message):
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute("SELECT street_name, borough, current_speed, traffic_status FROM gold_traffic_realtime WHERE traffic_status LIKE '%Heavy%' ORDER BY current_speed ASC LIMIT 3")
        rows = cur.fetchall(); cur.close(); conn.close()
        res = "🚦 *HEAVY TRAFFIC HOTSPOTS*\n━━━━━━━━━━━━━━━━━━━━\n\n"
        for row in rows:
            res += f"📍 *{row[0]}* ({row[1]})\n↳ Status: {row[3]} | `{row[2]} mph`\n\n"
        bot.send_message(message.chat.id, res, parse_mode='Markdown')
    except:
        bot.send_message(message.chat.id, "❌ Error fetching traffic.")

# ==========================================
# 7. 311 CITY COMPLAINTS PULSE
# ==========================================
@bot.message_handler(func=lambda message: message.text == '🧹 311 City Pulse')
def handle_311_pulse(message):
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT ON (borough) borough, complaint_type, complaint_count, peak_hour 
            FROM gold_311_stats WHERE borough IS NOT NULL AND borough != '' AND borough != 'None'
            ORDER BY borough, complaint_count DESC
        """)
        rows = cur.fetchall(); cur.close(); conn.close()
        res = "🧹 *NYC 311 - TOP COMPLAINTS*\n━━━━━━━━━━━━━━━━━━━━\n\n"
        for row in rows:
            res += f"🏙️ *{row[0]}*\n↳ Issue: `{row[1]}` ({int(row[2])})\n────────────────────\n"
        bot.send_message(message.chat.id, res, parse_mode='Markdown')
    except:
        bot.send_message(message.chat.id, "❌ Error fetching 311 data.")

# ==========================================
# 8. STREET-LEVEL UNIFIED SEARCH
# ==========================================

@bot.message_handler(func=lambda message: message.text == '🔍 Search Street')
def ask_for_street(message):
    msg = bot.send_message(message.chat.id, "📍 *Enter street keywords (e.g., 'Broadway'):*", parse_mode='Markdown')
    bot.register_next_step_handler(msg, process_street_search)

def process_street_search(message):
    user_input = message.text.strip().upper()
    if not user_input: return

    try:
        conn = get_connection(); cur = conn.cursor()
        
       
        filter_boro = " AND borough IS NOT NULL AND borough != 'None' AND borough != ''"
        
        cur.execute(f"""
            SELECT DISTINCT UPPER(street_name), UPPER(borough) FROM gold_311_stats WHERE UPPER(street_name) = %s {filter_boro}
            UNION
            SELECT DISTINCT UPPER(street_name), UPPER(borough) FROM gold_traffic_violations WHERE UPPER(street_name) = %s {filter_boro}
            UNION
            SELECT DISTINCT UPPER(street_name), UPPER(borough) FROM gold_crash_stats WHERE UPPER(street_name) = %s {filter_boro}
            UNION
            SELECT DISTINCT UPPER(street_name), UPPER(borough) FROM gold_traffic_realtime WHERE UPPER(street_name) = %s {filter_boro}
            LIMIT 5
        """, (user_input, user_input, user_input, user_input))
        
        street_combos = cur.fetchall()

        if not street_combos:
            bot.send_message(message.chat.id, f"❌ No data found for: '{user_input}' (or borough info missing)")
            return

        for s_name, boro in street_combos:
            match_params = [s_name, boro]
            match_cond = "UPPER(street_name) = %s AND UPPER(borough) = %s"

            # שליפת נתונים
            cur.execute(f"SELECT current_speed, traffic_status, event_time, latitude, longitude FROM gold_traffic_realtime WHERE {match_cond} LIMIT 1", match_params)
            traf = cur.fetchone()

            cur.execute(f"SELECT complaint_type, complaint_count, peak_hour, latitude, longitude FROM gold_311_stats WHERE {match_cond} ORDER BY complaint_count DESC LIMIT 1", match_params)
            c311 = cur.fetchone()

            cur.execute(f"SELECT violation_desc, tickets_count, risk_level, is_camera, lat, lon FROM gold_traffic_violations WHERE {match_cond} ORDER BY tickets_count DESC LIMIT 1", match_params)
            violation = cur.fetchone()

            cur.execute(f"SELECT * FROM gold_crash_stats WHERE {match_cond} ORDER BY total_crashes DESC LIMIT 1", match_params)
            crash = cur.fetchone()

            
            final_lat, final_lon = None, None
            if traf and traf[3]: final_lat, final_lon = traf[3], traf[4]
            elif c311 and c311[3]: final_lat, final_lon = c311[3], c311[4]
            elif violation and violation[4]: final_lat, final_lon = violation[4], violation[5]
            elif crash and crash[6]: final_lat, final_lon = crash[6], crash[7]

            map_url = get_map_link(final_lat, final_lon)

            # בניית הדוח
            res = f"📊 *STREET REPORT: {s_name}*\n"
            res += f"🏙️ *Borough:* `{boro}`\n"
            res += "━━━━━━━━━━━━━━━━━━━━\n\n"
            
            if traf:
                time_str = traf[2].strftime('%H:%M') if traf[2] else "N/A"
                res += f"🚗 *TRAFFIC*\n• Speed: `{traf[0]} mph` | `{traf[1]}`\n• At: `{time_str}`\n\n"
            
            if c311:
                p_311 = f"{int(c311[2]):02d}:00" if c311[2] is not None else "N/A"
                res += f"🧹 *311 PULSE*\n• Issue: `{c311[0]}` ({int(c311[1])})\n• Peak: `{p_311}`\n\n"
            
            if violation:
                cam = "📸 Camera Detected" if violation[3] == 'Yes' else "👮 Manual Enforcement"
                res += f"🎫 *VIOLATIONS*\n• Top: `{violation[0]}`\n• Risk: `{violation[2]}`\n• Type: `{cam}`\n\n"

            if crash:
                p_day = crash[14]
                p_hour = crash[13]
                time_desc = get_time_of_day(p_hour)

                res += f"💥 *CRASH HISTORY*\n"
                res += f"• Safety: `{crash[10]}`\n"
                res += f"• Total Crashes: `{int(crash[2])}`\n"
                res += f"• ⚠️ *Peak Timing:* `{p_day}` | `{time_desc}`\n"
                res += f"• Injured: `{int(crash[3])}` | Killed: `{int(crash[4])}`\n"
                res += f"• Main Cause: `{crash[5]}`\n\n"
            
            if map_url:
                res += f"🔗 [View Street on Map]({map_url})\n\n"
                
            res += "━━━━━━━━━━━━━━━━━━━━"
            bot.send_message(message.chat.id, res, parse_mode='Markdown', disable_web_page_preview=True)
        
        cur.close(); conn.close()

    except Exception as e:
        print(f"Error: {e}")
        bot.send_message(message.chat.id, "❌ Error fetching data during search.")

# ==========================================
# 9. MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    print("🚀 Bot is LIVE with Borough Filtering...")
    bot.polling(none_stop=True)