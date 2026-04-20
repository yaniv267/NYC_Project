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
            SELECT 
                borough,
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE is_camera = 'Yes') as cam_count,
                COUNT(*) FILTER (WHERE is_camera = 'No') as manual_count,
                issue_day_name,
                time_of_day,
                AVG(lat) as lat,
                AVG(lon) as lon,
                MAX(last_updated) as last_sync,
                RANK() OVER (PARTITION BY borough ORDER BY COUNT(*) DESC) as rnk
            FROM gold_traffic_violations
            GROUP BY borough, issue_day_name, time_of_day
        )
        SELECT 
            borough, total, cam_count, manual_count, 
            issue_day_name, time_of_day,
            (cam_count::float / total * 100) as cam_pct,
            lat, lon,last_sync
        FROM Stats WHERE rnk = 1
        ORDER BY total DESC LIMIT 5
        """
        cur.execute(query)
        rows = cur.fetchall()
        cur.close(); conn.close()

        res = "🎫 *NYC VIOLATIONS SUMMARY*\n━━━━━━━━━━━━━━━━━━━━\n\n"
        for row in rows:
            map_url = get_map_link(row[7], row[8])
            res += f"🏙️ *{row[0]}*\n"
            res += f"↳ 📸 Camera: `{row[2]}` ({row[6]:.1f}%) | 👮 Manual: `{row[3]}`\n"
            res += f"↳ 🗓️ Peak Day: `{row[4]}` in the `{row[5]}`\n"
            res += f"↳ Total Tickets: `{row[1]}`\n"
            if map_url: res += f"🔗 [View Borough Hotspot]({map_url})\n"
            res += "────────────────────\n"
        
        bot.send_message(message.chat.id, res, parse_mode='Markdown', disable_web_page_preview=True)
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
            SELECT 
                borough,
                SUM(total_crashes) as total_c,
                SUM(total_injured) as total_i,
                SUM(total_killed) as total_k,
                main_cause,
                AVG(latitude) as lat,
                AVG(longitude) as lon,
                MAX(last_updated) as last_sync,
                RANK() OVER (PARTITION BY borough ORDER BY SUM(total_crashes) DESC) as rnk
            FROM gold_crash_stats
            WHERE borough IS NOT NULL AND borough != ''
            GROUP BY borough, main_cause
        )
        SELECT 
            borough, total_c, total_i, total_k, 
            main_cause, lat, lon, last_sync
        FROM CrashStats WHERE rnk = 1
        ORDER BY total_c DESC LIMIT 5
        """
        cur.execute(query)
        rows = cur.fetchall()
        cur.close(); conn.close()

        res = "💥 *NYC CRASHES SUMMARY*\n━━━━━━━━━━━━━━━━━━━━\n\n"
        for row in rows:
            sync_time = row[7].strftime('%d/%m/%Y %H:%M') if row[7] else "N/A"
            map_url = get_map_link(row[5], row[6])
            
            res += f"🏙️ *{row[0]}*\n"
            res += f"↳ 🚗 Total Crashes: `{row[1]}`\n"
            res += f"↳ 🏥 Injuries: `{row[2]}` | ⚰️ Killed: `{row[3]}`\n"
            res += f"↳ ⚠️ Main Cause: `{row[4]}`\n"
            res += f"↳ 🕒 Last Sync: `{sync_time}`\n"
            
            if map_url:
                res += f"🔗 [View Crash Hotspot]({map_url})\n"
            res += "────────────────────\n"

        bot.send_message(message.chat.id, res, parse_mode='Markdown', disable_web_page_preview=True)
    except Exception as e:
        bot.send_message(message.chat.id, "❌ Error fetching crash data.")

# ==========================================
# 6. REAL-TIME TRAFFIC MONITORING
# ==========================================

@bot.message_handler(func=lambda message: message.text == '🚦 Live Traffic')
def handle_live_traffic(message):
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute("""
            SELECT street_name, borough, current_speed, traffic_status, last_updated, latitude, longitude ,event_time
            FROM gold_traffic_realtime 
            WHERE traffic_status LIKE '%Heavy%' ORDER BY current_speed ASC LIMIT 3
        """)
        rows = cur.fetchall()
        cur.close(); conn.close()
        
        res = "🚦 *HEAVY TRAFFIC HOTSPOTS*\n━━━━━━━━━━━━━━━━━━━━\n\n"
        for row in rows:
            event_time_formatted = row[7].strftime('%H:%M') if row[7] else "N/A"
            map_url = get_map_link(row[5], row[6])
            res += f"📍 *{row[0]}* ({row[1]})\n"
            res += f"↳ Status: {row[3]} | Speed: `{row[2]} mph`\n"
            res += f"🕒 *Measured At:* `{event_time_formatted}`\n"
            if map_url: res += f"🔗 [Google Maps]({map_url})\n"
            res += "\n"
        bot.send_message(message.chat.id, res, parse_mode='Markdown', disable_web_page_preview=True)
    except:
        bot.send_message(message.chat.id, "❌ Error fetching traffic.")

# ==========================================
# 7. 311 CITY COMPLAINTS PULSE
# ==========================================

@bot.message_handler(func=lambda message: message.text == '🧹 311 City Pulse')
def handle_311_pulse(message):
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        query = """
            SELECT DISTINCT ON (borough)
                borough, 
                complaint_type, 
                complaint_count, 
                peak_hour, 
                latitude, 
                longitude
            FROM gold_311_stats
            WHERE borough IS NOT NULL 
              AND borough != ''
            ORDER BY borough, complaint_count DESC
        """
        
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        if not rows:
            bot.send_message(message.chat.id, "📭 No 311 data found.")
            return

        res = "🧹 *NYC 311 - TOP COMPLAINTS BY BOROUGH*\n━━━━━━━━━━━━━━━━━━━━\n\n"
        for row in rows:
            boro = row[0]
            complaint = row[1]
            count = int(row[2]) if row[2] else 0
            peak = int(row[3]) if row[3] else 0
            lat = row[4]
            lon = row[5]
            
            map_url = get_map_link(lat, lon)
            
            res += f"🏙️ *{boro}*\n"
            res += f"↳ Issue: `{complaint}`\n"
            res += f"↳ Reports: `{count:,}`\n"
            res += f"↳ Peak Hour: `{peak:02d}:00`\n"
            if map_url:
                res += f"🔗 [Area Map]({map_url})\n"
            res += "────────────────────\n"
            
        bot.send_message(message.chat.id, res, parse_mode='Markdown', disable_web_page_preview=True)

    except Exception as e:
        print(f"DEBUG 311 ERROR: {e}")
        bot.send_message(message.chat.id, "❌ Error: Metadata mismatch in 311 table.")

# ==========================================
# 8. STREET-LEVEL UNIFIED SEARCH
# ==========================================

@bot.message_handler(func=lambda message: message.text == '🔍 Search Street')
def ask_for_street(message):
    msg = bot.send_message(message.chat.id, "📍 *Enter street keywords (e.g., 'Broadway'):*", parse_mode='Markdown')
    bot.register_next_step_handler(msg, process_street_search)

def process_street_search(message):
    user_input = message.text.strip()
    if not user_input:
        bot.send_message(message.chat.id, "Please enter a valid street name.")
        return

    params = [user_input.upper()]
    where_clause = "UPPER(street_name) = %s"

    try:
        conn = get_connection(); cur = conn.cursor()
        
        # שלב 1: מציאת שילובים מדויקים מכל הטבלאות הרלוונטיות
        cur.execute(f"""
            SELECT DISTINCT street_name, borough 
            FROM gold_311_stats 
            WHERE ({where_clause}) AND borough IS NOT NULL AND borough != 'None' AND borough != ''
            UNION
            SELECT DISTINCT street_name, borough 
            FROM gold_traffic_violations 
            WHERE ({where_clause}) AND borough IS NOT NULL AND borough != 'None' AND borough != ''
            UNION
            SELECT DISTINCT street_name, borough 
            FROM gold_crash_stats 
            WHERE ({where_clause}) AND borough IS NOT NULL AND borough != 'None' AND borough != ''
            UNION
            SELECT DISTINCT street_name, borough 
            FROM gold_traffic_realtime 
            WHERE ({where_clause}) AND borough IS NOT NULL AND borough != 'None' AND borough != ''
            LIMIT 5
        """, params * 4) # הכפלת הפרמטרים עבור 4 ה-SELECTs
        
        street_combos = cur.fetchall()

        if not street_combos:
            bot.send_message(message.chat.id, f"❌ No exact match found for: '{user_input.upper()}'")
            return

        for s_name, boro in street_combos:
            specific_params = [s_name, boro]
            match_cond = "street_name = %s AND borough = %s"

            # 1. שליפת תנועה
            cur.execute(f"SELECT current_speed, traffic_status, event_time, latitude, longitude FROM gold_traffic_realtime WHERE {match_cond} LIMIT 1", specific_params)
            traf = cur.fetchone()

            # 2. שליפת 311
            cur.execute(f"SELECT complaint_type, complaint_count, peak_hour, latitude, longitude FROM gold_311_stats WHERE {match_cond} ORDER BY complaint_count DESC LIMIT 1", specific_params)
            c311 = cur.fetchone()

            # 3. שליפת עבירות חניה
            cur.execute(f"""
                SELECT violation_desc, tickets_count, risk_level, is_camera, lat, lon 
                FROM gold_traffic_violations 
                WHERE {match_cond} 
                ORDER BY tickets_count DESC LIMIT 1
            """, specific_params)
            violation = cur.fetchone()

            # 4. שליפת תאונות (CRASH) - התוספת החדשה
            cur.execute(f"""
                SELECT total_crashes, total_injured, total_killed, main_cause, safety_label, latitude, longitude 
                FROM gold_crash_stats 
                WHERE {match_cond} 
                ORDER BY total_crashes DESC LIMIT 1
            """, specific_params)
            crash = cur.fetchone()

            if not any([traf, c311, violation, crash]):
                continue

            # קביעת מיקום למפה (העדפה: תנועה -> 311 -> עבירות -> תאונות)
            final_lat, final_lon = None, None
            if traf and traf[3]: final_lat, final_lon = traf[3], traf[4]
            elif c311 and c311[3]: final_lat, final_lon = c311[3], c311[4]
            elif violation and violation[4]: final_lat, final_lon = violation[4], violation[5]
            elif crash and crash[5]: final_lat, final_lon = crash[5], crash[6]

            map_url = get_map_link(final_lat, final_lon)

            # בניית הדוח
            res = f"📊 *STREET REPORT: {s_name.upper()}*\n"
            res += f"🏙️ *Borough:* `{boro}`\n"
            res += "━━━━━━━━━━━━━━━━━━━━\n\n"
            
            if traf:
                time_str = traf[2].strftime('%H:%M') if traf[2] else "N/A"
                res += f"🚗 *TRAFFIC*\n• Speed: `{traf[0]} mph` | `{traf[1]}`\n• At: `{time_str}`\n\n"
            
            if c311:
                peak = f"{int(c311[2]):02d}:00" if c311[2] is not None else "N/A"
                res += f"🧹 *311 PULSE*\n• Issue: `{c311[0]}` ({int(c311[1])})\n• Peak: `{peak}`\n\n"
            
            if violation:
                cam_status = "📸 Camera Detected" if violation[3] == 'Yes' else "👮 Manual Enforcement"
                res += f"🎫 *VIOLATIONS*\n"
                res += f"• Top: `{violation[0]}`\n"
                res += f"• Risk: `{violation[2]}`\n"
                res += f"• Type: `{cam_status}`\n\n"

            if crash:
                res += f"💥 *CRASH HISTORY*\n"
                res += f"• Safety: `{crash[4]}`\n"
                res += f"• Total Crashes: `{crash[0]}`\n"
                res += f"• Injured: `{crash[1]}` | Killed: `{crash[2]}`\n"
                res += f"• Main Cause: `{crash[3]}`\n\n"
            
            if map_url:
                res += f"🔗 [View Street on Map]({map_url})\n\n"
                
            res += "━━━━━━━━━━━━━━━━━━━━"
            bot.send_message(message.chat.id, res, parse_mode='Markdown', disable_web_page_preview=True)
        
        cur.close(); conn.close()

    except Exception as e:
        print(f"Search Error: {e}")
        bot.send_message(message.chat.id, "❌ Error fetching data during search.")

# ==========================================
# 9. MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    print("🚀 Bot is LIVE with Borough-wide 311 and Unified Street Search...")
    bot.polling(none_stop=True)