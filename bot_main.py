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

import pytz
from datetime import datetime, timedelta
import telebot
import psycopg2
from telebot import types
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

def get_map_link(street, borough):
    """Generates an official and reliable Google Maps search link"""
    clean_street = street.replace(' ', '+')
    clean_borough = borough.replace(' ', '+') if borough else "NYC"
    # Official Google Maps Search URL
    return f"https://www.google.com/maps/search/?api=1&query={clean_street}+{clean_borough}+New+York"

# --- DATA FETCHING FUNCTIONS ---

def fetch_311_detailed_report():
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute("SELECT MIN(data_time), MAX(data_time), SUM(complaint_count) FROM gold_311_stats")
        min_d, max_d, city_total = cur.fetchone()

        cur.execute("""
            SELECT DISTINCT ON (borough) borough, complaint_type, SUM(complaint_count) as vol, MAX(peak_hour)
            FROM gold_311_stats
            GROUP BY borough, complaint_type
            ORDER BY borough, vol DESC
        """)
        borough_issues = cur.fetchall()

        cur.execute("""
            WITH RankedStreets AS (
                SELECT borough, street_name, complaint_type, SUM(complaint_count) as street_sum,
                       MAX(peak_hour) as peak_h,
                       ROW_NUMBER() OVER(PARTITION BY borough ORDER BY SUM(complaint_count) DESC) as rank
                FROM gold_311_stats
                WHERE street_name NOT IN ('UNKNOWN', '', 'UNSPECIFIED')
                GROUP BY borough, street_name, complaint_type
            )
            SELECT borough, street_name, complaint_type, street_sum, peak_h
            FROM RankedStreets WHERE rank <= 2
            ORDER BY borough, street_sum DESC
        """)
        top_streets = cur.fetchall()
        cur.close(); conn.close()
        return min_d, max_d, city_total, borough_issues, top_streets
    except: return None
    
def fetch_traffic_safety_report():
    try:
        conn = get_connection(); cur = conn.cursor()
        
        # זמן סנכרון אחרון
        cur.execute("SELECT MIN(data_time), MAX(data_time) FROM gold_traffic_safety_stats")
        min_ts, latest_ts = cur.fetchone()
        
        # 3 הכבישים הכי איטיים - מושכים את שם הרחוב והרובע
        cur.execute("""
            SELECT DISTINCT ON (link_name) link_name, MAX(borough), AVG(speed), MAX(data_time)
            FROM gold_traffic_safety_stats 
            WHERE data_time >= %s - INTERVAL '1 hour' 
            GROUP BY link_name 
            ORDER BY link_name, AVG(speed) ASC LIMIT 3
        """, (latest_ts,))
        busiest = cur.fetchall()
        
        # 3 כבישים מסוכנים - הסרנו את ה-danger_rank
        cur.execute("""
            SELECT DISTINCT ON (total_crashes) link_name, borough, total_crashes, total_killed, total_injured, data_time,
                   safety_label
            FROM gold_traffic_safety_stats WHERE total_crashes > 0 
            ORDER BY total_crashes DESC LIMIT 3
        """)
        dangerous = cur.fetchall()
        
        cur.close(); conn.close()
        # מחזירים 5 ערכים בדיוק
        return min_ts, latest_ts, busiest, dangerous, latest_ts
    except Exception as e:
        print(f"Error in fetch_traffic: {e}")
        return None

# --- MESSAGE HANDLERS ---

@bot.message_handler(commands=['start'])
def send_welcome(message):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton('🚨 Traffic & Safety'), types.KeyboardButton('🧹 311 City Pulse'))
    markup.add(types.KeyboardButton('📸 Camera Summary'), types.KeyboardButton('🚗 Parking Summary'))
    markup.add(types.KeyboardButton('🔍 Search Street'))
    
    welcome_text = (
        "👋 *Welcome to the NYC Smart City Bot!*\n\n"
        "I am your real-time guide to New York City's data.\n\n"
        "*What can I do for you?*\n"
        "• 🚨 *Traffic & Safety:* Live speeds and accident reports.\n"
        "• 🧹 *311 City Pulse:* Neighborhood complaints and peak hours.\n"
        "• 📸 *Enforcement:* Camera and Parking ticket summaries.\n\n"
        "💡 *Pro Tip:* You can also just type any street name (e.g., *Broadway*) to get a full combined report!\n\n"
        "*Select an option below to start:*"
    )
    bot.send_message(message.chat.id, welcome_text, parse_mode='Markdown', reply_markup=markup)

@bot.message_handler(func=lambda message: True)
def handle_all_messages(message):
    text = message.text

    # --- 🧹 311 BUTTON ---
    if text == '🧹 311 City Pulse':
        data = fetch_311_detailed_report()
        if not data:
            bot.send_message(message.chat.id, "❌ No 311 data available."); return
        min_d, max_d, total, b_issues, s_issues = data
        res = "🧹 *311 CITY PULSE - DETAILED REPORT*\n"
        res += f"📅 *Period:* `{min_d} to {max_d}`\n\n"
        res += "🏢 *TOP ISSUE PER BOROUGH:*\n"
        for b in b_issues:
            peak = f"{int(b[3]):02d}:00" if b[3] is not None else "N/A"
            res += f"• *{b[0]}:* {b[1]} (Peak: {peak})\n"
        res += "\n📍 *TOP 2 STREETS PER BOROUGH:*\n"
        current_b = ""
        for s in s_issues:
            street_peak = f"{int(s[4]):02d}:00" if s[4] is not None else "N/A"
            if s[0] != current_b:
                current_b = s[0]; res += f"\n🏙 *{current_b}*\n"
            res += f" ↳ *{s[1]}* ({s[2]})\n    Count: `{int(s[3])}` | *Peak Hour: {street_peak}*\n    🔗 [Map]({get_map_link(s[1], s[0])})\n"
        bot.send_message(message.chat.id, res, parse_mode='Markdown', disable_web_page_preview=True)

    # --- 🚨 TRAFFIC & SAFETY BUTTON ---
    elif text == '🚨 Traffic & Safety':
        data = fetch_traffic_safety_report()
        if not data:
            bot.send_message(message.chat.id, "❌ No real-time traffic data available."); return
        
        min_ts, ts, busiest, dangerous, absolute_latest = data
        
        # עיצוב הכותרת
        res = f"🏎 *TRAFFIC & SAFETY REPORT*\n📅 `{min_ts.strftime('%d/%m')} - {ts.strftime('%d/%m/%Y')}`\n🕒 *Last Sync:* `{ts.strftime('%H:%M:%S')}`\n"
        res += "━━━━━━━━━━━━━━━━━━━━\n\n"
        
        # כבישים עמוסים - עם רובע בסוגריים ובלי Typical
        res += "🔥 *BUSIEST STREETS (SLOWEST):*\n"
        for b in busiest:
            res += f"• {b[0]} ({b[1]}): `{b[2]:.1f} mph`\n"
        
        # מקטעים מסוכנים - ללא Rank
        res += "\n⚠️ *DANGEROUS SEGMENTS:*\n"
        for d in dangerous:
            res += f"• {d[0]} ({d[1]})\n"
            res += f"  ↳ *{d[6]}*\n" 
            res += f"  ↳ 💥 `{int(d[2])}` Crashes | 🤕 Injured: `{int(d[4])}` | 🕯️ Fatal: `{int(d[3])}`\n"
        
        res += "\n━━━━━━━━━━━━━━━━━━━━"
        bot.send_message(message.chat.id, res, parse_mode='Markdown', disable_web_page_preview=True)
        
    # --- 🔍 SEARCH STREET ACTION ---
    elif text == '🔍 Search Street':
        bot.send_message(message.chat.id, "📍 Please type a street name and borough (e.g., *Broadway Manhattan*):")

    # --- COMBINED SEARCH LOGIC (STREET + BOROUGH + BASELINE) ---
# --- COMBINED SEARCH LOGIC (STREET + BOROUGH + BASELINE) ---
    elif text not in ['📸 Camera Summary', '🚗 Parking Summary']:
        try:
            conn = get_connection(); cur = conn.cursor()
            
            # א. זיהוי רובע מתוך הטקסט
            boroughs = ['Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island']
            search_street = text
            search_borough = None
            for b in boroughs:
                if b.lower() in text.lower():
                    search_borough = b
                    search_street = text.lower().replace(b.lower(), "").strip()
                    break

            # ב. שליפת נתונים - הוסר ה-danger_rank
            query = """
                SELECT speed, total_crashes, borough, data_time, total_injured, total_killed, 
                       safety_label, typical_speed 
                FROM gold_traffic_safety_stats 
                WHERE link_name ILIKE %s
            """
            params = [f"%{search_street}%"]
            if search_borough:
                query += " AND borough ILIKE %s"; params.append(f"%{search_borough}%")
            
            query += " ORDER BY total_crashes DESC, data_time DESC LIMIT 1"
            cur.execute(query, params)
            traf = cur.fetchone()
            
            # --- כאן התיקון שלנו (סעיף ג') ---
            # ג. שליפת נתוני 311 משולב רובע
            q_311 = """
                SELECT complaint_type, SUM(complaint_count), MAX(peak_hour), MAX(borough) 
                FROM gold_311_stats 
                WHERE street_name ILIKE %s
            """
            p_311 = [f"%{search_street}%"]
            
            if search_borough:
                q_311 += " AND borough ILIKE %s"
                p_311.append(f"%{search_borough}%")
            
            q_311 += " GROUP BY complaint_type ORDER BY 2 DESC LIMIT 1"
            
            cur.execute(q_311, p_311)
            c311 = cur.fetchone()
            # --- סוף התיקון ---
        
            if not c311 and not traf:
                bot.send_message(message.chat.id, f"🔍 No data found for '{text}'.")
            else:
                final_boro = (search_borough if search_borough else (traf[2] if traf else c311[3]))
                res = f"📍 *REPORT: {search_street.upper()}*\n🏙 Borough: `{final_boro}`\n\n"
                
                if traf:
                    # הוסר ה-rank מהפריקה כאן!
                    speed, crashes, boro, d_time, injured, killed, label, typical = traf
                    
                    ny_tz = pytz.timezone('America/New_York')
                    is_delayed = (datetime.now(pytz.utc) - d_time.replace(tzinfo=pytz.utc)).total_seconds() > 1800
                    
                    res += f"🏎 *TRAFFIC STATUS:*\n"
                    if is_delayed:
                        cur.execute("""
                            SELECT AVG(typical_speed)::INT FROM gold_traffic_safety_stats 
                            WHERE link_name ILIKE %s 
                              AND data_time::time BETWEEN 
                                  ((CURRENT_TIME AT TIME ZONE 'America/New_York') - INTERVAL '1 hour') 
                                  AND 
                                  ((CURRENT_TIME AT TIME ZONE 'America/New_York') + INTERVAL '30 minutes')
                        """, (f"%{search_street}%",))
                        win_avg = cur.fetchone()[0] or typical
                        res += f"🕒 *Status:* ⚠️ Live data delayed ({d_time.strftime('%H:%M')})\n"
                        res += f"💡 *Typical Speed Now:* `{win_avg} mph` (Historical)\n"
                    else:
                        flow = "⚠️ Heavy" if speed < typical * 0.7 else "✅ Normal"
                        res += f"• Speed: `{speed} mph` (*{flow}*)\n"
                    
                    res += f"\n⚠️ *SAFETY:* {label}\n"
                    # הוסרה שורת ההדפסה של ה-Rank
                    res += f"• 💥 Crashes: `{int(crashes)}` | 🤕 *Injured: {int(injured)}*\n"

                if c311:
                    peak = f"{int(c311[2]):02d}:00" if c311[2] is not None else "N/A"
                    res += f"\n🧹 *311 Pulse:* `{c311[0]}`\n  ↳ 📊 Reports: `{int(c311[1])}` | ⏰ Peak Hour: `{peak}`\n"

                res += f"\n🔗 [Open in Maps]({get_map_link(search_street, final_boro)})"
                bot.send_message(message.chat.id, res, parse_mode='Markdown', disable_web_page_preview=True)
            
            cur.close(); conn.close()
        except Exception as e:
            print(f"Error in Search: {e}")
            
if __name__ == "__main__":
    print("🚀 NYC Bot is LIVE with Accident Timestamps...")
    bot.polling(none_stop=True)