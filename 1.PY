# import telebot
# import psycopg2
# from telebot import types

# TOKEN = "7116435990:AAE23VMeB2d0yWR02GBYCaWDSN_Pi1K0uPk"

# DB_CONFIG = {
#     "dbname": "nyc_data",
#     "user": "postgres",
#     "password": "postgres",
#     "host": "postgres", 
#     "port": "5432"
# }

# def get_connection():
#     return psycopg2.connect(**DB_CONFIG)

# def fetch_category_stats(category_name):
#     """שליפת נתונים מתקדמת כולל אחוזים וחישובים"""
#     try:
#         conn = get_connection()
#         cur = conn.cursor()
        
#         # 1. סה"כ דוחות כללי (בכל העיר) לצורך חישוב אחוזים
#         cur.execute("SELECT SUM(tickets_count) FROM gold_traffic_violations")
#         grand_total = cur.fetchone()[0] or 1 # מניעת חילוק ב-0

#         # 2. נתונים ספציפיים לקטגוריה (מצלמות או חניה)
#         cur.execute("""
#             SELECT 
#                 SUM(tickets_count), 
#                 COUNT(DISTINCT street_name),
#                 TO_CHAR(MIN(last_updated), 'DD/MM/YY'),
#                 TO_CHAR(MAX(last_updated), 'DD/MM/YY')
#             FROM gold_traffic_violations 
#             WHERE category = %s
#         """, (category_name,))
#         cat_base = cur.fetchone()
        
#         if not cat_base or not cat_base[0]:
#             return None

#         cat_total = cat_base[0]
#         cat_pct_of_total = (cat_total / grand_total) * 100

#         # 3. העבירה המובילה בתוך הקטגוריה
#         cur.execute("""
#             SELECT violation_desc, SUM(tickets_count)
#             FROM gold_traffic_violations
#             WHERE category = %s
#             GROUP BY 1 ORDER BY 2 DESC LIMIT 1
#         """, (category_name,))
#         top_violation = cur.fetchone()
        
#         top_viol_desc = top_violation[0] if top_violation else "N/A"
#         top_viol_count = top_violation[1] if top_violation else 0
#         top_viol_pct = (top_viol_count / cat_total) * 100 if cat_total > 0 else 0

#         cur.close()
#         conn.close()
        
#         return {
#             "cat_total": cat_total,
#             "cat_pct": round(cat_pct_of_total, 1),
#             "streets": cat_base[1],
#             "period": f"{cat_base[2]} - {cat_base[3]}",
#             "top_desc": top_viol_desc,
#             "top_count": top_viol_count,
#             "top_pct": round(top_viol_pct, 1)
#         }
#     except Exception as e:
#         print(f"❌ Error: {e}")
#         return None

# # --- ממשק הבוט ---
# bot = telebot.TeleBot(TOKEN)

# @bot.message_handler(commands=['start'])
# def send_welcome(message):
#     markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
#     # הוספת הכפתור החדש לדוחות חניה
#     markup.add(types.KeyboardButton('📸 Camera Tickets'), types.KeyboardButton('🚗 Parking Tickets'))
#     markup.add(types.KeyboardButton('🔍 Search Street'))
    
#     bot.send_message(message.chat.id, "🗽 NYC Traffic & Parking Analytics\nSelect a category to view summary:", reply_markup=markup)

# @bot.message_handler(func=lambda message: True)
# def handle_message(message):
#     if message.text in ['📸 Camera Tickets', '🚗 Parking Tickets']:
#         category = 'Camera' if 'Camera' in message.text else 'Parking'
#         title = "CAMERA SUMMARY" if category == 'Camera' else "PARKING SUMMARY"
#         icon = "📸" if category == 'Camera' else "🚗"
        
#         data = fetch_category_stats(category)
        
#         if data:
#             res = (
#                 f"{icon} *NYC {title}*\n"
#                 "━━━━━━━━━━━━━━━━━━━━\n"
#                 f"🎫 *Total Tickets:* {int(data['cat_total']):,}\n"
#                 f"📊 *Share of Total:* {data['cat_pct']}%\n"
#                 f"📍 *Streets Monitored:* {data['streets']:,}\n"
#                 f"🗓 *Period:* {data['period']}\n\n"
#                 f"🏆 *Top Violation:* \n"
#                 f"└ {data['top_desc']}\n"
#                 f"└ *Count:* {int(data['top_count']):,} ({data['top_pct']}% of category)"
#             )
#             bot.send_message(message.chat.id, res, parse_mode='Markdown')
#         else:
#             bot.send_message(message.chat.id, f"⚠️ No {category} data found.")

#     elif message.text == '🔍 Search Street':
#         bot.send_message(message.chat.id, "Please type the street name:")

#     else:
#         # לוגיקת החיפוש הקיימת שלך (Broadway וכו')
#         pass

# if __name__ == "__main__":
#     print("🤖 Analytics Bot is running...")
#     bot.polling(none_stop=True)
# import telebot
# import psycopg2
# from telebot import types

# # --- הגדרות ---
# TOKEN = "7116435990:AAE23VMeB2d0yWR02GBYCaWDSN_Pi1K0uPk"

# DB_CONFIG = {
#     "dbname": "nyc_data",
#     "user": "postgres",
#     "password": "postgres",
#     "host": "postgres", 
#     "port": "5432",
#     "connect_timeout": 5
# }

# def get_connection():
#     return psycopg2.connect(**DB_CONFIG)

# # --- פונקציות שליפת נתונים ---

# def fetch_traffic_safety_data():
#     """שליפת נתוני מהירות, תאונות ועומס"""
#     print("🔍 Fetching stats from DB (Traffic & Safety)...")
#     try:
#         conn = get_connection()
#         cur = conn.cursor()
        
#         # שאילתה שמביאה את כל נתוני הבטיחות
#         cur.execute("""
#             SELECT 
#                 COALESCE(ROUND(AVG(speed), 1), 0) as avg_speed,
#                 COALESCE(SUM(total_crashes), 0) as total_accidents,
#                 (SELECT traffic_level FROM gold_traffic_safety_stats ORDER BY data_time DESC LIMIT 1) as load,
#                 TO_CHAR(MAX(data_time), 'DD/MM/YY HH24:MI') as last_upd
#             FROM gold_traffic_safety_stats;
#         """)
#         res = cur.fetchone()
#         cur.close()
#         conn.close()
        
#         if res and res[3]: # בודק אם יש תאריך עדכון (סימן שיש נתונים)
#             return {
#                 "speed": res[0],
#                 "accidents": res[1],
#                 "load": res[2] or "Normal",
#                 "updated": res[3]
#             }
#         return None
#     except Exception as e:
#         print(f"❌ Traffic DB Error: {e}")
#         return None

# def fetch_enhanced_stats(category_name):
#     """שליפת נתוני אכיפה (מצלמות/חניה) כולל מפות"""
#     print(f"🔍 Fetching stats from DB ({category_name} Summary)...")
#     try:
#         conn = get_connection()
#         cur = conn.cursor()
        
#         # 1. סה"כ כללי לאחוזים
#         cur.execute("SELECT SUM(tickets_count) FROM gold_traffic_violations")
#         grand_total = cur.fetchone()[0] or 1

#         # 2. סיכום קטגוריה ותקופה
#         cur.execute("""
#             SELECT 
#                 SUM(tickets_count), 
#                 COUNT(DISTINCT street_name),
#                 TO_CHAR(MIN(start_date), 'DD/MM/YY'), 
#                 TO_CHAR(MAX(end_date), 'DD/MM/YY')
#             FROM gold_traffic_violations 
#             WHERE category = %s
#         """, (category_name,))
#         base = cur.fetchone()
        
#         if not base or not base[0]: return None
#         cat_total = base[0]

#         # 3. העבירה המובילה
#         cur.execute("""
#             SELECT violation_desc, SUM(tickets_count)
#             FROM gold_traffic_violations WHERE category = %s
#             GROUP BY 1 ORDER BY 2 DESC LIMIT 1
#         """, (category_name,))
#         top_viol = cur.fetchone()

#         # 4. שלושת הרחובות המובילים + קואורדינטות למפות
#         cur.execute("""
#             SELECT street_name, SUM(tickets_count), MAX(lat), MAX(lon)
#             FROM gold_traffic_violations WHERE category = %s
#             GROUP BY 1 ORDER BY 2 DESC LIMIT 3
#         """, (category_name,))
#         top_streets = cur.fetchall()

#         cur.close()
#         conn.close()
        
#         return {
#             "total": cat_total,
#             "pct_of_grand": round((cat_total / grand_total) * 100, 1),
#             "streets_monitored": base[1],
#             "period": f"{base[2]} - {base[3]}",
#             "top_viol_desc": top_viol[0],
#             "top_viol_count": top_viol[1],
#             "top_viol_pct": round((top_viol[1] / cat_total) * 100, 1),
#             "top_streets": top_streets
#         }
#     except Exception as e:
#         print(f"❌ Violations DB Error: {e}")
#         return None

# # --- ניהול הבוט ---
# bot = telebot.TeleBot(TOKEN)

# @bot.message_handler(commands=['start'])
# def send_welcome(message):
#     markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
#     markup.add(types.KeyboardButton('🚨 Traffic & Safety'))
#     markup.add(types.KeyboardButton('📸 Camera Summary'), types.KeyboardButton('🚗 Parking Summary'))
#     markup.add(types.KeyboardButton('🔍 Search Street'))
#     bot.send_message(message.chat.id, "🗽 NYC Traffic Bot\nSelect a dashboard:", reply_markup=markup)

# @bot.message_handler(func=lambda message: message.text == '🚨 Traffic & Safety')
# def handle_traffic(message):
#     data = fetch_traffic_safety_data()
#     if data:
#         res = (
#             "🚨 *NYC TRAFFIC & SAFETY REPORT*\n"
#             "━━━━━━━━━━━━━━━━━━━━\n"
#             f"🏎 *Avg Speed:* {data['speed']} mph\n"
#             f"⚠️ *Total Crashes:* {int(data['accidents']):,}\n"
#             f"📊 *Traffic Load:* {data['load']}\n"
#             f"🕒 *Last Updated:* {data['updated']}\n"
#         )
#         bot.send_message(message.chat.id, res, parse_mode='Markdown')
#     else:
#         bot.send_message(message.chat.id, "🚧 Traffic data is currently being synced from Spark...")

# @bot.message_handler(func=lambda message: message.text in ['📸 Camera Summary', '🚗 Parking Summary'])
# def handle_summary(message):
#     category = 'Camera' if 'Camera' in message.text else 'Parking'
#     icon = "📸" if category == 'Camera' else "🚗"
    
#     data = fetch_enhanced_stats(category)
    
#     if data:
#         res = (
#             f"{icon} *NYC {category.upper()} REPORT*\n"
#             "━━━━━━━━━━━━━━━━━━━━\n"
#             f"🎫 *Total Tickets:* {int(data['total']):,}\n"
#             f"📊 *Share of Total:* {data['pct_of_grand']}%\n"
#             f"📍 *Streets Active:* {data['streets_monitored']:,}\n"
#             f"🗓 *Ticket Period:* {data['period']}\n\n"
            
#             f"🏆 *Top Violation:*\n"
#             f"└ {data['top_viol_desc']}\n"
#             f"└ {int(data['top_viol_count']):,}, ({data['top_viol_pct']}% of category)\n\n"
            
#             f"🚩 *Top 3 Hotspots:*\n"
#         )
        
#         for i, street in enumerate(data['top_streets'], 1):
#             s_name, s_count, s_lat, s_lon = street
#             # יצירת קישור למפות אם קיימות קואורדינטות
#             map_url = f"https://www.google.com/maps?q={s_lat},{s_lon}" if s_lat else None
#             map_text = f"🔗 [Map]({map_url})" if map_url else "📍 No GPS"
            
#             res += f"{i}. *{s_name}*\n   └ {int(s_count):,} tix | {map_text}\n"
            
#         bot.send_message(message.chat.id, res, parse_mode='Markdown', disable_web_page_preview=True)
#     else:
#         bot.send_message(message.chat.id, f"⚠️ No {category} data found.")

# if __name__ == "__main__":
#     print("🤖 Bot is starting...")
#     bot.polling(none_stop=True)
  
import telebot
import psycopg2
from telebot import types

TOKEN = "7116435990:AAE23VMeB2d0yWR02GBYCaWDSN_Pi1K0uPk"

DB_CONFIG = {
    "dbname": "nyc_data", "user": "postgres", "password": "postgres",
    "host": "postgres", "port": "5432"
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def fetch_street_search(query):
    """חיפוש רחוב מאוחד עם כל הנתונים שביקשת"""
    print(f"🔍 Deep Search for: {query}...")
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # 1. נתוני אכיפה (מתוך gold_traffic_violations)
        cur.execute("""
            SELECT street_name, violation_desc, tickets_count, lat, lon, borough
            FROM gold_traffic_violations 
            WHERE street_name ILIKE %s ORDER BY tickets_count DESC LIMIT 1
        """, (f"%{query}%",))
        viol = cur.fetchone()

        # 2. נתוני תנועה ובטיחות (מתוך gold_traffic_safety_stats)
        cur.execute("""
            SELECT link_name, speed, traffic_level, total_crashes, 
                   TO_CHAR(data_time, 'DD/MM/YY HH24:MI')
            FROM gold_traffic_safety_stats 
            WHERE link_name ILIKE %s ORDER BY data_time DESC LIMIT 1
        """, (f"%{query}%",))
        safe = cur.fetchone()

        cur.close()
        conn.close()
        return viol, safe
    except Exception as e:
        print(f"❌ DB Error: {e}")
        return None, None

# --- ניהול הבוט ---
bot = telebot.TeleBot(TOKEN)

@bot.message_handler(commands=['start'])
def send_welcome(message):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(types.KeyboardButton('🚨 Traffic & Safety'))
    markup.add(types.KeyboardButton('📸 Camera Summary'), types.KeyboardButton('🚗 Parking Summary'))
    markup.add(types.KeyboardButton('🔍 Search Street'))
    bot.send_message(message.chat.id, "🗽 NYC Traffic Bot\nSearch for a street or use the buttons below:", reply_markup=markup)

@bot.message_handler(func=lambda message: True)
def handle_all_messages(message):
    text = message.text

    # כאן יבואו ה-Handlers של הכפתורים (Summary) שכתבנו קודם...
    if text in ['🚨 Traffic & Safety', '📸 Camera Summary', '🚗 Parking Summary']:
        bot.send_message(message.chat.id, "📊 Generating summary report...")
        # (יש להוסיף כאן את הקריאה לפונקציות ה-Summary)

    elif text == '🔍 Search Street':
        bot.send_message(message.chat.id, "Please type the street name:")

    else:
        # לוגיקת החיפוש המורחבת
        viol, safe = fetch_street_search(text)

        if not viol and not safe:
            bot.reply_to(message, f"🔍 No data found for '{text}'.")
            return

        name = (viol[0] if viol else safe[0]).upper()
        res = f"📍 *REPORT: {name}*\n"
        res += "━━━━━━━━━━━━━━━━━━━━\n\n"

        # חלק 1: נתוני תנועה ובטיחות (מה שביקשת להוסיף)
        if safe:
            res += "🏎 *TRAFFIC & SAFETY STATUS:*\n"
            res += f"• *Avg Speed:* {safe[1]} mph\n"
            res += f"• *Traffic Load:* {safe[2]}\n"
            res += f"• *Recent Accidents:* {'🚨 Yes (' + str(safe[3]) + ')' if safe[3] > 0 else '✅ None'}\n"
            res += f"• *Last Update:* {safe[4]}\n\n"
        else:
            res += "🏎 *Traffic:* No real-time data found.\n\n"

        # חלק 2: נתוני אכיפה (הקיים)
        if viol:
            res += "📸 *ENFORCEMENT DATA:*\n"
            res += f"• *Top Violation:* {viol[1]}\n"
            res += f"• *Tickets Count:* {int(viol[2]):,}\n"
            res += f"• *Borough:* {viol[5]}\n"
            
            # לינק לגוגל מפות
            map_url = f"https://www.google.com/maps?q={viol[3]},{viol[4]}"
            res += f"\n🔗 [Open in Google Maps]({map_url})\n"
        
        bot.send_message(message.chat.id, res + "━━━━━━━━━━━━━━━━━━━━", parse_mode='Markdown')

if __name__ == "__main__":
    print("🤖 Bot is starting...")
    bot.polling(none_stop=True)