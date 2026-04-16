import pytz
import telebot
import psycopg2
from telebot import types
from datetime import datetime, timedelta

# --- CONFIGURATION ---
TOKEN = "7116435990:AAE23VMeB2d0yWR02GBYCaWDSN_Pi1K0uPk"
bot = telebot.TeleBot(TOKEN)

# הגדרות חיבור - host='postgres' עבור Docker
DB_CONFIG = {
    "dbname": "nyc_data", 
    "user": "postgres", 
    "password": "postgres",
    "host": "postgres", 
    "port": "5432"
}




def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def to_nyc(dt):
    if not dt:
        return None
    utc = pytz.utc
    nyc = pytz.timezone('America/New_York')

    if dt.tzinfo is None:
        dt = utc.localize(dt)

    return dt.astimezone(nyc)

def fetch_data():
    try:
        conn = get_connection()
        cur = conn.cursor()

        # META
        cur.execute("SELECT MAX(data_time), MAX(last_updated) FROM gold_traffic_stats")
        meta = cur.fetchone()

        # ✅ SMART QUERY (fallback)
        cur.execute("""
            WITH recent AS (
                SELECT *
                FROM (
                    SELECT DISTINCT ON (link_name)
                        link_name,
                        borough,
                        speed,
                        traffic_level,
                        data_time
                    FROM gold_traffic_stats
                    WHERE speed > 0
                      AND data_time > NOW() - INTERVAL '3 hours'
                    ORDER BY link_name, data_time DESC
                ) t
            ),
            fallback AS (
                SELECT *
                FROM (
                    SELECT DISTINCT ON (link_name)
                        link_name,
                        borough,
                        speed,
                        traffic_level,
                        data_time
                    FROM gold_traffic_stats
                    WHERE speed > 0
                    ORDER BY link_name, data_time DESC
                ) t
            )
            SELECT * FROM recent
            UNION ALL
            SELECT * FROM fallback
            WHERE NOT EXISTS (SELECT 1 FROM recent)
            ORDER BY speed ASC
            LIMIT 3
        """)
        traffic = cur.fetchall()

        # SAFETY
        cur.execute("""
            SELECT street_name, borough, total_crashes, total_injured, total_killed, main_cause
            FROM gold_crash_stats
            WHERE street_name IS NOT NULL
              AND street_name NOT IN ('None','UNKNOWN','')
            ORDER BY total_crashes DESC
            LIMIT 3
        """)
        safety = cur.fetchall()

        cur.close()
        conn.close()

        return meta, traffic, safety

    except Exception as e:
        print("DB ERROR:", e)
        return None, None, None

@bot.message_handler(commands=['start'])
def start(message):
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(types.KeyboardButton('🚨 Traffic & Safety'))
    bot.send_message(message.chat.id, "NYC Traffic Bot Ready", reply_markup=kb)

@bot.message_handler(func=lambda m: m.text == '🚨 Traffic & Safety')
def report(message):

    meta, traffic, safety = fetch_data()

    if not traffic:
        bot.send_message(message.chat.id, "❌ No data available at all")
        return

    nyc = pytz.timezone('America/New_York')
    now = datetime.now(nyc)

    last_sync = to_nyc(meta[1])

    res = "🏎 NYC TRAFFIC REPORT\n"
    res += f"🕒 Last Sync: {last_sync.strftime('%H:%M:%S')}\n"
    res += f"🏙 Current Time: {now.strftime('%H:%M:%S')}\n"
    res += "━━━━━━━━━━━━━━━━━━━━\n\n"

    res += "📊 TRAFFIC OVERVIEW\n\n"

    for t in traffic:
        link, boro, speed, level, d_time = t

        boro = boro if boro else "Unknown"
        d_time_nyc = to_nyc(d_time)

        res += f"• {link} ({boro})\n"
        res += f"  ↳ {speed} mph (at {d_time_nyc.strftime('%H:%M')})\n"
        res += f"  ↳ {level}\n"

    res += "\n⚠️ MOST DANGEROUS STREETS\n\n"

    for s in safety:
        name, boro, crashes, injured, killed, cause = s

        boro = boro if boro else "Unknown"

        res += f"• {name} ({boro})\n"
        res += f"  ↳ {int(crashes)} crashes\n"
        res += f"  ↳ {int(injured)} injured | {int(killed)} killed\n"
        res += f"  ↳ Cause: {cause}\n"

    bot.send_message(message.chat.id, res)

if __name__ == "__main__":
    print("BOT RUNNING...")
    bot.polling(none_stop=True)