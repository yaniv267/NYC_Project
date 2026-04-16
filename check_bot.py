import psycopg2

DB_CONFIG = {
    "dbname": "nyc_data", 
    "user": "postgres", 
    "password": "postgres",
    "host": "localhost", # שנה ל-postgres אם אתה בתוך Docker
    "port": "5432"
}

def run_check():
    try:
        print(f"🔍 מנסה להתחבר לבסיס הנתונים '{DB_CONFIG['dbname']}'...")
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("✅ חיבור הצליח!\n")

        # רשימת הטבלאות שאנחנו מצפים לראות
        tables = [
            "gold_traffic_stats", 
            "gold_crash_stats", 
            "gold_311_stats", 
            "gold_camera_stats", 
            "gold_parking_stats"
        ]

        print("📊 מצב הטבלאות:")
        print("-" * 30)
        
        for table in tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                
                # בדיקת זמן עדכון אחרון אם קיים
                last_upd = "N/A"
                try:
                    cur.execute(f"SELECT MAX(last_updated) FROM {table}")
                    last_upd = cur.fetchone()[0]
                except:
                    pass
                
                print(f"📋 {table:20} | שורות: {count:6} | עדכון אחרון: {last_upd}")
            except Exception as e:
                print(f"❌ {table:20} | שגיאה: הטבלה לא קיימת או ריקה")
                conn.rollback() # מחזיר את הטרנזקציה למצב תקין

        cur.close()
        conn.close()
        print("-" * 30)
        print("\n🚀 סיום בדיקה.")

    except Exception as e:
        print(f"🚨 שגיאת חיבור כללית: {e}")

if __name__ == "__main__":
    run_check()