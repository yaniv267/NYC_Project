import psycopg2

# הגדרות החיבור שלך
DB_CONFIG = {
    "dbname": "nyc_data",
    "user": "postgres",
    "password": "postgres",
    "host": "postgres",
    "port": "5432"
}

def diagnose():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        print("🔍 --- אבחון בסיס נתונים ---")
        
        # 1. בדיקת קיום הטבלה
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'gold_traffic_violations');")
        exists = cur.fetchone()[0]
        print(f"1. האם הטבלה קיימת? {'✅ כן' if exists else '❌ לא'}")
        
        if exists:
            # 2. כמות שורות כוללת
            cur.execute("SELECT COUNT(*) FROM gold_traffic_violations;")
            count = cur.fetchone()[0]
            print(f"2. כמות שורות בטבלה: {count}")
            
            # 3. בדיקת שמות עמודות (אולי יש טעות בשם?)
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'gold_traffic_violations';")
            cols = [row[0] for row in cur.fetchall()]
            print(f"3. עמודות שמצאתי: {', '.join(cols)}")
            
            # 4. בדיקת הקטגוריות (חשוב מאוד!)
            cur.execute("SELECT DISTINCT category FROM gold_traffic_violations;")
            categories = [row[0] for row in cur.fetchall()]
            print(f"4. קטגוריות קיימות (category): {categories}")
            
            # 5. דגימה של שורה אחת
            cur.execute("SELECT * FROM gold_traffic_violations LIMIT 1;")
            sample = cur.fetchone()
            print(f"5. דגימת נתונים (שורה ראשונה): {sample}")

        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ שגיאת חיבור: {e}")

if __name__ == "__main__":
    diagnose()