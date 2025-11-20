import time
import requests
import json
import mysql.connector

API_URL = "https://script.google.com/macros/s/AKfycbyIsDDrMIdZhtkiQEUUP1agWXaXtVccMYSfautGC6Dn51DpOq4A71CRJKW9gj4J6xuR/exec"

while True:
    print("\n=== Running SMS Sync Job ===")

    try:
        # 1) Fetch data
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()

        # 2) Connect to MySQL
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Vfpl@1234",
            database="SMS_DB"
        )
        cursor = conn.cursor()

        # 3) Ensure list
        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            rows = [data]
        else:
            rows = []

        print(f"Fetched {len(rows)} records from API")

        # 4) Queries
        insert_sql = """
            INSERT INTO sms_messages (timestamp_text, sender, name, message)
            VALUES (%s, %s, %s, %s)
        """

        check_sql = """
            SELECT COUNT(*)
            FROM sms_messages
            WHERE timestamp_text = %s
              AND sender = %s
              AND message = %s
        """

        # 5) Insert logic
        insert_count = 0
        skip_count = 0

        for row in rows:
            ts = row.get("timestamp")
            sender = row.get("sender")
            name = row.get("name") or ""
            msg = row.get("message")

            cursor.execute(check_sql, (ts, sender, msg))
            exists = cursor.fetchone()[0]

            if exists == 0:
                cursor.execute(insert_sql, (ts, sender, name, msg))
                insert_count += 1
                print(f"[INSERTED] {ts} | {sender}")
            else:
                skip_count += 1
                print(f"[SKIPPED - DUPLICATE] {ts} | {sender}")

        conn.commit()

        print("\n=== SUMMARY ===")
        print(f"Inserted: {insert_count}")
        print(f"Skipped: {skip_count}")

    except Exception as e:
        print("Error:", e)

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

    print("Waiting 30 seconds...\n")
    time.sleep(30)       # ⏳ waits 30 seconds before next run
