import sqlite3

conn = sqlite3.connect("app.db")
cursor = conn.cursor()

columns = [
    ("api_username", "ALTER TABLE don_vi ADD COLUMN api_username VARCHAR(100)"),
    ("api_password", "ALTER TABLE don_vi ADD COLUMN api_password VARCHAR(100)"),
    ("api_base_url", "ALTER TABLE don_vi ADD COLUMN api_base_url VARCHAR(255)"),
    ("api_uuid", "ALTER TABLE don_vi ADD COLUMN api_uuid TEXT"),
    ("api_his_token", "ALTER TABLE don_vi ADD COLUMN api_his_token TEXT"),
    ("api_token_expire_at", "ALTER TABLE don_vi ADD COLUMN api_token_expire_at DATETIME"),
]

for col_name, sql in columns:
    try:
        cursor.execute(sql)
        print(f"Added: {col_name}")
    except Exception as e:
        print(f"Skip {col_name}: {e}")

conn.commit()
conn.close()

print("Done.")