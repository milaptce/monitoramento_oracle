# test_connection.py
from functions.login_db import connect_to_db

if __name__ == "__main__":
    conn = connect_to_db()
    if conn:
        print("✅ Conexão bem-sucedida!")
        conn.close()
    else:
        print("❌ Falha na conexão.")