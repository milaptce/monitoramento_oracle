import cx_Oracle
import os
from dotenv import load_dotenv

load_dotenv()

def connect_to_db():
    user = os.getenv("ORACLE_USER")
    password = os.getenv("ORACLE_PASSWORD")
    dsn = os.getenv("ORACLE_DSN")

    try:
        connection = cx_Oracle.connect(user, password, dsn)
        print("Conexão ao banco de dados estabelecida.")
        return connection
    except cx_Oracle.DatabaseError as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
