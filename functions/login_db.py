# functions/login_db.py
import cx_Oracle
import os
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

def connect_to_db() -> Optional[cx_Oracle.Connection]:
    """
    Conecta ao banco de dados Oracle usando credenciais do .env
    """
    try:
        # Carregar variáveis diretamente
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        service_name = os.getenv("DB_SERVICE_NAME")
        user = os.getenv("DB_USERNAME")
        password = os.getenv("DB_PASSWORD")

        if not all([host, port, service_name, user, password]):
            print("❌ Credenciais incompletas no .env")
            return None

        # Montar DSN dinamicamente
        dsn = cx_Oracle.makedsn(host, port, service_name=service_name)

        connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)
        print("✅ Conexão estabelecida.")
        return connection

    except cx_Oracle.DatabaseError as e:
        print(f"❌ Erro na conexão Oracle: {e}")
        return None