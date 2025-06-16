# functions/schema_collector.py
import json
from functions.login_db import connect_to_db
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def collect_schemas():
    """
    Coleta os nomes de todos os esquemas (usuários) válidos do Oracle
    e salva em config/schemas.json
    """
    connection = connect_to_db()
    if not connection:
        logger.error("❌ Falha ao conectar ao banco. Cancelando coleta de esquemas.")
        return

    try:
        cursor = connection.cursor()

        # Query para listar usuários não mantidos pelo Oracle
        cursor.execute("""
            SELECT username FROM all_users 
            WHERE oracle_maintained = 'N' 
            ORDER BY username
        """)

        schemas = [row[0] for row in cursor.fetchall()]

        # Salvar no arquivo JSON
        with open("config/schemas.json", "w") as f:
            json.dump(schemas, f, indent=2)

        logger.info(f"✅ {len(schemas)} esquemas coletados e salvos em config/schemas.json")

    except Exception as e:
        logger.error(f"[ERRO] Ao coletar esquemas: {e}")
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    collect_schemas()