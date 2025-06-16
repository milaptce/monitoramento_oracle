# functions/utils.py

import os
import logging
import configparser
from datetime import datetime, timedelta
from sql_metadata import Parser

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# === Funções de análise SQL ===
def extract_tables(sql_text):
    """
    Extrai nomes das tabelas da query usando sql-metadata.
    Garante fallback seguro caso parsing falhe.
    """
    try:
        parser = Parser(sql_text)
        tables = parser.tables
        logger.debug(f"[extract_tables] Tabelas extraídas: {tables}")
        return tables
    except Exception as e:
        logger.warning(f"[WARNING] Erro ao extrair tabelas da query: {str(e)}")
        return []


def extract_where_conditions(sql_text):
    """
    Extrai condições do WHERE de forma segura.
    Usa .where ou retorna lista vazia em caso de falha.
    """
    try:
        parser = Parser(sql_text)
        where_conditions = parser.where if hasattr(parser, "where") else []
        logger.debug(f"[extract_where_conditions] Condições WHERE extraídas: {where_conditions}")
        return where_conditions
    except Exception as e:
        logger.warning(f"[WARNING] Erro ao extrair WHERE da query: {str(e)}")
        return []


def get_table_schema(connection, table_name):
    """
    Retorna o esquema (owner) da tabela no Oracle.
    """
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT owner FROM all_tables WHERE table_name = '{table_name.upper()}'")
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        logger.warning(f"[WARNING] Falha ao obter schema da tabela {table_name}: {e}")
        return None
    finally:
        cursor.close()


# === Controle de Execução ===
def check_first_run():
    """
    Verifica se é a primeira execução lendo/writing config/execucao.ini
    """
    config_file = "config/execucao.ini"
    config = configparser.ConfigParser()

    # Garantir diretório existe
    os.makedirs("config", exist_ok=True)

    if not os.path.exists(config_file):
        logger.info("[INFO] Primeira execução detectada. Criando execucao.ini...")
        config["Execution"] = {
            "FIRST_RUN": "1",
            "LAST_RUN_TIMESTAMP": datetime.now().isoformat()
        }
        with open(config_file, "w") as f:
            config.write(f)
        return True

    config.read(config_file)
    first_run = config.get("Execution", "FIRST_RUN", fallback="1")

    if first_run == "1":
        logger.info("[INFO] Execução inicial detectada.")
        config.set("Execution", "FIRST_RUN", "0")
        config.set("Execution", "LAST_RUN_TIMESTAMP", datetime.now().isoformat())
        with open(config_file, "w") as f:
            config.write(f)
        return True

    return False


def schedule_next_run(hours=6):
    """
    Agenda a próxima execução com base na periodicidade definida.
    """
    next_time = (datetime.now() + timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M")
    logger.info(f"🕒 Próxima execução agendada para {next_time}")
    print(f"🕒 Próxima execução agendada para {next_time}.")