# functions/table_analysis.py

import cx_Oracle
import logging
from functions.utils import extract_tables  # Certifique-se que esta função existe no utils.py

# Configuração de logging
logger = logging.getLogger(__name__)


def classify_tables(queries, connection):
    """
    Classifica as tabelas envolvidas nas queries FTS em T1 (<10MB) ou T2 (>=10MB)

    Args:
        queries (list): Lista de dicionários com queries FTS
        connection (cx_Oracle.Connection): Conexão Oracle já estabelecida

    Returns:
        dict: {"T1": [...], "T2": [...]}
    """
    cursor = None
    tables = set()

    for query in queries:
        try:
            tables_in_query = extract_tables(query.get("sql_text", ""))
            tables.update(tables_in_query)
        except Exception as e:
            logger.warning(f"[classify_tables] Erro ao extrair tabelas da query: {e}")

    t1 = []
    t2 = []

    if not tables:
        logger.info("⚠️ Nenhuma tabela encontrada nas queries FTS.")
        return {"T1": t1, "T2": t2}

    cursor = connection.cursor()
    for table in tables:
        try:
            # Usando bind variables para evitar SQL injection
            cursor.execute("""
                SELECT bytes / 1024 / 1024 AS size_mb
                FROM dba_segments
                WHERE segment_name = UPPER(:table_name) AND segment_type = 'TABLE'
            """, table_name=table)

            result = cursor.fetchone()
            if result:
                size = float(result[0])
                if size < 10:
                    t1.append({"table": table, "size_mb": size})
                else:
                    t2.append({"table": table, "size_mb": size)
                logger.debug(f"Tabela {table} classificada com {size:.2f} MB")
            else:
                logger.warning(f"Tabela {table} não encontrada no dba_segments.")

        except cx_Oracle.DatabaseError as e:
            logger.error(f"[classify_tables] Erro ao consultar tabela {table}: {e}")
        except Exception as e:
            logger.error(f"[classify_tables] Erro inesperado com {table}: {e}")

    if cursor:
        cursor.close()

    logger.info(f"📊 Tabelas classificadas: T1={len(t1)}, T2={len(t2)}")
    return {"T1": t1, "T2": t2}