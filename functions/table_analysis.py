# table_analysis.py
from sql_metadata import Parser
import cx_Oracle
import logging

# Configuração de logging (opcional)
logging.basicConfig(level=logging.INFO)

def extract_tables_from_query(sql_text):
    """Extrai nomes de tabelas de uma query SQL."""
    try:
        parser = Parser(sql_text)
        return parser.tables_uniq
    except Exception as e:
        logging.warning(f"[extract_tables_from_query] Erro ao parsear query: {e}")
        return []

def classify_tables(queries, connection):
    """
    Classifica tabelas envolvidas nas queries FTS como T1 (< 10MB) ou T2 (>= 10MB)
    """
    cursor = connection.cursor()
    tables = set()

    for query in queries:
        try:
            tables_in_query = extract_tables_from_query(query.get("sql_text", ""))
            tables.update(tables_in_query)
        except Exception as e:
            logging.error(f"[classify_tables] Erro ao extrair tabelas da query: {e}")

    t1 = []
    t2 = []

    for table in tables:
        try:
            # Usamos bind variables para evitar SQL injection
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
                    t2.append({"table": table, "size_mb": size})
                logging.info(f"Tabela {table} classificada com {size:.2f} MB")
            else:
                logging.warning(f"Tabela {table} não encontrada no dba_segments.")

        except cx_Oracle.DatabaseError as e:
            logging.error(f"[classify_tables] Erro ao consultar tabela {table}: {e}")
        except Exception as e:
            logging.error(f"[classify_tables] Erro inesperado com {table}: {e}")

    cursor.close()
    return {"T1": t1, "T2": t2}