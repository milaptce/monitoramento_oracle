# functions/query_monitor.py
import cx_Oracle
import logging
from sql_metadata import Parser
import hashlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def identify_fts_queries(connection):
    """
    Identifica queries FTS no v$sql e retorna listas normalizadas
    """
    try:
        cursor = connection.cursor()
        cursor.execute("""
        SELECT sql_id, sql_text, executions, elapsed_time
        FROM v$sql
        WHERE sql_text LIKE '%TABLE ACCESS FULL%' OR sql_text LIKE '%/*+ FULL(%'
        ORDER BY elapsed_time DESC
        """)
        rows = cursor.fetchall()

        queries = []
        for row in rows:
            try:
                parser = Parser(row[1])
                tables = parser.tables_uniq
                where_columns = parser.where_columns
                schema = get_table_schema(connection, tables[0]) if tables else None
            except:
                tables = []
                where_columns = []
                schema = None

            queries.append({
                "sql_id": row[0],
                "sql_text": row[1],
                "executions": row[2],
                "elapsed_time": row[3],
                "tables": tables,
                "where_columns": where_columns,
                "schema": schema
            })

        logger.info(f"🔍 {len(queries)} queries FTS identificadas.")
        return queries

    except Exception as e:
        logger.error(f"[ERRO] Ao buscar queries FTS: {e}")
        return []


def group_similar_queries(queries):
    """
    Agrupa queries com estruturas similares usando hash da assinatura SQL
    """
    groups = {}

    for q in queries:
        # Extrair elementos-chave da query
        try:
            parser = Parser(q["sql_text"])
            normalized = f"{parser.tables_uniq} WHERE {parser.where_columns}"
        except:
            normalized = "unknown"

        # Gerar hash como assinatura única
        signature = hashlib.md5(normalized.encode()).hexdigest()

        if signature not in groups:
            groups[signature] = {
                "group_key": signature,
                "sample_sql": q["sql_text"],
                "normalized_key": normalized,
                "count": 0,
                "total_exec_time": 0,
                "avg_exec_time": 0,
                "tables": list(set(parser.tables_uniq)) if parser.tables_uniq else [],
                "schemas": set(),
                "where_columns": where_columns or [],
                "priority_score": 0
            }

        # Atualizar dados do grupo
        group = groups[signature]
        group["count"] += 1
        group["total_exec_time"] += q["elapsed_time"]
        group["avg_exec_time"] = round(group["total_exec_time"] / group["count"], 2)
        group["schemas"].add(q["schema"])

    # Converter para lista e calcular pontuação de prioridade
    grouped_queries = list(groups.values())
    for g in grouped_queries:
        g["priority_score"] = calculate_priority_score(g)

    logger.info(f"📊 {len(grouped_queries)} grupos de queries FTS identificados.")
    return grouped_queries


def calculate_priority_score(group):
    """
    Calcula um score baseado em quantidade de execuções e tempo médio
    """
    weight_exec = group["count"] * 0.6
    weight_time = group["avg_exec_time"] * 0.4
    return round(weight_exec + weight_time, 2)


def get_table_schema(connection, table_name):
    """
    Retorna o owner (schema) de uma tabela
    """
    cursor = connection.cursor()
    try:
        cursor.execute(f"""
        SELECT owner FROM all_tables 
        WHERE table_name = '{table_name.upper()}'
        """)
        result = cursor.fetchone()
        return result[0] if result else None
    except:
        return None
    finally:
        cursor.close()