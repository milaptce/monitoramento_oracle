# functions/query_monitor.py

import logging
import hashlib
from functions.utils import extract_tables, extract_where_conditions, get_table_schema

logger = logging.getLogger(__name__)


def identify_fts_queries(connection):
    """
    Identifica queries FTS na view v$sql.
    Extrai informações como sql_id, sql_text, executions, elapsed_time, schema, tabelas e where_conditions.
    """
    cursor = connection.cursor()
    try:
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
                tables = extract_tables(row[1])
                where_conditions = extract_where_conditions(row[1])
                schema = get_table_schema(connection, tables[0]) if tables else None
            except Exception as e:
                logger.warning(f"[WARNING] Erro ao parsear query SQL: {str(e)}")
                tables = []
                where_conditions = []
                schema = None

            queries.append({
                "sql_id": row[0],
                "sql_text": row[1],
                "executions": row[2],
                "elapsed_time": row[3],
                "tables": tables,
                "where_conditions": where_conditions,
                "schema": schema
            })

        logger.info(f"🔍 {len(queries)} queries FTS identificadas.")
        return queries

    except Exception as e:
        logger.error(f"[ERRO] Ao buscar queries FTS: {e}")
        return []
    finally:
        cursor.close()


def group_similar_queries(queries):
    """
    Agrupa queries similares com base em hash MD5 das tabelas e condições do WHERE.
    """
    groups = {}

    for q in queries:
        # Garantir que sempre tenha valores padrão
        tables = q.get("tables", [])
        where_conditions = q.get("where_conditions", [])

        normalized = f"{tables} WHERE {where_conditions}"
        signature = hashlib.md5(normalized.encode()).hexdigest()

        if signature not in groups:
            groups[signature] = {
                "group_key": signature,
                "sample_sql": q["sql_text"],
                "normalized_key": normalized,
                "count": 0,
                "total_exec_time": 0,
                "avg_exec_time": 0.0,
                "tables": list(set(tables)) if tables else [],
                "schemas": set(),
                "where_conditions": list(set(where_conditions)) if where_conditions else [],
                "priority_score": 0
            }

        group = groups[signature]
        group["count"] += 1
        group["total_exec_time"] += q.get("elapsed_time", 0)
        group["avg_exec_time"] = round(group["total_exec_time"] / group["count"], 2) if group["count"] else 0
        if q.get("schema"):
            group["schemas"].add(q["schema"])

    # Calcular pontuação de prioridade
    grouped_queries = list(groups.values())
    for g in grouped_queries:
        g["priority_score"] = calculate_priority_score(g)

    logger.info(f"📊 {len(grouped_queries)} grupos de queries FTS identificados.")
    return grouped_queries


def calculate_priority_score(group):
    """
    Calcula pontuação de prioridade com base no uso do grupo de queries
    """
    weight_exec = group["count"] * 0.6
    weight_time = group["avg_exec_time"] * 0.4
    return round(weight_exec + weight_time, 2)