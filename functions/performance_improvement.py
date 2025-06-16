# functions/performance_improvement.py
import cx_Oracle
from functions.login_db import connect_to_db
from functions.utils import check_first_run, schedule_next_run
from functions.table_analysis import classify_tables
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_existing_indexes(connection, table_name):
    """
    Verifica quais índices já existem para uma tabela.
    """
    cursor = connection.cursor()
    try:
        cursor.execute(f"""
        SELECT index_name FROM all_indexes 
        WHERE table_name = '{table_name}'
        """)
        indexes = [row[0] for row in cursor.fetchall()]
        return indexes
    except Exception as e:
        logger.warning(f"[WARNING] Falha ao listar índices da tabela {table_name}: {e}")
        return []
    finally:
        cursor.close()


def estimate_gain(query, table_size_mb):
    """
    Estima o ganho percentual de performance com base no tamanho da tabela e na query
    """
    if table_size_mb < 10:
        gain = 0.5  # Ganho médio para tabelas pequenas
    else:
        gain = 0.3  # Ganho menor, mas significativo para tabelas grandes

    return {
        "estimated_gain_percent": round(gain * 100, 2),
        "old_time": query.get("elapsed_time", 100),
        "new_time": round(query.get("elapsed_time", 100) * (1 - gain), 2),
        "schema": query.get("schema"),
        "sql_id": query.get("sql_id")
    }


def evaluate_performance(tables, queries=None):
    """
    Avalia impacto potencial de melhorias por tabela, considerando índices existentes
    
    Args:
        tables (dict): Dicionário com T1 e T2
        queries (list): Lista de dicionários com informações das queries FTS
    
    Returns:
        list: Sugestões de melhoria com pontuação e ação recomendada
    """
    connection = connect_to_db()
    if not connection:
        logger.error("❌ Falha na conexão ao Oracle. Cancelando análise de performance.")
        return []

    solutions = []

    # Processar tabelas T1
    for table_info in tables.get("T1", []):
        table_name = table_info["table"]
        size_mb = table_info["size_mb"]

        existing_indexes = check_existing_indexes(connection, table_name)

        solution = {
            "table": table_name,
            "size_mb": size_mb,
            "suggestion": "criar índice" if not existing_indexes else "índice já existe",
            "existing_indexes": existing_indexes,
            "priority_score": 8 if not existing_indexes else 2,
            "impact": "Alta" if not existing_indexes else "Baixa"
        }
        solutions.append(solution)

    # Processar tabelas T2
    for table_info in tables.get("T2", []):
        table_name = table_info["table"]
        size_mb = table_info["size_mb"]

        existing_indexes = check_existing_indexes(connection, table_info["table"])

        gain = 0
        relevant_queries = []

        if queries:
            relevant_queries = [q for q in queries if table_name in q.get("tables", [])]

        if relevant_queries:
            gain = sum(
                estimate_gain(q, size_mb).get("estimated_gain_percent", 0) for q in relevant_queries
            ) / len(relevant_queries)

        solution = {
            "table": table_name,
            "size_mb": size_mb,
            "suggestion": "refatorar query ou particionar" if existing_indexes else "considerar índice",
            "queries_affected": [q["sql_id"] for q in relevant_queries],
            "avg_gain_percent": round(gain, 2),
            "existing_indexes": existing_indexes,
            "priority_score": 9 if gain > 0 else 7,
            "impact": "Crítico" if gain > 0 else "Médio"
        }

        solutions.append(solution)

    connection.close()
    logger.info(f"💡 {len(solutions)} sugestões de melhoria geradas.")
    return solutions