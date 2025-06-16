# functions/performance_improvement.py
import logging
from functions.login_db import connect_to_db

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
    Estima ganho percentual de performance com base no tamanho da tabela
    """
    if table_size_mb < 10:
        gain = 0.5  # Tabelas pequenas tendem a se beneficiar mais com índices
    else:
        gain = 0.3  # Tabelas grandes podem ter ganhos menores ou exigir particionamento

    return {
        "estimated_gain_percent": round(gain * 100, 2),
        "old_time": query.get("elapsed_time", 100),
        "new_time": round(query.get("elapsed_time", 100) * (1 - gain), 2),
        "table": query.get("table", "unknown"),
        "schema": query.get("schema", "unknown")
    }


def evaluate_performance(tables, queries=None):
    """
    Avalia impacto potencial de melhorias por tabela, considerando índices existentes
    """
    connection = connect_to_db()
    if not connection:
        logger.error("❌ Falha na conexão ao Oracle. Cancelando análise de performance.")
        return []

    solutions = []

    for table_info in tables.get("T1", []):
        table_name = table_info["table"]
        size_mb = table_info["size_mb"]

        existing_indexes = check_existing_indexes(connection, table_name)

        solution = {
            "table": table_name,
            "size_mb": size_mb,
            "suggestion": "criar índice" if not existing_indexes else "índice já existe",
            "existing_indexes": existing_indexes,
            "priority": 8 if not existing_indexes else 2,
            "impact": "Alta" if not existing_indexes else "Baixa"
        }
        solutions.append(solution)

    for table_info in tables.get("T2", []):
        table_name = table_info["table"]
        size_mb = table_info["size_mb"]

        existing_indexes = check_existing_indexes(connection, table_name)

        gain = 0
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
            "queries_affected": [q["sql_id"] for q in queries if table_name in q.get("tables", [])],
            "avg_gain_percent": round(gain, 2),
            "existing_indexes": existing_indexes,
            "priority": 9 if not existing_indexes else 7,
            "impact": "Crítico" if gain > 0 else "Médio"
        }
        solutions.append(solution)

    connection.close()
    logger.info(f"💡 {len(solutions)} sugestões de melhoria geradas.")

    return solutions