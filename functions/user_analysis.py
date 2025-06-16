# functions/user_analysis.py
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def classify_schemas(grouped_queries, threshold_low=10, threshold_regular=100):
    """
    Classifica esquemas com base no número de queries FTS e tempo total gasto.
    
    Args:
        grouped_queries (list): Lista de grupos de queries FTS
        threshold_low (int): Limite inferior para esquema LOW
        threshold_regular (int): Limite para esquema REGULAR
    
    Returns:
        dict: Esquema classificado por categoria (LOW, REGULAR, HEAVY)
    """
    schema_stats = {}

    for group in grouped_queries:
        schemas = group.get("schemas", [])
        count = group.get("count", 0)
        time_spent = group.get("total_exec_time", 0)

        for schema in set(schemas):
            if schema not in schema_stats:
                schema_stats[schema] = {
                    "total_queries": 0,
                    "total_time_sec": 0
                }
            schema_stats[schema]["total_queries"] += count
            schema_stats[schema]["total_time_sec"] += time_spent / 1_000_000  # micro -> segundos

    classified = {}
    for schema, stats in schema_stats.items():
        queries = stats["total_queries"]
        time_sec = stats["total_time_sec"]

        if queries < threshold_low or time_sec < 1:
            category = "LOW"
        elif queries < threshold_regular or time_sec < 60:
            category = "REGULAR"
        else:
            category = "HEAVY"

        classified[schema] = {
            "category": category,
            "total_queries": queries,
            "total_time_sec": round(time_sec, 2),
            "priority_score": calculate_priority_score(stats)
        }

    logger.info(f"📊 {len(classified)} esquemas classificados.")
    return classified


def calculate_priority_score(stats):
    """
    Calcula pontuação de prioridade com base no uso do esquema
    """
    score = stats["total_queries"] * 0.7 + stats["total_time_sec"] * 0.3
    return round(score, 2)