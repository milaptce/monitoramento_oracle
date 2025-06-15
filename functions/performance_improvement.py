def evaluate_performance(tables):
    # Lógica para avaliar impacto de melhorias
    solutions = {}
    for table in tables["T1"]:
        solutions[table] = {"action": "create_index", "impact": "low"}
    for table in tables["T2"]:
        solutions[table] = {"action": "refactor_query", "impact": "high"}
