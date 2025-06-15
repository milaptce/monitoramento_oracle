import os
from dotenv import load_dotenv
from functions.login_db import connect_to_db
from functions.query_monitor import identify_fts_queries
from functions.table_analysis import classify_tables
from functions.performance_improvement import evaluate_performance
from functions.script_generator import generate_scripts
from functions.github_updater import update_github

load_dotenv()

def main():
    # Conexão ao banco de dados
    connection = connect_to_db()

    # Identificar queries FTS
    queries = identify_fts_queries(connection)

    # Classificar tabelas
    tables = classify_tables(queries)

    # Avaliar melhorias de performance
    solutions = evaluate_performance(tables)

    # Gerar scripts
    generate_scripts(solutions)

    # Atualizar GitHub
    update_github()

    # Fechar conexão
    connection.close()

if __name__ == "__main__":
