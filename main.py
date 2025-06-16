import os
import logging
import configparser
from datetime import datetime
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Configuração de logging
logging.basicConfig(
    filename="logs/monitor.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Funções modulares
from functions.login_db import connect_to_db
from functions.query_monitor import identify_fts_queries, group_similar_queries
from functions.table_analysis import classify_tables
from functions.performance_improvement import evaluate_performance
from functions.script_generator import generate_scripts
from functions.github_updater import update_github
from functions.utils import check_first_run, schedule_next_run
from functions.performance_improvement import evaluate_performance
from functions.script_generator import generate_scripts


def log_execution_start():
    logging.info("🚀 Iniciando ciclo de monitoramento de Full Table Scans.")
    print("🚀 Iniciando ciclo de monitoramento de Full Table Scans.")


def log_execution_end():
    logging.info("✅ Ciclo de monitoramento concluído com sucesso.\n")
    print("✅ Ciclo de monitoramento concluído com sucesso.")


def main():
    log_execution_start()

    # Verificar se é a primeira execução
    first_run = check_first_run()
    if first_run:
        logging.info("[INFO] Primeira execução detectada. Inicializando configurações.")
        print("[INFO] Primeira execução detectada.")

    # Conectar ao banco de dados Oracle
    connection = connect_to_db()
    if not connection:
        logging.error("❌ Falha na conexão com o banco de dados. Encerrando.")
        return

    try:
        # Identificar queries FTS
        fts_queries = identify_fts_queries(connection)
        if not fts_queries:
            logging.warning("⚠️ Nenhuma query FTS identificada nesta execução.")
            print("⚠️ Nenhuma query FTS identificada nesta execução.")
            return

        logging.info(f"🔍 {len(fts_queries)} queries FTS identificadas.")
        grouped_queries = group_similar_queries(raw_queries)

        for group in grouped_queries:
            print(f"Grupo {group['group_key']} - {group['count']} instâncias")
            print(f"Média de tempo: {group['avg_exec_time']}ms")
            print(f"Pontuação de prioridade: {group['priority_score']}\n")


        # Classificar tabelas em T1 e T2
        classified_tables = classify_tables(fts_queries)
        t1_count = len(classified_tables.get("T1", []))
        t2_count = len(classified_tables.get("T2", []))
        logging.info(f"📊 Tabelas classificadas: T1={t1_count}, T2={t2_count}")
        print(f"📊 Tabelas classificadas: T1={t1_count}, T2={t2_count}")

        # Avaliar possíveis melhorias de performance
        performance_suggestions = evaluate_performance(classified_tables)
   
        logging.info(f"💡 Sugestões geradas para {len(performance_suggestions)} objetos.")
        print(f"💡 Sugestões geradas para {len(performance_suggestions)} objetos.")

        # Avaliar melhorias
        solutions = evaluate_performance(tables, queries)
        # Gerar scripts
        generated_scripts = generate_scripts(solutions)

        # Gerar scripts SQL com base nas sugestões
        generated_scripts = generate_scripts(performance_suggestions)
        logging.info(f"📝 {len(generated_scripts)} scripts SQL gerados.")
        print(f"📝 {len(generated_scripts)} scripts SQL gerados.")


    
        # Atualizar repositório GitHub
        if os.getenv("GITHUB_TOKEN") and os.getenv("GITHUB_REPO"):
            try:
                update_github()
                logging.info("📦 Scripts atualizados no repositório GitHub.")
                print("📦 Scripts atualizados no repositório GitHub.")
            except Exception as e:
                logging.warning(f"[WARNING] Não foi possível atualizar o GitHub: {e}")
        else:
            logging.warning("🔒 Credenciais do GitHub não encontradas. Pulando atualização.")
            print("🔒 Credenciais do GitHub não encontradas. Pulando atualização.")

    except Exception as e:
        logging.error(f"❌ Erro durante a execução: {e}", exc_info=True)
        print(f"❌ Erro durante a execução: {e}")
    finally:
        # Fechar conexão com o banco
        if 'connection' in locals() and connection:
            connection.close()
            logging.info("🔌 Conexão com o banco de dados encerrada.")
            print("🔌 Conexão com o banco de dados encerrada.")

    log_execution_end()

    # Agendar próxima execução (6 horas)
    schedule_next_run(hours=6)


if __name__ == "__main__":
    main()