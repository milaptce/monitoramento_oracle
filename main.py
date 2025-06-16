# main.py

import os
import logging
import configparser
from datetime import datetime
from dotenv import load_dotenv

# Configuração de logging
logging.basicConfig(
    filename="logs/monitor.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente
load_dotenv()

# Funções modulares
from functions.login_db import connect_to_db
from functions.query_monitor import identify_fts_queries, group_similar_queries
from functions.table_analysis import classify_tables
from functions.performance_improvement import evaluate_performance
from functions.script_generator import generate_scripts
from functions.github_updater import update_github
from functions.utils import check_first_run, schedule_next_run


def log_execution_start():
    logger.info("🚀 Iniciando ciclo de monitoramento de Full Table Scans.")
    print("🚀 Iniciando ciclo de monitoramento de Full Table Scans.")


def log_execution_end():
    logger.info("✅ Ciclo de monitoramento concluído com sucesso.\n")
    print("✅ Ciclo de monitoramento concluído com sucesso.")


def main():
    log_execution_start()

    # 1. Verificar se é a primeira execução
    first_run = check_first_run()
    if first_run:
        logger.info("[INFO] Primeira execução detectada. Inicializando configurações.")
        print("[INFO] Primeira execução detectada.")

    # 2. Conectar ao banco de dados Oracle
    connection = connect_to_db()
    if not connection:
        logger.error("❌ Falha na conexão com o banco de dados. Encerrando.")
        return

    try:
        # 3. Identificar queries FTS
        fts_queries = identify_fts_queries(connection)
        if not fts_queries:
            logger.warning("⚠️ Nenhuma query FTS identificada nesta execução.")
            print("⚠️ Nenhuma query FTS identificada nesta execução.")
            return

        logger.info(f"🔍 {len(fts_queries)} queries FTS identificadas.")

        # 4. Agrupar queries similares
        grouped_queries = group_similar_queries(fts_queries)
        logger.info(f"📊 {len(grouped_queries)} grupos de queries criados.")

        # Mostrar grupo mais relevante
        for group in grouped_queries[:5]:  # exibir apenas top 5 para não poluir console
            print(f"Grupo {group['group_key']} - {group['count']} instâncias")
            print(f"Média de tempo: {group['avg_exec_time']}μs")
            print(f"Pontuação de prioridade: {group['priority_score']}\n")

        # 5. Classificar tabelas em T1 e T2
        classified_tables = classify_tables(fts_queries, connection)
        t1_count = len(classified_tables.get("T1", []))
        t2_count = len(classified_tables.get("T2", []))
        logger.info(f"📈 Tabelas classificadas: T1={t1_count}, T2={t2_count}")
        print(f"📊 Tabelas classificadas: T1={t1_count}, T2={t2_count}")

        # 6. Avaliar possíveis melhorias de performance
        solutions = evaluate_performance(classified_tables, fts_queries)
        logger.info(f"💡 Sugestões geradas para {len(solutions)} objetos.")
        print(f"💡 Sugestões geradas para {len(solutions)} objetos.")

        # 7. Gerar scripts SQL com base nas sugestões
        generated_scripts = generate_scripts(solutions)
        logger.info(f"📝 {len(generated_scripts)} scripts SQL gerados.")
        print(f"📝 {len(generated_scripts)} scripts SQL gerados.")

        # 8. Atualizar repositório GitHub (se configurado)
        github_token = os.getenv("GITHUB_TOKEN")
        github_repo = os.getenv("GITHUB_REPO")

        if github_token and github_repo:
            try:
                update_github()
                logger.info("📦 Scripts atualizados no repositório GitHub.")
                print("📦 Scripts atualizados no repositório GitHub.")
            except Exception as e:
                logger.warning(f"[WARNING] Não foi possível atualizar o GitHub: {e}")
        else:
            logger.warning("🔒 Credenciais do GitHub não encontradas. Pulando atualização.")
            print("🔒 Credenciais do GitHub não encontradas. Pulando atualização.")

    except Exception as e:
        logger.error(f"❌ Erro durante a execução: {e}", exc_info=True)
        print(f"❌ Erro durante a execução: {e}")
    finally:
        # 9. Fechar conexão com segurança
        if 'connection' in locals() and connection:
            try:
                connection.close()
                logger.info("🔌 Conexão com o banco de dados encerrada.")
                print("🔌 Conexão com o banco de dados encerrada.")
            except:
                pass

    log_execution_end()

    # 10. Agendar próxima execução
    schedule_next_run(hours=6)


if __name__ == "__main__":
    main()