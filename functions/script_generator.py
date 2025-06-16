# functions/script_generator.py
import os
from datetime import datetime
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_index_script(table_name, column="ID"):
    """Gera script para criar índice em uma coluna específica"""
    return f"-- Script gerado em {datetime.now().strftime('%Y-%m-%d %H:%M')}\nCREATE INDEX idx_{table_name.lower()}_{column.lower()} ON {table_name}({column});\n"

def generate_refactor_script(query):
    """Sugere refatoração básica da query com dica de otimização"""
    return f"-- Refatoração sugerida para: {query['sql_id']}\n-- Tempo médio antes: {query.get('avg_time', 'desconhecido')}ms\n-- Esquema: {query.get('schema', 'desconhecido')}\n{query['sample_sql']};\n-- Sugerimos revisão do plano de execução e possíveis filtros adicionais.\n"

def save_sql_script(script, filename):
    """Salva o script no diretório de saída"""
    output_dir = "output/generated_scripts"
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, f"{filename}.sql")
    try:
        with open(file_path, "w") as f:
            f.write(script)
        logger.info(f"📝 Script salvo: {file_path}")
        return True
    except Exception as e:
        logger.error(f"[ERRO] Ao salvar script {filename}: {e}")
        return False


def generate_scripts(solutions):
    """
    Recebe uma lista de soluções e gera os scripts adequados.
    
    Args:
        solutions (list): Lista de dicionários com informações de melhoria
    
    Returns:
        list: Lista de caminhos dos scripts gerados
    """
    generated_files = []

    if not solutions:
        logger.warning("⚠️ Nenhuma solução encontrada. Nenhum script será gerado.")
        return []

    for solution in solutions:
        table_name = solution.get("table", "unknown_table")
        category = solution.get("category", "T1")
        priority = solution.get("priority_score", 0)
        sample_sql = solution.get("sample_sql", "")

        # Garantir que a tabela tenha sido classificada corretamente
        if category == "T1":
            script = generate_index_script(table_name)
            filename = f"T1_idx_{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        elif category == "T2":
            script = generate_refactor_script({
                "sql_id": solution.get("group_key", "unknown"),
                "avg_time": solution.get("avg_exec_time", 0),
                "schema": solution.get("schema", "unknown"),
                "sample_sql": sample_sql
            })
            filename = f"T2_refactor_{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        else:
            logger.warning(f"⚠️ Categoria desconhecida para tabela {table_name}: {category}")
            continue

        # Salvar script
        success = save_sql_script(script, filename)
        if success:
            generated_files.append(filename)

    logger.info(f"✅ {len(generated_files)} scripts SQL gerados.")
    return generated_files