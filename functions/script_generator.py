# script_generator.py

import os
from datetime import datetime
import logging
from typing import List, Dict, Optional

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_index_script(table_name: str, column: str = "ID") -> str:
    """Gera script para criar índice em uma coluna específica"""
    if not isinstance(table_name, str) or not table_name.strip():
        raise ValueError("Nome da tabela inválido")
    if not isinstance(column, str) or not column.strip():
        raise ValueError("Nome da coluna inválido")
        
    return f"""-- Script gerado em {datetime.now().strftime('%Y-%m-%d %H:%M')}
CREATE INDEX idx_{table_name.lower()}_{column.lower()} ON {table_name}({column});
"""

def generate_refactor_script(query: Dict) -> str:
    """Sugere refatoração básica da query com dica de otimização"""
    required_fields = ['sql_id', 'sample_sql']
    if not all(field in query for field in required_fields):
        raise ValueError(f"Query deve conter os campos: {required_fields}")
    
    return f"""-- Refatoração sugerida para: {query['sql_id']}
-- Tempo médio antes: {query.get('avg_time', 'desconhecido')}ms
-- Esquema: {query.get('schema', 'desconhecido')}
{query['sample_sql']};
-- Sugerimos revisão do plano de execução e possíveis filtros adicionais.
"""

def save_sql_script(script: str, filename: str) -> bool:
    """Salva o script no diretório de saída"""
    if not script or not isinstance(script, str):
        raise ValueError("Script inválido")
    if not filename or not isinstance(filename, str):
        raise ValueError("Nome de arquivo inválido")
    
    output_dir = "output/generated_scripts"
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, f"{filename}.sql")
    
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(script)
        logger.info(f"📝 Script salvo: {file_path}")
        return True
    except Exception as e:
        logger.error(f"[ERRO] Ao salvar script {filename}: {e}")
        return False

def generate_scripts(solutions: List[Dict]) -> List[str]:
    """
    Recebe uma lista de soluções e gera os scripts adequados.
    
    Args:
        solutions: Lista de dicionários com informações de melhoria
    
    Returns:
        Lista de caminhos dos scripts gerados
    
    Raises:
        ValueError: Se solutions não for uma lista
    """
    if not isinstance(solutions, list):
        raise ValueError("solutions deve ser uma lista")
    
    generated_files = []

    if not solutions:
        logger.warning("⚠️ Nenhuma solução encontrada. Nenhum script será gerado.")
        return []

    for solution in solutions:
        if not isinstance(solution, dict):
            logger.warning("⚠️ Solução inválida (não é dicionário), ignorando")
            continue

        try:
            table_name = solution.get("table", "unknown_table")
            category = solution.get("category", "T1")
            sample_sql = solution.get("sample_sql", "")

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

            if save_sql_script(script, filename):
                generated_files.append(filename)
        except Exception as e:
            logger.error(f"Erro ao gerar script para {table_name}: {e}")

    logger.info(f"✅ {len(generated_files)} scripts SQL gerados.")
    return generated_files