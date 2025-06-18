
## performance_improvement.py
import logging
from typing import List, Dict, Any, Optional
from .db_utils import OracleTableReader

logger = logging.getLogger(__name__)

def evaluate_performance(db_reader: OracleTableReader, 
                        tables: Dict[str, List[Dict[str, Any]]],
                        queries: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """
    Avalia impacto potencial de melhorias usando OracleTableReader.
    
    Args:
        db_reader: Instância de OracleTableReader
        tables: Dicionário com tabelas classificadas (T1 e T2)
        queries: Lista opcional de queries para análise
        
    Returns:
        Lista de dicionários com sugestões de melhoria
    
    Raises:
        TypeError: Se os parâmetros forem de tipos inválidos
        RuntimeError: Se ocorrer um erro durante a avaliação
    """
    # Validação de entrada
    if not isinstance(db_reader, OracleTableReader):
        logger.error("❌ db_reader deve ser uma instância de OracleTableReader")
        raise TypeError("db_reader deve ser uma instância de OracleTableReader")
        
    if not isinstance(tables, dict):
        logger.error("❌ tables deve ser um dicionário")
        raise TypeError("tables deve ser um dicionário")
    
    try:
        if not db_reader.is_connected():
            logger.error("❌ Conexão com banco de dados não disponível.")
            return []
    except Exception as e:
        logger.error(f"❌ Erro ao verificar conexão: {str(e)}")
        raise RuntimeError(f"Erro ao verificar conexão com o banco: {str(e)}")
    
    solutions = []
    
    try:
        # Processar tabelas T1
        for table_info in tables.get("T1", []):
            if not isinstance(table_info, dict):
                logger.warning(f"⚠️ Informações inválidas para tabela T1: {table_info}")
                continue
                
            table_name = table_info.get("table")
            if not table_name:
                logger.warning("⚠️ Tabela T1 sem nome, ignorando")
                continue
                
            indexes = db_reader.get_existing_indexes(table_name)
            
            solutions.append({
                "table": table_name,
                "size_mb": table_info.get("size_mb", 0),
                "category": "T1",
                "suggestion": "criar índice" if not indexes else "índice já existe",
                "existing_indexes": indexes,
                "priority_score": 8 if not indexes else 2,
                "impact": "Alta" if not indexes else "Baixa"
            })
        
        # Processar tabelas T2
        for table_info in tables.get("T2", []):
            if not isinstance(table_info, dict):
                logger.warning(f"⚠️ Informações inválidas para tabela T2: {table_info}")
                continue
                
            table_name = table_info.get("table")
            if not table_name:
                logger.warning("⚠️ Tabela T2 sem nome, ignorando")
                continue
                
            size_mb = table_info.get("size_mb", 0)
            indexes = db_reader.get_existing_indexes(table_name)
            
            relevant_queries = [
                q for q in (queries or [])
                if isinstance(q, dict) and table_name in q.get("tables", [])
            ]
            
            gain = sum(
                _estimate_gain(q, size_mb)["estimated_gain_percent"]
                for q in relevant_queries
            ) / len(relevant_queries) if relevant_queries else 0
            
            solutions.append({
                "table": table_name,
                "size_mb": size_mb,
                "category": "T2",
                "suggestion": "refatorar query ou particionar" if indexes else "considerar índice",
                "queries_affected": [q.get("sql_id") for q in relevant_queries if q.get("sql_id")],
                "avg_gain_percent": round(gain, 2),
                "existing_indexes": indexes,
                "priority_score": 9 if gain > 20 else (7 if gain > 0 else 5),
                "impact": "Crítico" if gain > 20 else ("Médio" if gain > 0 else "Baixo")
            })
        
        logger.info(f"💡 {len(solutions)} sugestões de melhoria geradas.")
        return solutions
        
    except Exception as e:
        logger.error(f"❌ Erro ao avaliar performance: {str(e)}")
        raise RuntimeError(f"Erro ao avaliar performance: {str(e)}")

def _estimate_gain(query: Dict[str, Any], table_size_mb: float) -> Dict[str, Any]:
    """Estimativa de ganho de performance para uma query."""
    if not isinstance(query, dict):
        raise ValueError("Query deve ser um dicionário")
        
    if not isinstance(table_size_mb, (int, float)) or table_size_mb < 0:
        raise ValueError("table_size_mb deve ser um número positivo")
    
    base_time = max(1, query.get("elapsed_time", 100))
    gain_factor = 0.6 if table_size_mb < 10 else (0.4 if table_size_mb < 50 else 0.2)
    
    return {
        "estimated_gain_percent": round(gain_factor * 100, 2),
        "old_time": base_time,
        "new_time": round(base_time * (1 - gain_factor), 2),
        "schema": query.get("schema"),
        "sql_id": query.get("sql_id")
    }