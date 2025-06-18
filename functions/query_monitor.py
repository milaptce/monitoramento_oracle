
## query_monitor

import logging
import hashlib
from typing import List, Dict, Any
from functions.db_utils import OracleTableReader
from functions.utils import SQLParser

logger = logging.getLogger(__name__)

def identify_fts_queries(db_reader: OracleTableReader) -> List[Dict[str, Any]]:
    """
    Identifica queries FTS (Full Table Scan) no banco de dados Oracle.
    
    Args:
        db_reader: Instância de OracleTableReader para conexão com o banco
        
    Returns:
        Lista de dicionários contendo informações das queries FTS encontradas
    """
    query = """
        SELECT sql_id, sql_text, executions, elapsed_time
        FROM v$sql
        WHERE sql_text LIKE '%TABLE ACCESS FULL%' 
           OR sql_text LIKE '%/*+ FULL(%'
        ORDER BY elapsed_time DESC
    """
    
    try:
        results = db_reader.execute_query(query)
        if not results:
            logger.info("Nenhuma query FTS encontrada.")
            return []
        
        queries = []
        for row in results:
            sql_id, sql_text, executions, elapsed_time = row
            
            try:
                tables = SQLParser.extract_tables(sql_text)
                where_conditions = SQLParser.extract_where_conditions(sql_text)
                schema = db_reader.get_table_schema(tables[0]) if tables else None
                
                queries.append({
                    "sql_id": sql_id,
                    "sql_text": sql_text,
                    "executions": executions,
                    "elapsed_time": elapsed_time,
                    "tables": tables,
                    "where_conditions": where_conditions,
                    "schema": schema
                })
                
            except Exception as e:
                logger.warning(f"Erro ao processar query {sql_id}: {str(e)}")
                continue
        
        logger.info(f"Identificadas {len(queries)} queries FTS")
        return queries
        
    except Exception as e:
        logger.error(f"Falha ao buscar queries FTS: {str(e)}", exc_info=True)
        return []

def group_similar_queries(queries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Agrupa queries similares com base em tabelas e condições WHERE.
    
    Args:
        queries: Lista de queries FTS a serem agrupadas
        
    Returns:
        Lista de grupos de queries com estatísticas consolidadas
    """
    groups = {}
    
    for query in queries:
        # Criar assinatura única para o grupo
        signature = _create_query_signature(
            query.get("tables", []),
            query.get("where_conditions", [])
        )
        
        # Inicializar grupo se não existir
        if signature not in groups:
            groups[signature] = _initialize_query_group(query, signature)
        
        # Atualizar estatísticas do grupo
        _update_group_stats(groups[signature], query)
    
    # Calcular métricas finais e prioridades
    grouped_queries = list(groups.values())
    for group in grouped_queries:
        group["avg_exec_time"] = group["total_exec_time"] / group["count"]
        group["priority_score"] = _calculate_priority_score(group)
    
    logger.info(f"Criados {len(grouped_queries)} grupos de queries")
    return sorted(grouped_queries, key=lambda x: x["priority_score"], reverse=True)

def _create_query_signature(tables: List[str], conditions: List[str]) -> str:
    """Cria assinatura única para agrupamento de queries"""
    normalized = f"{sorted(tables)} WHERE {sorted(conditions)}"
    return hashlib.md5(normalized.encode()).hexdigest()

def _initialize_query_group(query: Dict[str, Any], signature: str) -> Dict[str, Any]:
    """Inicializa um novo grupo de queries"""
    return {
        "group_key": signature,
        "sample_sql": query["sql_text"],
        "normalized_key": f"{query['tables']} WHERE {query['where_conditions']}",
        "count": 0,
        "total_exec_time": 0,
        "tables": list(set(query.get("tables", []))),
        "schemas": set(),
        "where_conditions": list(set(query.get("where_conditions", []))),
        "priority_score": 0,
        "sql_ids": []
    }

def _update_group_stats(group: Dict[str, Any], query: Dict[str, Any]) -> None:
    """Atualiza estatísticas de um grupo com nova query"""
    group["count"] += 1
    group["total_exec_time"] += query.get("elapsed_time", 0)
    if query.get("schema"):
        group["schemas"].add(query["schema"])
    group["sql_ids"].append(query["sql_id"])

def _calculate_priority_score(group: Dict[str, Any]) -> float:
    """Calcula pontuação de prioridade para o grupo"""
    exec_weight = group["count"] * 0.6
    time_weight = (group["total_exec_time"] / 1000) * 0.4  # Converter para ms
    return round(exec_weight + time_weight, 2)