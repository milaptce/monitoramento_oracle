import logging
from typing import Dict, List, Any, Optional, TypedDict
from functions.db_utils import OracleTableReader

logger = logging.getLogger(__name__)

class TableInfo(TypedDict):
    """Typed dictionary for table information"""
    table: str
    size_mb: float
    schema: Optional[str]
    indexes: Optional[List[str]]

class ClassificationResult(TypedDict):
    """Typed dictionary for classification results"""
    T1: List[TableInfo]
    T2: List[TableInfo]

def classify_tables(
    db_reader: OracleTableReader, 
    queries: List[Dict[str, Any]],
    size_threshold: float = 10.0
) -> ClassificationResult:
    """
    Classify tables into T1 (< threshold) and T2 (>= threshold) based on size
    
    Args:
        db_reader: OracleTableReader instance
        queries: List of query dictionaries
        size_threshold: Size threshold in MB (default: 10.0)
        
    Returns:
        Dictionary with tables classified into T1 and T2
    """
    # Input validation
    if not isinstance(db_reader, OracleTableReader):
        logger.error("db_reader must be an OracleTableReader instance")
        raise TypeError("db_reader must be an OracleTableReader instance")
        
    if not isinstance(queries, list):
        logger.error("queries must be a list")
        raise TypeError("queries must be a list")

    # Initialize result structure
    result: ClassificationResult = {
        "T1": [],
        "T2": []
    }
    
    # Get unique tables from all queries
    tables = sorted({table.upper() for query in queries 
                    if isinstance(query, dict)
                    for table in query.get("tables", [])
                    if isinstance(table, str) and table.strip()})
    
    if not tables:
        logger.info("No tables found in queries")
        return result
    
    # Process each table
    for table in sorted(tables):  # Process in consistent order
        try:
            size = db_reader.get_table_size(table)
            if size is None:
                logger.warning(f"Could not get size for table {table}")
                continue
                
            schema = db_reader.get_table_schema(table)
            indexes = db_reader.get_existing_indexes(table)
            
            table_info: TableInfo = {
                "table": table,
                "size_mb": size,
                "schema": schema,
                "indexes": indexes if indexes else None
            }
            
            if size < size_threshold:
                result["T1"].append(table_info)
                logger.debug(f"Table {table} classified as T1 ({size:.2f} MB)")
            else:
                result["T2"].append(table_info)
                logger.debug(f"Table {table} classified as T2 ({size:.2f} MB)")
                
        except Exception as e:
            logger.error(f"Error processing table {table}: {str(e)}")
            continue
    
    logger.info(
        f"Classification complete - T1: {len(result['T1'])} tables | "
        f"T2: {len(result['T2'])} tables | "
        f"Threshold: {size_threshold} MB"
    )
    
    return result


def analyze_table_access_patterns(
    db_reader: OracleTableReader,
    queries: List[Dict[str, Any]],
    classification: ClassificationResult
) -> Dict[str, Any]:
    """
    Analyze table access patterns based on query usage
    
    Args:
        db_reader: OracleTableReader instance
        queries: List of analyzed queries
        classification: Classification result
        
    Returns:
        Dictionary with access pattern statistics
    """
    access_stats = {
        "high_usage_tables": [],
        "join_patterns": {},
        "filter_conditions": {}
    }
    
    # Get all classified tables in uppercase
    all_tables = [t["table"].upper() for t in classification["T1"] + classification["T2"]]
    
    for query in queries:
        if not isinstance(query, dict):
            continue
            
        query_tables = query.get("tables", [])
        if not isinstance(query_tables, list):
            continue
            
        for table in query_tables:
            table_upper = table.upper()
            if table_upper in all_tables:
                if table_upper not in access_stats["join_patterns"]:
                    access_stats["join_patterns"][table_upper] = 0
                access_stats["join_patterns"][table_upper] += 1
    
    return access_stats


def generate_optimization_recommendations(
    classification: ClassificationResult,
    access_stats: Dict[str, Any]
) -> List[str]:
    """
    Generate optimization recommendations based on classification and access patterns
    
    Args:
        classification: Classification result
        access_stats: Access pattern statistics
        
    Returns:
        List of optimization recommendations
    """
    recommendations = []
    
    # Recommendations for T1 tables
    for table in classification["T1"]:
        rec = f"Table {table['table']} (T1 - {table['size_mb']:.2f}MB): "
        
        if not table['indexes']:
            rec += "Consider adding basic indexes"
        elif len(table['indexes']) < 3:
            rec += "Review existing indexes for optimization"
        else:
            rec += "Has sufficient indexes"
            
        recommendations.append(rec)
    
    # Recommendations for T2 tables
    for table in classification["T2"]:
        rec = f"Table {table['table']} (T2 - {table['size_mb']:.2f}MB): "
        
        if table['size_mb'] > 100:
            rec += "Consider partitioning "
        else:
            rec += "Evaluate future partitioning strategies "
            
        join_count = access_stats["join_patterns"].get(table['table'].upper(), 0)
        if join_count > 5:
            rec += "| Frequently joined - optimize relationship indexes"
            
        recommendations.append(rec)
    
    return recommendations