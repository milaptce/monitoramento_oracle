# test_query_monitor.py
import pytest
from unittest.mock import patch
from functions.query_monitor import (
    identify_fts_queries,
    group_similar_queries,
    _create_query_signature,
    _initialize_query_group,
    _update_group_stats,
    _calculate_priority_score
)

class MockOracleTableReader:
    def __init__(self):
        self.test_data = [
            ("sql1", "SELECT * FROM users WHERE id = 1", 10, 1000),
            ("sql2", "SELECT * FROM products WHERE price > 100", 5, 2000),
            ("sql3", "SELECT * FROM users WHERE name LIKE 'A%'", 8, 1500)
        ]
    
    def execute_query(self, query, params=None, fetch_all=True):
        return self.test_data
    
    def get_table_schema(self, table_name):
        return f"schema_{table_name.lower()}"

class MockSQLParser:
    @staticmethod
    def extract_tables(sql_text):
        if "FROM" not in sql_text:  # Para queries sem tabelas explícitas
            return []
        if "users" in sql_text:
            return ["users"]
        elif "products" in sql_text:
            return ["products"]
        elif "DUAL" in sql_text:  # Para queries com FROM DUAL
            return []
        return []
    
    @staticmethod
    def extract_where_conditions(sql_text):
        if "WHERE" not in sql_text:
            return []
        if "id = 1" in sql_text:
            return ["id = 1"]
        elif "price > 100" in sql_text:
            return ["price > 100"]
        elif "name LIKE 'A%'" in sql_text:
            return ["name LIKE 'A%'"]
        return []

@pytest.fixture
def mock_db_reader():
    return MockOracleTableReader()

# -----------------------------------------------------------
# TESTES PRINCIPAIS
# -----------------------------------------------------------

def test_identify_fts_queries(mock_db_reader):
    """Testa identificação básica de queries FTS"""
    with patch('functions.query_monitor.SQLParser', new=MockSQLParser):
        results = identify_fts_queries(mock_db_reader)
        
        assert len(results) == 3
        assert results[0]["sql_id"] == "sql1"
        assert results[1]["tables"] == ["products"]
        assert results[2]["where_conditions"] == ["name LIKE 'A%'"]
        assert results[0]["schema"] == "schema_users"

def test_group_similar_queries():
    """Testa agrupamento de queries similares"""
    test_queries = [
        {
            "sql_id": "sql1",
            "sql_text": "SELECT * FROM users WHERE id = 1",
            "executions": 10,
            "elapsed_time": 1000,
            "tables": ["users"],
            "where_conditions": ["id = 1"],
            "schema": "schema_users"
        },
        {
            "sql_id": "sql2",
            "sql_text": "SELECT * FROM users WHERE id = 2",
            "executions": 5,
            "elapsed_time": 2000,
            "tables": ["users"],
            "where_conditions": ["id = 2"],
            "schema": "schema_users"
        }
    ]
    
    groups = group_similar_queries(test_queries)
    assert len(groups) == 2
    assert groups[0]["count"] == 1
    assert groups[0]["tables"] == ["users"]

def test_query_signature():
    """Testa geração de assinatura única para queries"""
    sig1 = _create_query_signature(["users"], ["id = 1"])
    sig2 = _create_query_signature(["users"], ["id = 2"])
    sig3 = _create_query_signature(["products"], ["price > 100"])
    
    assert sig1 != sig2
    assert sig1 != sig3
    assert len(sig1) == 32  # MD5 hash length

def test_priority_score_calculation():
    """Testa cálculo de score de prioridade"""
    group = {
        "count": 10,
        "total_exec_time": 5000  # 5 segundos
    }
    score = _calculate_priority_score(group)
    assert score == 8.0  # (10*0.6 + 5*0.4)

def test_empty_results(mock_db_reader):
    """Testa comportamento com resultados vazios"""
    mock_db_reader.test_data = []
    with patch('functions.query_monitor.SQLParser', new=MockSQLParser):
        results = identify_fts_queries(mock_db_reader)
        assert results == []

# -----------------------------------------------------------
# TESTES ADICIONAIS PARA CASOS ESPECIAIS
# -----------------------------------------------------------

def test_queries_with_no_tables(mock_db_reader):
    """Testa queries que não referenciam tabelas"""
    mock_db_reader.test_data = [
        ("sql4", "SELECT 1 FROM DUAL", 1, 100),
        ("sql5", "SELECT SYSDATE", 1, 50)
    ]
    
    with patch('functions.query_monitor.SQLParser', new=MockSQLParser):
        results = identify_fts_queries(mock_db_reader)
        assert len(results) == 2
        assert results[0]["tables"] == []
        assert results[1]["tables"] == []

def test_malformed_queries(mock_db_reader):
    """Testa tratamento de SQL inválido"""
    mock_db_reader.test_data = [
        ("sql6", "INVALID SQL SYNTAX", 1, 100),
        ("sql7", "SELECT * FROM", 1, 150)  # SQL incompleto
    ]
    
    with patch('functions.query_monitor.SQLParser', new=MockSQLParser):
        results = identify_fts_queries(mock_db_reader)
        assert len(results) == 2
        assert results[0]["tables"] == []
        assert results[1]["tables"] == []

def test_queries_with_complex_conditions(mock_db_reader):
    """Testa queries com múltiplas condições WHERE"""
    mock_db_reader.test_data = [
        ("sql8", "SELECT * FROM orders WHERE status='OPEN' AND value>1000", 3, 750)
    ]
    
    with patch('functions.query_monitor.SQLParser') as mock_parser:
        mock_parser.extract_tables.return_value = ["orders"]
        mock_parser.extract_where_conditions.return_value = ["status='OPEN'", "value>1000"]
        
        results = identify_fts_queries(mock_db_reader)
        assert len(results[0]["where_conditions"]) == 2

# -----------------------------------------------------------
# TESTES DE PERFORMANCE (OPCIONAIS)
# -----------------------------------------------------------

@pytest.mark.benchmark
def test_performance_with_large_dataset(benchmark, mock_db_reader):
    """Teste de performance com grande volume de queries"""
    mock_db_reader.test_data = [
        (f"sql{i}", f"SELECT * FROM table{i%10} WHERE col={i}", i%10, i*100)
        for i in range(1000)  # 1000 queries simuladas
    ]
    
    with patch('functions.query_monitor.SQLParser', new=MockSQLParser):
        result = benchmark(identify_fts_queries, mock_db_reader)
        assert len(result) == 1000