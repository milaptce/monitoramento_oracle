# test_utils.py
import pytest
import configparser
from pathlib import Path
from functions.utils import ExecutionController, SQLParser, OracleUtils

class TestSQLParser:
    def test_extract_tables(self):
        assert SQLParser.extract_tables("SELECT * FROM users") == ["users"]
        assert SQLParser.extract_tables("SELECT * FROM users u JOIN orders o ON u.id=o.user_id") == ["users", "orders"]
    
    def test_extract_where_conditions(self):
        # Teste com diferentes formatos de WHERE
        conditions = SQLParser.extract_where_conditions(
            "SELECT * FROM users WHERE id=1 AND name='test'"
        )
        assert len(conditions) == 2
        assert "id=1" in conditions[0] or "id=1" in conditions[1]
        assert "name='test'" in conditions[0] or "name='test'" in conditions[1]

class TestExecutionController:
    def test_config_file_creation(self, tmp_path):
        config_file = tmp_path / "new_config.ini"
        ec = ExecutionController(config_file)
        
        # Verifica comportamento de primeira execução
        assert ec.check_first_run() is True
        assert config_file.exists()
        
        # Verifica conteúdo do arquivo
        config = configparser.ConfigParser()
        config.read(config_file)
        assert config.get("Execution", "FIRST_RUN", fallback="1") == "0"
    
    def test_subsequent_runs(self, tmp_path):
        config_file = tmp_path / "existing_config.ini"
        
        # Configuração inicial
        config = configparser.ConfigParser()
        config["Execution"] = {"FIRST_RUN": "0", "LAST_RUN": "2023-01-01"}
        with open(config_file, 'w') as f:
            config.write(f)
        
        # Testa com arquivo existente
        ec = ExecutionController(config_file)
        assert ec.check_first_run() is False

class TestOracleUtils:
    def test_format_sql_query(self):
        sql = "SELECT * FROM users WHERE id=:id AND date=:date"
        params = {"id": 1, "date": "2023-01-01"}
        formatted = OracleUtils.format_sql_query(sql, params)
        assert "id=1" in formatted
        assert "date=2023-01-01" in formatted
        assert "WHERE" in formatted