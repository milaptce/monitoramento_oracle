# test_script_generator.py
import pytest
from unittest.mock import patch, mock_open, MagicMock
from functions.script_generator import (
    generate_scripts,
    generate_index_script,
    generate_refactor_script,
    save_sql_script
)
from datetime import datetime
import logging

class TestScriptGenerator:
    def test_generate_index_script_success(self):
        """Test successful index script generation"""
        result = generate_index_script("Users", "ID")
        assert "CREATE INDEX idx_users_id ON Users(ID)" in result
        assert datetime.now().strftime("%Y-%m-%d") in result

    def test_generate_index_script_invalid_input(self):
        """Test index script with invalid input"""
        with pytest.raises(ValueError):
            generate_index_script("", "id")
        with pytest.raises(ValueError):
            generate_index_script("users", "")

    def test_generate_refactor_script_success(self):
        """Test successful refactor script generation"""
        test_data = {
            "sql_id": "q123",
            "avg_time": 150,
            "schema": "public",
            "sample_sql": "SELECT * FROM users"
        }
        result = generate_refactor_script(test_data)
        assert "q123" in result
        assert "150ms" in result
        assert "public" in result
        assert "SELECT * FROM users" in result

    def test_generate_refactor_script_missing_fields(self):
        """Test refactor script with missing required fields"""
        with pytest.raises(ValueError):
            generate_refactor_script({"sql_id": "q123"})  # missing sample_sql

    @patch("os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    def test_save_sql_script_success(self, mock_file, mock_makedirs):
        """Test successful script saving"""
        assert save_sql_script("TEST SCRIPT", "test") is True
        mock_makedirs.assert_called_once()
        mock_file.assert_called_once()

    @patch("os.makedirs")
    @patch("builtins.open", side_effect=IOError("Test error"))
    def test_save_sql_script_failure(self, mock_file, mock_makedirs):
        """Test script saving failure"""
        assert save_sql_script("TEST SCRIPT", "test") is False

    def test_save_sql_script_invalid_input(self):
        """Test script saving with invalid input"""
        with pytest.raises(ValueError):
            save_sql_script("", "test")
        with pytest.raises(ValueError):
            save_sql_script("VALID", "")

    def test_generate_scripts_empty(self):
        """Test with empty solutions list"""
        assert generate_scripts([]) == []

    def test_generate_scripts_invalid_input(self):
        """Test with invalid input type"""
        with pytest.raises(ValueError):
            generate_scripts("not a list")

    @patch("functions.script_generator.save_sql_script", return_value=True)
    def test_generate_scripts_t1(self, mock_save):
        """Test T1 script generation"""
        solutions = [{
            "table": "users",
            "category": "T1",
            "priority_score": 5
        }]
        result = generate_scripts(solutions)
        assert len(result) == 1
        assert "T1_idx_users" in result[0]

    @patch("functions.script_generator.save_sql_script", return_value=True)
    def test_generate_scripts_t2(self, mock_save):
        """Test T2 script generation"""
        solutions = [{
            "table": "orders",
            "category": "T2",
            "group_key": "q123",
            "sample_sql": "SELECT * FROM orders",
            "avg_exec_time": 200
        }]
        result = generate_scripts(solutions)
        assert len(result) == 1
        assert "T2_refactor_orders" in result[0]

    def test_generate_scripts_invalid_item(self, caplog):
        """Test with invalid solution item"""
        result = generate_scripts(["not a dict"])
        assert len(result) == 0
        assert "Solução inválida" in caplog.text

    def test_generate_scripts_unknown_category(self, caplog):
        """Test with unknown category"""
        result = generate_scripts([{
            "table": "logs",
            "category": "T3"
        }])
        assert len(result) == 0
        assert "Categoria desconhecida" in caplog.text

    @patch("functions.script_generator.generate_index_script", side_effect=Exception("Test error"))
    def test_generate_scripts_handles_errors(self, mock_gen, caplog):
        """Test error handling during generation"""
        result = generate_scripts([{
            "table": "users",
            "category": "T1"
        }])
        assert len(result) == 0
        assert "Erro ao gerar script" in caplog.text