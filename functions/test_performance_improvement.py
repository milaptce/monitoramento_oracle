# test_performance_improvement.py
import pytest
from unittest.mock import patch, MagicMock
from functions.performance_improvement import (
    evaluate_performance,
    _estimate_gain
)
from functions.db_utils import OracleTableReader

class TestPerformanceImprovement:
    @pytest.fixture
    def mock_reader(self):
        mock = MagicMock(spec=OracleTableReader)
        mock.is_connected.return_value = True
        return mock

    @pytest.fixture
    def sample_tables(self):
        return {
            "T1": [{"table": "users", "size_mb": 5.0}],
            "T2": [{"table": "orders", "size_mb": 15.0}]
        }

    @pytest.fixture
    def sample_queries(self):
        return [
            {"sql_id": "q1", "tables": ["users"], "elapsed_time": 100},
            {"sql_id": "q2", "tables": ["orders"], "elapsed_time": 200}
        ]

    def test_evaluate_performance_basic(self, mock_reader, sample_tables):
        """Test basic performance evaluation"""
        mock_reader.get_existing_indexes.return_value = []
        solutions = evaluate_performance(mock_reader, sample_tables)
        
        assert len(solutions) == 2
        assert solutions[0]["suggestion"] == "criar índice"
        assert solutions[1]["suggestion"] == "considerar índice"

    def test_evaluate_performance_with_queries(self, mock_reader, sample_tables, sample_queries):
        """Test evaluation with query analysis"""
        mock_reader.get_existing_indexes.return_value = ["existing_idx"]
        solutions = evaluate_performance(mock_reader, sample_tables, sample_queries)
        
        assert len(solutions) == 2
        assert solutions[1]["queries_affected"] == ["q2"]
        assert isinstance(solutions[1]["avg_gain_percent"], float)

    def test_evaluate_performance_no_connection(self, mock_reader, sample_tables):
        """Test with no database connection"""
        mock_reader.is_connected.return_value = False
        solutions = evaluate_performance(mock_reader, sample_tables)
        assert solutions == []

    def test_evaluate_performance_invalid_input(self, mock_reader):
        """Test with invalid input types"""
        with pytest.raises(TypeError):
            evaluate_performance(None, {})
            
        with pytest.raises(TypeError):
            evaluate_performance(mock_reader, "not a dict")

    def test_evaluate_performance_empty_tables(self, mock_reader):
        """Test with empty tables dictionary"""
        solutions = evaluate_performance(mock_reader, {})
        assert solutions == []

    def test_evaluate_performance_invalid_table_data(self, mock_reader, caplog):
        """Test with invalid table data"""
        solutions = evaluate_performance(mock_reader, {"T1": ["invalid"]})
        assert len(solutions) == 0
        assert "Informações inválidas" in caplog.text

    def test_estimate_gain_basic(self):
        """Test basic gain estimation"""
        result = _estimate_gain({"elapsed_time": 100}, 8.0)
        assert result["estimated_gain_percent"] > 0
        assert result["new_time"] < 100

    def test_estimate_gain_various_sizes(self):
        """Test gain estimation with different table sizes"""
        small = _estimate_gain({"elapsed_time": 100}, 5.0)
        medium = _estimate_gain({"elapsed_time": 100}, 30.0)
        large = _estimate_gain({"elapsed_time": 100}, 100.0)
        
        assert small["estimated_gain_percent"] > medium["estimated_gain_percent"]
        assert medium["estimated_gain_percent"] > large["estimated_gain_percent"]

    def test_estimate_gain_invalid_input(self):
        """Test gain estimation with invalid input"""
        with pytest.raises(ValueError):
            _estimate_gain("not a dict", 10.0)
            
        with pytest.raises(ValueError):
            _estimate_gain({}, -1.0)

    @patch('functions.performance_improvement.logger')
    def test_error_handling(self, mock_logger, mock_reader):
        """Test error handling during evaluation"""
        mock_reader.is_connected.side_effect = Exception("DB error")
        
        with pytest.raises(RuntimeError, match="Erro ao verificar conexão com o banco: DB error"):
            evaluate_performance(mock_reader, {"T1": [{"table": "users"}]})
        
        mock_logger.error.assert_called_with("❌ Erro ao verificar conexão: DB error")