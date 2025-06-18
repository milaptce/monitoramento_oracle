import pytest
from unittest.mock import MagicMock, patch
from functions.table_analysis import (
    classify_tables,
    analyze_table_access_patterns,
    generate_optimization_recommendations,
    TableInfo,
    ClassificationResult
)
from functions.db_utils import OracleTableReader

class TestTableAnalysis:
    @pytest.fixture
    def mock_reader(self):
        """Fixture providing a mocked OracleTableReader"""
        mock = MagicMock(spec=OracleTableReader)
        mock._last_error = None
        return mock

    @pytest.fixture
    def sample_queries(self):
        """Fixture with sample queries for testing"""
        return [
            {"tables": ["users"], "sql": "SELECT * FROM users"},
            {"tables": ["orders"], "sql": "SELECT * FROM orders WHERE status = 'shipped'"},
            {"tables": ["customers", "orders"], "sql": "JOIN query"}
        ]

    @pytest.fixture
    def sample_classification(self):
        """Fixture with sample classification results"""
        return {
            "T1": [
                {"table": "USERS", "size_mb": 5.0, "schema": "APP", "indexes": ["users_pk"]}
            ],
            "T2": [
                {"table": "ORDERS", "size_mb": 15.0, "schema": "APP", "indexes": ["orders_pk"]},
                {"table": "CUSTOMERS", "size_mb": 25.0, "schema": "APP", "indexes": None}
            ]
        }

    def test_classify_tables_basic(self, mock_reader, sample_queries):
        """Basic table classification test"""
        # Configure mock responses in explicit order
        mock_responses = {
            "USERS": (5.0, "APP", ["users_pk"]),
            "ORDERS": (15.0, "APP", ["orders_pk"]),
            "CUSTOMERS": (25.0, "APP", None)
        }
        
        def get_table_size(table):
            return mock_responses[table.upper()][0]
            
        def get_table_schema(table):
            return mock_responses[table.upper()][1]
            
        def get_existing_indexes(table):
            return mock_responses[table.upper()][2]
        
        mock_reader.get_table_size.side_effect = get_table_size
        mock_reader.get_table_schema.side_effect = get_table_schema
        mock_reader.get_existing_indexes.side_effect = get_existing_indexes
        
        result = classify_tables(mock_reader, sample_queries)
        
        # Verify counts
        assert len(result["T1"]) == 1
        assert len(result["T2"]) == 2
        
        # Verify specific tables
        t1_tables = [t["table"] for t in result["T1"]]
        t2_tables = [t["table"] for t in result["T2"]]
        
        assert "USERS" in t1_tables
        assert "ORDERS" in t2_tables
        assert "CUSTOMERS" in t2_tables
        
        # Verify metadata
        users_info = next(t for t in result["T1"] if t["table"] == "USERS")
        assert users_info["size_mb"] == 5.0
        assert users_info["schema"] == "APP"
        assert users_info["indexes"] == ["users_pk"]

    def test_classify_tables_empty_input(self, mock_reader):
        """Test with empty query list"""
        result = classify_tables(mock_reader, [])
        assert result == {"T1": [], "T2": []}

    def test_classify_tables_invalid_input(self, mock_reader):
        """Test with invalid inputs"""
        with pytest.raises(TypeError):
            classify_tables(None, [])  # Invalid db_reader
            
        with pytest.raises(TypeError):
            classify_tables(mock_reader, "not_a_list")  # Invalid queries

    def test_classify_tables_with_none_size(self, mock_reader):
        """Test when table size is unavailable"""
        mock_reader.get_table_size.return_value = None
        queries = [{"tables": ["unknown_table"]}]
        
        result = classify_tables(mock_reader, queries)
        assert result == {"T1": [], "T2": []}

    def test_classify_tables_duplicates(self, mock_reader):
        """Test with duplicate tables in queries"""
        mock_reader.get_table_size.return_value = 8.0
        mock_reader.get_table_schema.return_value = "APP"
        mock_reader.get_existing_indexes.return_value = ["pk"]
        
        queries = [
            {"tables": ["users"]},
            {"tables": ["users", "orders"]},
            {"tables": ["users"]}
        ]
        
        result = classify_tables(mock_reader, queries)
        assert len(result["T1"]) == 2  # users and orders (only one entry for users)

    def test_classify_tables_custom_threshold(self, mock_reader, sample_queries):
        """Test with custom size threshold"""
        mock_reader.get_table_size.side_effect = [15.0, 25.0, 5.0]
        mock_reader.get_table_schema.return_value = "APP"
        
        # 20MB threshold
        result = classify_tables(mock_reader, sample_queries, size_threshold=20.0)
        
        assert len(result["T1"]) == 2  # orders (15) and users (5)
        assert len(result["T2"]) == 1  # customers (25)

    def test_classify_tables_mixed_query_formats(self, mock_reader):
        """Test with varied query formats"""
        mock_reader.get_table_size.side_effect = [5.0, 15.0, 8.0, None]
        mock_reader.get_table_schema.return_value = "APP"
        mock_reader.get_existing_indexes.return_value = ["pk"]
        
        queries = [
            {"tables": ["valid1"]},
            {"tables": "not_a_list"},  # invalid
            {"tables": ["valid2"]},
            {},  # no tables field
            {"tables": ["valid3"]},
            {"tables": [None, ""]}  # invalid
        ]
        
        result = classify_tables(mock_reader, queries)
        assert len(result["T1"]) == 2  # valid1 (5), valid3 (8)
        assert len(result["T2"]) == 1  # valid2 (15)

    def test_analyze_table_access_patterns(self, sample_classification, sample_queries):
        """Test table access pattern analysis"""
        mock_reader = MagicMock()
        result = analyze_table_access_patterns(mock_reader, sample_queries, sample_classification)
        
        assert "high_usage_tables" in result
        assert "join_patterns" in result
        assert isinstance(result["join_patterns"], dict)
        
        # Verify join counts
        assert result["join_patterns"].get("USERS") == 1
        assert result["join_patterns"].get("ORDERS") == 2  # appears in 2 queries
        assert result["join_patterns"].get("CUSTOMERS") == 1

    def test_generate_optimization_recommendations(self, sample_classification):
        """Test optimization recommendation generation"""
        access_stats = {
            "join_patterns": {
                "USERS": 5,
                "ORDERS": 10,
                "CUSTOMERS": 3
            }
        }
        
        recommendations = generate_optimization_recommendations(
            sample_classification,
            access_stats
        )
        
        assert len(recommendations) == 3  # 1 T1 + 2 T2
        assert any("T1" in rec for rec in recommendations)
        assert any("T2" in rec for rec in recommendations)
        
        # Verify recommendations contain expected info
        assert any("USERS" in rec for rec in recommendations)
        assert any("ORDERS" in rec for rec in recommendations)
        assert any("CUSTOMERS" in rec for rec in recommendations)
        assert any("partitioning" in rec.lower() for rec in recommendations)

    @patch('functions.table_analysis.logger')
    def test_classify_tables_logging(self, mock_logger, mock_reader, sample_queries):
        """Test logging messages"""
        mock_reader.get_table_size.side_effect = [5.0, 15.0, 25.0]
        mock_reader.get_table_schema.return_value = "APP"
        
        classify_tables(mock_reader, sample_queries)
        
        # Verify final log message
        mock_logger.info.assert_called_with(
            "Classification complete - T1: 1 tables | T2: 2 tables | Threshold: 10.0 MB"
        )

    def test_classify_tables_error_handling(self, mock_reader, sample_queries):
        """Test error handling during classification"""
        mock_reader.get_table_size.side_effect = [
            5.0,
            Exception("Simulated error"),
            25.0
        ]
        mock_reader.get_table_schema.return_value = "APP"
        
        result = classify_tables(mock_reader, sample_queries)
        
        # Verify it processed tables without error and logged the error
        assert len(result["T1"]) == 1  # users (5.0)
        assert len(result["T2"]) == 1  # customers (25.0)