# test_db_utils.py
import pytest
from unittest.mock import MagicMock, patch
from functions.db_utils import OracleTableReader

class TestOracleTableReader:
    @patch('cx_Oracle.connect')
    def test_connection(self, mock_connect):
        mock_connect.return_value = MagicMock()
        reader = OracleTableReader()
        assert reader.connect() is True
    
    def test_execute_query(self):
        reader = OracleTableReader()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("test",)]
        
        reader._connection = MagicMock()
        reader._connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        result = reader.execute_query("SELECT 1 FROM DUAL")
        assert result == [("test",)]
    
    @patch('functions.db_utils.OracleTableReader.execute_query')
    def test_get_table_size(self, mock_execute):
        # Configura o mock para retornar o valor diretamente (não uma tupla)
        mock_execute.return_value = 10.5  # Retorno direto do valor float
        
        reader = OracleTableReader()
        size = reader.get_table_size("users")
        
        assert size == 10.5
        assert isinstance(size, float)

    @patch('functions.db_utils.OracleTableReader.execute_query')
    def test_get_table_size_with_tuple(self, mock_execute):
        # Teste alternativo para quando o Oracle retorna uma tupla
        mock_execute.return_value = (10.5,)  # Formato de tupla
        
        reader = OracleTableReader()
        size = reader.get_table_size("users")
        
        assert size == 10.5
        assert isinstance(size, float)

    @patch('functions.db_utils.OracleTableReader.execute_query')
    def test_get_table_size_not_found(self, mock_execute):
        mock_execute.return_value = None  # Simula tabela não encontrada
        reader = OracleTableReader()
        size = reader.get_table_size("nonexistent")
        assert size is None