import pytest
from unittest.mock import patch, MagicMock, call
from main import OracleMonitor
from functions.db_utils import OracleTableReader
from functions.utils import ExecutionController
import logging
import os

class TestOracleMonitor:
    @pytest.fixture
    def monitor(self):
        """Fixture que retorna uma instância do OracleMonitor para testes"""
        return OracleMonitor()

    @pytest.fixture
    def mock_db_reader(self):
        """Fixture que retorna um mock do OracleTableReader"""
        mock = MagicMock(spec=OracleTableReader)
        mock.connect.return_value = True
        mock.is_connected.return_value = True
        return mock

    @pytest.fixture
    def mock_execution_controller(self):
        """Fixture que retorna um mock do ExecutionController"""
        mock = MagicMock(spec=ExecutionController)
        mock.check_first_run.return_value = False
        mock.schedule_next_run.return_value = "2025-06-19 12:00"
        return mock

    def test_log_execution_start_end(self, monitor, caplog):
        """Testa o registro de início e fim de execução"""
        with caplog.at_level(logging.INFO):
            monitor.log_execution_start()
            assert "🚀 Iniciando ciclo" in caplog.text
            assert monitor.execution_start is not None

            monitor.log_execution_end()
            assert "✅ Ciclo concluído" in caplog.text
            assert monitor.execution_end is not None

    @patch('main.OracleTableReader')
    def test_initialize_db_connection_success(self, mock_reader, monitor):
        """Testa conexão bem-sucedida com o banco"""
        mock_reader.return_value.connect.return_value = True
        assert monitor.initialize_db_connection() is True
        assert monitor.db_reader is not None

    @patch('main.OracleTableReader')
    def test_initialize_db_connection_failure(self, mock_reader, monitor, caplog):
        """Testa falha na conexão com o banco"""
        mock_reader.return_value.connect.return_value = False
        with caplog.at_level(logging.ERROR):
            assert monitor.initialize_db_connection() is False
            assert "❌ Erro na conexão" in caplog.text

    @patch('main.OracleMonitor.process_fts_queries')
    @patch('main.OracleMonitor.initialize_db_connection')
    def test_run_success(self, mock_init, mock_process, monitor, caplog):
        """Testa execução completa bem-sucedida"""
        mock_init.return_value = True
        mock_process.return_value = True
        
        with caplog.at_level(logging.INFO):
            assert monitor.run() is True
            assert "🚀 Iniciando ciclo" in caplog.text
            assert "✅ Ciclo concluído" in caplog.text

    @patch('main.OracleMonitor.initialize_db_connection')
    def test_run_db_connection_failure(self, mock_init, monitor, caplog):
        """Testa execução com falha na conexão"""
        mock_init.return_value = False
        
        with caplog.at_level(logging.ERROR):
            assert monitor.run() is False
            assert "Falha na conexão" in caplog.text

    @patch('main.identify_fts_queries')
    @patch('main.group_similar_queries')
    @patch('main.classify_tables')
    @patch('main.evaluate_performance')
    @patch('main.generate_scripts')
    def test_process_fts_queries_success(self, mock_gen, mock_eval, mock_class, mock_group, mock_id, monitor, mock_db_reader):
        """Testa o fluxo completo de processamento"""
        monitor.db_reader = mock_db_reader
        mock_id.return_value = [{"sql": "SELECT * FROM users"}]
        mock_group.return_value = [{"count": 1, "tables": ["users"], "priority_score": 1}]
        mock_class.return_value = {"T1": [{"table": "users", "size_mb": 5.0}]}
        mock_eval.return_value = [{"table": "users", "suggestion": "create index"}]
        mock_gen.return_value = ["script1.sql"]
        
        assert monitor.process_fts_queries() is True

    @patch('main.identify_fts_queries')
    def test_process_fts_queries_no_results(self, mock_id, monitor, mock_db_reader, caplog):
        """Testa processamento quando não há queries FTS"""
        monitor.db_reader = mock_db_reader
        mock_id.return_value = []
        
        with caplog.at_level(logging.WARNING):
            assert monitor.process_fts_queries() is False
            assert "Nenhuma query FTS" in caplog.text

    @patch('main.identify_fts_queries')
    def test_process_fts_queries_error(self, mock_id, monitor, mock_db_reader, caplog):
        """Testa tratamento de erro durante o processamento"""
        monitor.db_reader = mock_db_reader
        mock_id.side_effect = Exception("Test error")
        
        with caplog.at_level(logging.ERROR):
            assert monitor.process_fts_queries() is False
            assert "Erro durante o processamento" in caplog.text

    @patch.dict('os.environ', {'GITHUB_TOKEN': 'test', 'GITHUB_REPO': 'test/repo'})
    @patch('main.update_github')
    def test_update_github_success(self, mock_update, monitor, caplog):
        """Testa atualização do GitHub com credenciais configuradas"""
        mock_update.return_value = True
        
        with caplog.at_level(logging.INFO):
            monitor._update_github()
            assert "Scripts atualizados" in caplog.text

    @patch.dict('os.environ', {})
    def test_update_github_no_credentials(self, monitor, caplog):
        """Testa atualização do GitHub sem credenciais"""
        with caplog.at_level(logging.WARNING):
            monitor._update_github()
            assert "Credenciais GitHub não configuradas" in caplog.text

    @patch('main.ExecutionController')
    def test_first_run_check(self, mock_controller, monitor):
        """Testa verificação de primeira execução"""
        mock_instance = mock_controller.return_value
        mock_instance.check_first_run.return_value = True
        
        with patch.object(monitor, 'initialize_db_connection', return_value=True):
            with patch.object(monitor, 'process_fts_queries', return_value=True):
                monitor.run()
                mock_instance.check_first_run.assert_called_once()

    @patch('main.ExecutionController')
    def test_schedule_next_run(self, mock_controller, monitor):
        """Testa agendamento da próxima execução"""
        mock_instance = mock_controller.return_value
        mock_instance.schedule_next_run.return_value = "2025-06-19 12:00"
        
        with patch.object(monitor, 'initialize_db_connection', return_value=True):
            with patch.object(monitor, 'process_fts_queries', return_value=True):
                monitor.run()
                mock_instance.schedule_next_run.assert_called_once_with(hours=6)