import os
import logging
import configparser
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from functions.db_utils import OracleTableReader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('monitor.log')
    ]
)
logger = logging.getLogger(__name__)
# Carregar variáveis de ambiente
load_dotenv()

class ExecutionController:
    """Controla o estado e agendamento das execuções do monitor"""
    
    def __init__(self, config_path: str = "config/execution.ini"):
        self.config_path = Path(config_path)
        self.config = configparser.ConfigParser()
        self._ensure_config_dir()
        
        if self.config_path.exists():
            self.config.read(self.config_path)

    def _ensure_config_dir(self) -> None:
        self.config_path.parent.mkdir(parents=True, exist_ok=True)

    def check_first_run(self) -> bool:
        if not self.config_path.exists():
            self._init_config_file()
            return True
            
        if not self.config.has_section("Execution"):
            self._init_config_file()
            return True
            
        first_run = self.config.get("Execution", "FIRST_RUN", fallback="1") == "1"
        
        if first_run:
            self._update_config_first_run()
            
        return first_run

    def _init_config_file(self) -> None:
        self.config["Execution"] = {
            "FIRST_RUN": "0",
            "LAST_RUN_TIMESTAMP": datetime.now().isoformat(),
            "NEXT_RUN_TIMESTAMP": ""
        }
        self._save_config()

    def _update_config_first_run(self) -> None:
        self.config.set("Execution", "FIRST_RUN", "0")
        self.config.set("Execution", "LAST_RUN_TIMESTAMP", datetime.now().isoformat())
        self._save_config()

    def schedule_next_run(self, hours: int = 6) -> str:
        next_run = datetime.now() + timedelta(hours=hours)
        next_run_str = next_run.strftime("%Y-%m-%d %H:%M")
        
        self.config.read(self.config_path)
        self.config.set("Execution", "NEXT_RUN_TIMESTAMP", next_run_str)
        self._save_config()
        
        return next_run_str

    def _save_config(self) -> None:
        with open(self.config_path, 'w') as configfile:
            self.config.write(configfile)

class OracleMonitor:
    def __init__(self, execution_controller: Optional[ExecutionController] = None):
        self.logger = logging.getLogger(f"{__name__}.OracleMonitor")
        self.db_reader = None
        self.execution_start = None
        self.execution_end = None
        self.execution_controller = execution_controller or ExecutionController()

    def log_execution_start(self) -> None:
        self.execution_start = datetime.now()
        self.logger.info("🚀 Iniciando ciclo de monitoramento de Full Table Scans")

    def log_execution_end(self) -> None:
        self.execution_end = datetime.now()
        duration = self.execution_end - self.execution_start
        self.logger.info(f"✅ Ciclo concluído em {duration.total_seconds():.2f} segundos")

    def initialize_db_connection(self) -> bool:
        try:
            self.db_reader = OracleTableReader()
            if self.db_reader.connect():
                return True
            self.logger.error("❌ Erro na conexão com o banco de dados")
            return False
        except Exception as e:
            self.logger.error(f"❌ Erro inesperado na conexão: {str(e)}")
            return False

    def run(self) -> bool:
        self.log_execution_start()
        
        # First run check - must be called before any other operation
        if self.execution_controller.check_first_run():
            self.logger.info("⭐ Primeira execução do sistema detectada")
        
        if not self.initialize_db_connection():
            self.logger.error("Falha na conexão com o banco de dados")
            return False
        
        if not self.process_fts_queries():
            self.log_execution_end()
            return False
        
        self._update_github()
        
        # Schedule next run - must be called at the end
        self.execution_controller.schedule_next_run(hours=6)
        
        self.log_execution_end()
        return True

    def process_fts_queries(self) -> bool:
        try:
            queries = identify_fts_queries(self.db_reader)
            if not queries:
                self.logger.warning("Nenhuma query FTS identificada")
                return False
            
            grouped_queries = group_similar_queries(queries)
            self._log_query_groups(grouped_queries)
            
            classified_tables = classify_tables(self.db_reader, grouped_queries)
            performance_data = evaluate_performance(self.db_reader, classified_tables)
            generate_scripts(performance_data)
            
            return True
        except Exception as e:
            self.logger.error(f"❌ Erro durante o processamento: {str(e)}")
            return False

    def _log_query_groups(self, grouped_queries: List[Dict[str, Any]]) -> None:
        top_groups = sorted(
            grouped_queries,
            key=lambda x: x.get('priority_score', 0),
            reverse=True
        )[:5]
        
        for i, group in enumerate(top_groups, 1):
            self.logger.info(
                f"🔍 Grupo {i}: {group['count']} execuções "
                f"em tabelas: {', '.join(group['tables'])}"
            )

    def _update_github(self) -> bool:
        if not (os.getenv("GITHUB_TOKEN") and os.getenv("GITHUB_REPO")):
            self.logger.warning("⚠️ Credenciais GitHub não configuradas - Pulando atualização")
            return False
        
        try:
            if update_github():
                self.logger.info("🔄 Scripts atualizados no GitHub")
                return True
            return False
        except Exception as e:
            self.logger.error(f"❌ Erro ao atualizar GitHub: {str(e)}")
            return False

# Helper functions
def identify_fts_queries(db_reader: OracleTableReader) -> List[Dict[str, Any]]:
    return []

def group_similar_queries(queries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return []

def classify_tables(db_reader: OracleTableReader, query_groups: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {}

def evaluate_performance(db_reader: OracleTableReader, classified_tables: Dict[str, Any]) -> List[Dict[str, Any]]:
    return []

def generate_scripts(performance_data: List[Dict[str, Any]]) -> List[str]:
    return []

def update_github() -> bool:
    return True

if __name__ == "__main__":
    monitor = OracleMonitor()
    monitor.run()