# utils.py
import os
import logging
import configparser
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from pathlib import Path
from sql_metadata import Parser

# Configuração básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SQLParser:
    """Responsável por analisar e extrair informações de queries SQL"""
    
    @staticmethod
    def extract_tables(sql_text: str) -> List[str]:
        """
        Extrai nomes de tabelas de uma query SQL.
        
        Args:
            sql_text: Texto completo da query SQL
            
        Returns:
            Lista de nomes de tabelas encontradas
        """
        try:
            tables = Parser(sql_text).tables
            logger.debug(f"Tabelas extraídas: {tables}")
            return tables
        except Exception as e:
            logger.warning(f"Erro ao extrair tabelas: {e}")
            return []

    @staticmethod
    def extract_where_conditions(sql_text: str) -> List[str]:
        """
        Extrai condições WHERE de uma query SQL de forma robusta.
        
        Args:
            sql_text: Texto completo da query SQL
            
        Returns:
            Lista de condições WHERE encontradas
        """
        try:
            if " WHERE " not in sql_text.upper():
                return []
                
            # Extrai a parte após WHERE mantendo o case original
            where_part = sql_text.split(" WHERE ")[1].split(";")[0]
            
            # Simplificação para casos básicos
            conditions = []
            for condition in where_part.split(" AND "):
                condition = condition.strip()
                if condition:
                    # Remove sub-expressões OR para simplificar
                    main_condition = condition.split(" OR ")[0].strip()
                    # Remove possíveis parênteses
                    main_condition = main_condition.replace("(", "").replace(")", "")
                    if main_condition:
                        conditions.append(main_condition)
            
            logger.debug(f"Condições WHERE extraídas: {conditions}")
            return conditions
            
        except Exception as e:
            logger.warning(f"Erro ao extrair condições WHERE: {e}")
            return []


class ExecutionController:
    """Controla o estado e agendamento das execuções do monitor"""
    
    def __init__(self, config_path: str = "config/execution.ini"):
        """
        Inicializa o controlador de execução.
        
        Args:
            config_path: Caminho para o arquivo de configuração
        """
        self.config_path = Path(config_path)
        self.config = configparser.ConfigParser()
        self._ensure_config_dir()
        
        # Carrega configuração se o arquivo existir
        if self.config_path.exists():
            self.config.read(self.config_path)

    def _ensure_config_dir(self) -> None:
        """Garante que o diretório de configuração existe"""
        self.config_path.parent.mkdir(parents=True, exist_ok=True)

    def check_first_run(self) -> bool:
        """
        Verifica se é a primeira execução do sistema.
        Retorna True apenas na primeira execução real.
        """
        # Se o arquivo não existe, é primeira execução
        if not self.config_path.exists():
            self._init_config_file()
            return True
            
        # Se existe mas não tem a seção Execution
        if not self.config.has_section("Execution"):
            self._init_config_file()
            return True
            
        # Verifica o flag FIRST_RUN
        first_run = self.config.get("Execution", "FIRST_RUN", fallback="1") == "1"
        
        # Se for primeira execução, atualiza o arquivo
        if first_run:
            self._update_config_first_run()
            
        return first_run

    def _init_config_file(self) -> None:
        """Inicializa o arquivo de configuração para primeira execução"""
        self.config["Execution"] = {
            "FIRST_RUN": "0",  # Já marca como não é mais primeira execução
            "LAST_RUN_TIMESTAMP": datetime.now().isoformat(),
            "NEXT_RUN_TIMESTAMP": ""
        }
        self._save_config()
        logger.info("Arquivo de configuração inicializado")

    def _update_config_first_run(self) -> None:
        """Atualiza o status após a primeira execução"""
        self.config.set("Execution", "FIRST_RUN", "0")
        self.config.set("Execution", "LAST_RUN_TIMESTAMP", datetime.now().isoformat())
        self._save_config()
        logger.info("Configuração de primeira execução atualizada")

    def schedule_next_run(self, hours: int = 6) -> str:
        """
        Agenda a próxima execução do monitor.
        
        Args:
            hours: Horas até a próxima execução
            
        Returns:
            Timestamp da próxima execução formatada
        """
        next_run = datetime.now() + timedelta(hours=hours)
        next_run_str = next_run.strftime("%Y-%m-%d %H:%M")
        
        self.config.read(self.config_path)
        self.config.set("Execution", "NEXT_RUN_TIMESTAMP", next_run_str)
        self._save_config()
        
        logger.info(f"Próxima execução agendada para {next_run_str}")
        return next_run_str

    def _save_config(self) -> None:
        """Salva o arquivo de configuração"""
        with open(self.config_path, "w") as config_file:
            self.config.write(config_file)


class OracleUtils:
    """Utilitários específicos para Oracle Database"""
    
    @staticmethod
    def format_sql_query(query: str, params: Dict[str, Any] = None) -> str:
        """
        Formata uma query SQL com parâmetros para logging.
        
        Args:
            query: Query SQL
            params: Dicionário de parâmetros
            
        Returns:
            Query formatada como string
        """
        if not params:
            return query
            
        formatted = query
        for key, value in params.items():
            formatted = formatted.replace(f":{key}", str(value))
        return formatted


def setup_logging(log_dir: str = "logs", log_file: str = "monitor.log", level: int = logging.INFO) -> None:
    """
    Configura o sistema de logging centralizado.
    
    Args:
        log_dir: Diretório para armazenar logs
        log_file: Nome do arquivo de log
        level: Nível de logging (default: INFO)
    """
    log_path = Path(log_dir) / log_file
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    handlers = [
        logging.StreamHandler(),
        logging.FileHandler(log_path)
    ]
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )
    logger.info(f"Logging configurado. Arquivo: {log_path}")