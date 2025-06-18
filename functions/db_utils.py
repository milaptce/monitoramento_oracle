#db_utils.py 
import cx_Oracle
import logging
import os
from functools import wraps
from typing import List, Dict, Any, Optional, Union
from dotenv import load_dotenv
from pathlib import Path

logger = logging.getLogger(__name__)

def handle_db_errors(func):
    """
    Decorator para tratamento padrão de erros de banco de dados.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except cx_Oracle.DatabaseError as e:
            error = f"Erro de banco de dados em {func.__name__}: {e}"
            logger.error(error)
            if args and hasattr(args[0], '_last_error'):
                args[0]._last_error = error
            return None
        except Exception as e:
            error = f"Erro inesperado em {func.__name__}: {e}"
            logger.error(error)
            if args and hasattr(args[0], '_last_error'):
                args[0]._last_error = error
            return None
    return wrapper


class OracleTableReader:
    """
    Classe central para operações de banco de dados Oracle.
    Gerencia conexões, executa queries e fornece utilitários.
    """
    
    def __init__(self, env_file: str = None):
        """
        Inicializa o leitor de banco de dados.
        
        Args:
            env_file: Caminho opcional para arquivo .env personalizado
        """
        self._connection = None
        self._last_error = None
        self._load_config(env_file)
        
    def _load_config(self, env_file: str = None) -> None:
        """Carrega configurações do .env"""
        env_path = env_file or os.path.join(os.getenv('PROJECT_PATH', '.'), '.env')
        try:
            load_dotenv(env_path)
            self.db_config = {
                'host': os.getenv('DB_HOST'),
                'port': os.getenv('DB_PORT'),
                'service_name': os.getenv('DB_SERVICE_NAME'),
                'username': os.getenv('DB_USERNAME'),
                'password': os.getenv('DB_PASSWORD')
            }
            logger.debug("Configurações de banco de dados carregadas")
        except Exception as e:
            logger.error(f"Erro ao carregar configurações: {e}")
            raise
            
    def __enter__(self):
        """Suporte para context manager"""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Garante que a conexão será fechada"""
        self.close()
        
    @property
    def last_error(self) -> Optional[str]:
        """Retorna o último erro ocorrido"""
        return self._last_error
    
    def connect(self) -> bool:
        """
        Estabelece conexão com o banco de dados Oracle.
        
        Returns:
            bool: True se a conexão foi bem sucedida
        """
        if self.is_connected():
            return True
            
        try:
            dsn = cx_Oracle.makedsn(
                self.db_config['host'],
                self.db_config['port'],
                service_name=self.db_config['service_name']
            )
            self._connection = cx_Oracle.connect(
                user=self.db_config['username'],
                password=self.db_config['password'],
                dsn=dsn
            )
            logger.info("✅ Conexão com Oracle estabelecida com sucesso.")
            return True
        except cx_Oracle.DatabaseError as e:
            self._last_error = str(e)
            logger.error(f"❌ Falha na conexão com Oracle: {e}")
            return False
        except Exception as e:
            self._last_error = str(e)
            logger.error(f"❌ Erro inesperado ao conectar: {e}")
            return False
    
    def close(self) -> None:
        """Fecha a conexão com o banco de dados"""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("🔌 Conexão com Oracle encerrada.")
    
    def is_connected(self) -> bool:
        """Verifica se há uma conexão ativa"""
        return self._connection is not None
    
    @handle_db_errors
    def execute_query(self, query: str, params: Optional[dict] = None, 
                    fetch_all: bool = True) -> Optional[Union[List[tuple], tuple]]:
        """
        Executa uma query SQL e retorna os resultados.
        
        Args:
            query: Query SQL a ser executada
            params: Parâmetros para a query (opcional)
            fetch_all: Se True retorna todos os resultados, senão apenas um
            
        Returns:
            Resultados da query ou None em caso de erro
        """
        if not self.is_connected() and not self.connect():
            return None
                
        with self._connection.cursor() as cursor:
            cursor.execute(query, params or {})
            return cursor.fetchall() if fetch_all else cursor.fetchone()
    
    @handle_db_errors
    def get_table_size(self, table_name: str) -> Optional[float]:
        """
        Obtém o tamanho de uma tabela em MB.
        
        Args:
            table_name: Nome da tabela
            
        Returns:
            Tamanho em MB ou None se não encontrado
        """
        query = """
            SELECT bytes / 1024 / 1024 AS size_mb
            FROM dba_segments
            WHERE segment_name = UPPER(:table_name) AND segment_type = 'TABLE'
        """
        result = self.execute_query(query, {'table_name': table_name}, fetch_all=False)
        # Extração segura do valor - trata tanto resultados vazios quanto estrutura de tupla
        if not result:
            return None
        return result[0] if isinstance(result, (tuple, list)) else result
    
    @handle_db_errors
    def get_table_schema(self, table_name: str) -> Optional[str]:
        """
        Obtém o schema (owner) de uma tabela.
        
        Args:
            table_name: Nome da tabela
            
        Returns:
            Nome do schema ou None se não encontrado
        """
        query = "SELECT owner FROM all_tables WHERE table_name = UPPER(:table_name)"
        result = self.execute_query(query, {'table_name': table_name}, fetch_all=False)
        return result[0] if result else None
    
    @handle_db_errors
    def get_existing_indexes(self, table_name: str) -> List[str]:
        """
        Lista os índices existentes para uma tabela.
        
        Args:
            table_name: Nome da tabela
            
        Returns:
            Lista de nomes de índices
        """
        query = """
            SELECT index_name 
            FROM all_indexes 
            WHERE table_name = UPPER(:table_name)
        """
        results = self.execute_query(query, {'table_name': table_name})
        return [row[0] for row in results] if results else []

def handle_db_errors(func):
    """
    Decorator para tratamento padrão de erros de banco de dados.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except cx_Oracle.DatabaseError as e:
            error = f"Erro de banco de dados em {func.__name__}: {e}"
            logger.error(error)
            if args and hasattr(args[0], '_last_error'):
                args[0]._last_error = error
            return None
        except Exception as e:
            error = f"Erro inesperado em {func.__name__}: {e}"
            logger.error(error)
            if args and hasattr(args[0], '_last_error'):
                args[0]._last_error = error
            return None
    return wrapper