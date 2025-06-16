# functions/github_updater_v2.py

import os
import subprocess
import logging
from dotenv import load_dotenv

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Carregar variáveis do .env
load_dotenv()

def update_github():
    """
    Realiza git add, commit e push automático para o repositório GitHub.
    Utiliza variáveis de ambiente para credenciais e nome do repositório.
    """
    github_token = os.getenv("GITHUB_TOKEN")
    github_repo = os.getenv("GITHUB_REPO")

    if not github_token or not github_repo:
        logger.error("❌ GITHUB_TOKEN ou GITHUB_REPO não configurados.")
        return False

    try:
        # Mudar para o diretório do projeto
        repo_path = os.getenv("PROJECT_PATH", os.getcwd())
        os.chdir(repo_path)

        # Executar git add .
        logger.info("🔄 Executando git add .")
        subprocess.run(["git", "add", "."], check=True)

        # Executar git commit (com mensagem automática)
        logger.info("📦 Executando git commit")
        subprocess.run(["git", "commit", "-m", "Atualização automática de scripts FTS"], check=False)

        # Montar URL segura com token
        remote_url = f"https://{github_token}@github.com/{github_repo}.git" 

        # Definir remote (opcional, se ainda não estiver definido)
        logger.info("🔧 Configurando remote origin")
        subprocess.run(["git", "remote", "set-url", "origin", remote_url], check=True)

        # Exibir remote -v
        logger.info("🔍 git remote -v")
        subprocess.run(["git", "remote", "-v"], check=True)

        # Executar git push
        logger.info("📤 Enviando alterações para GitHub")
        subprocess.run(["git", "push", remote_url, "dev"], check=True)

        logger.info("✅ Push realizado com sucesso!")
        return True

    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Erro ao executar comandos Git: {e}")
        return False