
import os
import requests
import base64
import argparse
from dotenv import load_dotenv
import pathspec  # Para interpretar o .gitignore

# Carrega variáveis de ambiente
load_dotenv()

"""
==============================
COMO EXECUTAR ESTE MÓDULO
==============================

Este módulo permite atualizar arquivos no repositório GitHub de forma parametrizada.
Ele pode ser executado diretamente via linha de comando.

1. ATUALIZAR CÓDIGO-FONTE:
   Execute o comando abaixo para atualizar os arquivos de código-fonte no repositório GitHub:
   python functions/github_updater.py --type source --branch main


2. ATUALIZAR ARQUIVOS GERADOS:
Execute o comando abaixo para atualizar os arquivos gerados no repositório GitHub:
python functions/github_updater.py --type generated --branch main


3. AJUDA:
Para ver as opções disponíveis, use:
python functions/github_updater.py --help

"""



# Carrega variáveis de ambiente
load_dotenv()

def create_directory_structure(repo, token, relative_path, branch="main"):
    """
    Cria diretórios recursivamente no repositório GitHub.

    Args:
        repo (str): Nome do repositório no formato `username/repo_name`.
        token (str): Token de acesso pessoal do GitHub.
        relative_path (str): Caminho relativo do arquivo ou diretório.
        branch (str): Branch do repositório GitHub.
    """
    github_api_url = f"https://api.github.com/repos/{repo}/contents" 
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }

    # Divide o caminho em partes
    parts = relative_path.split("/")
    current_path = ""
    for part in parts[:-1]:  # Ignora o último elemento (nome do arquivo)
        current_path += f"{part}/"
        response = requests.get(f"{github_api_url}/{current_path}", headers=headers)
        if response.status_code == 404:
            print(f"Diretório não encontrado. Criando: {current_path}")
            payload = {
                "message": f"Criação automática de diretório: {current_path}",
                "content": "",  # Diretórios são criados enviando um conteúdo vazio
                "branch": branch
            }
            response = requests.put(f"{github_api_url}/{current_path}", headers=headers, json=payload)
            if response.status_code not in [200, 201]:
                print(f"Erro ao criar diretório {current_path}: {response.status_code} - {response.text}")
                return


def update_github(directory, message_prefix, branch="main"):
    """
    Atualiza arquivos no repositório GitHub.
    
    Args:
        directory (str): Diretório raiz dos arquivos a serem enviados.
        message_prefix (str): Prefixo da mensagem de commit.
        branch (str): Branch do repositório GitHub.
    """
    token = os.getenv("GITHUB_TOKEN")
    repo = os.getenv("GITHUB_REPO")

    if not token or not repo:
        print("Erro: Token do GitHub ou nome do repositório não configurados no .env.")
        return

    github_api_url = f"https://api.github.com/repos/{repo}/contents" 
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }

    # Verifica se o diretório existe
    if not os.path.exists(directory):
        print(f"Erro: Diretório '{directory}' não encontrado.")
        return

    # Lê o .gitignore e cria uma especificação de exclusão
    gitignore_path = os.path.join(directory, ".gitignore")
    spec = None
    if os.path.exists(gitignore_path):
        with open(gitignore_path, "r", encoding="utf-8") as gitignore_file:
            spec = pathspec.PathSpec.from_lines(pathspec.patterns.GitWildMatchPattern, gitignore_file)

    def list_all_files(directory):
        """Lista todos os arquivos no diretório e subdiretórios, exceto os ignorados pelo .gitignore."""
        for root, _, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, directory)
                # Ignora arquivos que correspondem ao .gitignore
                if spec and spec.match_file(relative_path):
                    print(f"Ignorando arquivo (via .gitignore): {relative_path}")
                    continue
                yield file_path

    # Itera sobre os arquivos
    files_found = False
    for file_path in list_all_files(directory):
        files_found = True
        try:
            relative_path = os.path.relpath(file_path, ".")  # Mantém a estrutura relativa
            print(f"Processando arquivo: {relative_path}")

            # Cria diretórios recursivamente, se necessário
            create_directory_structure(repo, token, relative_path, branch)

            with open(file_path, "r", encoding="utf-8") as file:
                content = file.read()
            encoded_content = base64.b64encode(content.encode("utf-8")).decode("utf-8")

            response = requests.get(f"{github_api_url}/{relative_path}", headers=headers)
            if response.status_code == 404:
                print(f"Arquivo não encontrado no GitHub: {relative_path}. Criando novo arquivo.")
                payload = {
                    "message": f"{message_prefix}: {relative_path}",
                    "content": encoded_content,
                    "branch": branch
                }
                response = requests.put(f"{github_api_url}/{relative_path}", headers=headers, json=payload)
            elif response.status_code == 200:
                sha = response.json().get("sha")
                payload = {
                    "message": f"{message_prefix}: {relative_path}",
                    "content": encoded_content,
                    "sha": sha,
                    "branch": branch
                }
                print(f"Atualizando arquivo existente: {relative_path}")
                response = requests.put(f"{github_api_url}/{relative_path}", headers=headers, json=payload)
            else:
                print(f"Erro ao acessar arquivo no GitHub: {response.status_code} - {response.text}")
                continue

            if response.status_code in [200, 201]:
                print(f"Arquivo enviado com sucesso: {relative_path}")
            else:
                print(f"Erro ao enviar arquivo {relative_path}: {response.status_code} - {response.text}")

        except Exception as e:
            print(f"Erro ao processar o arquivo {file_path}: {e}")

    if not files_found:
        print(f"Nenhum arquivo encontrado no diretório: {directory}")


if __name__ == "__main__":
    # Configuração do parser de argumentos
    parser = argparse.ArgumentParser(
        description="Atualiza arquivos no repositório GitHub.",
        epilog="""
        Exemplos de uso:
        1. Atualizar código-fonte:
           python functions/github_updater.py --type source --branch main

        2. Atualizar arquivos gerados:
           python functions/github_updater.py --type generated --branch main
        """
    )
    parser.add_argument(
        "--type",
        choices=["source", "generated"],
        required=True,
        help="Tipo de atualização: 'source' para código-fonte ou 'generated' para arquivos gerados."
    )
    parser.add_argument(
        "--branch",
        default="main",
        help="Branch do repositório GitHub (padrão: main)."
    )

    args = parser.parse_args()

    if args.type == "source":
        # Atualiza arquivos de código-fonte
        update_github(
            directory=".",
            message_prefix="Atualização automática do código-fonte",
            branch=args.branch
        )
    elif args.type == "generated":
        # Atualiza arquivos gerados
        update_github(
            directory="output/generated_scripts",
            message_prefix="Atualização automática de scripts gerados",
            branch=args.branch
        )