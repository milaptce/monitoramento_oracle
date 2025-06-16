#!/bin/bash

# Verifica se as variáveis de ambiente estão definidas
if [ -z "$GITHUB_TOKEN" ] || [ -z "$GITHUB_REPO" ]; then
  echo "Erro: Variáveis GITHUB_TOKEN ou GITHUB_REPO não definidas."
  exit 1
fi

# Configura o usuário Git correto
git config user.name "milaptce"
git config user.email "milap.tce@gmail.com"

# Adiciona todos os arquivos modificados
git add .

# Faz o commit com uma mensagem específica
git commit -m "Atualização automática de scripts"

# Envia as alterações para o repositório remoto
git push https://$GITHUB_TOKEN@github.com/$GITHUB_REPO.git 