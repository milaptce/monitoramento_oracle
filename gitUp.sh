#!/bin/bash

# Carregar variáveis do .env
export $(grep -v '^#' .env | xargs)

# Garantir que estamos no diretório correto
cd /home/milap/dev/app/oracle/monitoramento_oracle || exit

# Configurar remote (atualiza se já existe)
git remote set-url origin https://$GITHUB_TOKEN@github.com/$GITHUB_REPO.git 

# Adicionar, commitar e enviar
git status
git add .
git commit -m "Atualização automática: $(date '+%Y-%m-%d %H:%M')"
git push origin dev
git remote -v 