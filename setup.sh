# setup.sh
python -m venv fts
source fts/bin/activate
git clone https://github.com/milaptce/monitoramento_oracle.git 
cd monitoramento_oracle
pip install -r requirements.txt
echo "copiar .env para o diretorio raiz"
echo "GITHUB_TOKEN=YOUR_GITHUB_TOKEN" > .env
echo "GITHUB_REPO=milaptce/monitoramento_oracle" >> .env
echo "Setup concluído!" 
echo "Execute o script usando: python functions/github_updater.py --type source --branch main"