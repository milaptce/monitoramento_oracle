Monitor Oracle FTS
O projeto monitor_oracle_fts é uma solução modular desenvolvida em Python para monitorar continuamente queries que realizam full table scans (FTS) no banco de dados Oracle 12C. O objetivo é identificar problemas de performance, classificar tabelas impactadas, sugerir melhorias e gerar scripts SQL automaticamente.

Funcionalidades Principais
Identificação de Queries FTS :
Monitora queries que utilizam FULL SCAN ou TABLE ACCESS FULL.
Registra o nome do esquema associado à query.
Classificação de Tabelas :
Classifica tabelas em dois grupos:
T1 : Tabelas pequenas (< 10MB).
T2 : Tabelas grandes (>= 10MB).
Análise de Performance :
Avalia o impacto de melhorias, como criação de índices ou refatoração de queries.
Calcula o percentual estimado de melhoria de performance.
Geração de Scripts :
Cria scripts SQL para implementar soluções sugeridas.
Armazena os scripts gerados no diretório output/generated_scripts/.
Integração com GitHub :
Atualiza automaticamente os scripts gerados no repositório GitHub.
Execução Contínua :
Executa periodicamente a cada 6 horas, iniciando às 00:00h.
Controla a primeira execução usando o arquivo execucao.ini.
Estrutura do Projeto
monitor_oracle_fts/
├── .env                     # Arquivo de configuração (credenciais)
├── .gitignore               # Ignora arquivos sensíveis
├── requirements.txt         # Dependências do projeto
├── fts.sh                   # Script para inicialização do projeto
├── main.py                  # Script principal
├── functions/               # Funções modulares
│   ├── login_db.py          # Conexão ao banco de dados
│   ├── query_monitor.py     # Identificação de queries FTS
│   ├── table_analysis.py    # Classificação de tabelas
│   ├── performance_improvement.py  # Análise de performance
│   ├── script_generator.py  # Geração de scripts SQL
│   └── github_updater.py    # Atualização no GitHub
├── logs/                    # Logs de execução
├── output/                  # Saída dos scripts gerados
└── config/                  # Configurações adicionais
    ├── execucao.ini         # Controle de execução
    └── schemas.json         # Lista de esquemas monitorados

Pré-requisitos
Python 3.x instalado.
Acesso administrativo ao banco de dados Oracle 12C.

Bibliotecas Python:
pip install -r requirements.txt

Credenciais do GitHub para integração automática.
Configuração Inicial
1. Clone o Repositório
git clone https://github.com/seu-usuario/monitor_oracle_fts.git 
cd monitor_oracle_fts
