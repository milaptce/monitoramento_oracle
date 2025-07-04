Monitor Oracle FTS
O projeto monitor_oracle_fts é uma solução modular desenvolvida em Python para monitorar continuamente queries que realizam full table scans (FTS) no banco de dados Oracle 12C. O objetivo é identificar problemas de performance, classificar tabelas impactadas, sugerir melhorias e gerar scripts SQL automaticamente.

Funcionalidades Principais
Identificação de Queries FTS :
Monitora queries que utilizam FULL SCAN ou TABLE ACCESS FULL.
Registra o nome do esquema associado à query.
Classificação de Tabelas :<br>
Classifica tabelas em dois grupos:<br>
T1 : Tabelas pequenas (< 10MB).<br>
T2 : Tabelas grandes (>= 10MB).<br><br>
Análise de Performance :<br>
Avalia o impacto de melhorias, como criação de índices ou refatoração de queries.<br>
Calcula o percentual estimado de melhoria de performance.<br>
Geração de Scripts :<br>
Cria scripts SQL para implementar soluções sugeridas.<br>
Armazena os scripts gerados no diretório output/generated_scripts/. <br>
Integração com GitHub :<br>
Atualiza automaticamente os scripts gerados no repositório GitHub. <br>
Execução Contínua :<br>
Executa periodicamente a cada 6 horas, iniciando às 00:00h.<br>
Controla a primeira execução usando o arquivo execucao.ini.<br>
Estrutura do Projeto<br>
monitor_oracle_fts/<br>
├── .env                     # Arquivo de configuração (credenciais) <br>
├── .gitignore               # Ignora arquivos sensíveis <br>
├── requirements.txt         # Dependências do projeto <br>
├── fts.sh                   # Script para inicialização do projeto <br>
├── main.py                  # Script principal <br>
├── functions/               # Funções modulares <br>
│   ├── login_db.py          # Conexão ao banco de dados <br> 
│   ├── query_monitor.py     # Identificação de queries FTS <br>
│   ├── table_analysis.py    # Classificação de tabelas
│   ├── performance_improvement.py  # Análise de performance
│   ├── script_generator.py  # Geração de scripts SQL <br>
│   └── github_updater.py    # Atualização no GitHub <br>
├── logs/                    # Logs de execução <br>
├── output/                  # Saída dos scripts gerados <br>
└── config/                  # Configurações adicionais <br>
    ├── execucao.ini         # Controle de execução<br>
    └── schemas.json         # Lista de esquemas monitorados<br>
<br>
<br>
Pré-requisitos<br>
Python 3.x instalado.<br>
Acesso administrativo ao banco de dados Oracle 12C.<br>
<br>
Bibliotecas Python:<br>
pip install -r requirements.txt<br>
<br>
Credenciais do GitHub para integração automática.<br>
Configuração Inicial<br>
1. Clone o Repositório<br>
git clone https://github.com/seu-usuario/monitor_oracle_fts.git <br>
cd monitor_oracle_fts<br>
<br>
Ref<br>
precisamos monitorar continuamente queries que estão fazendo full table scan.<br>
primeiro passo é identificar as queries Q1<br>
deste conjunto Q1, temos dois cenários: tabelas pequenas T1 e não-pequenas T2<br>
para maior dinamismo deste processo, estes dois conjuntos T1 e T2 terão cenários de monitoramentos diferentes.<br>
Para T1, avaliar as melhores opções como criação de indice, refatorar a query etc.<br>
Para T2, a análise deverá ser muito minuciosa pois se trata de tabelas grandes e significativo impacto na performance como um todo. <br>
Para T2, faz-se necessário gerar um conjunto de regras/queries que precisam ser interpretadas no mesmo processo indicando quanto de melhoria de performance, em percental, poderá ser alcançado.<br>
tanto cenário T1 quanto T2 precisamos manter as seguintes informações: tabelas envolvidas e respectivos tamanhos, <br>
a query, numero de vezes que aparece; Com estas informações, deverá ser indicado dentro da solução, <br>
o grau de prioridade na solução do full table scan, por ex, uma query que tem pouca frequencia, <br>
baixo volume de linhas terá baixa prioridade (10 na escala 1 a 10).<br>
fazendo um apanhado geral, o objtetivo deste monitoramento contínuo deverá proporcionar indicadores significativos de melhoria de performance, <br>
gerar automaticamente todos scripts necessários e complementares para - através de seus outputs - retroalimentar o sistema para mitigar todos os problemas de full table scan.<br>
todos os scritps deverão ser escritos em python<br>
todas as funções necessárias a esta solução deverão ser isoladas e independentes em outros programas   python possibilitando a execuçao paralela de acordo com o respectivo e programado input.<br>
Digamos que serão gerados n funções para contemplar 100% do cenário de monitoramento de full table scan, <br>
não haverá problema algum que sejam executadas k funcoes (k <n) no cenário disponibilizado, ou seja, <br>
mesmo não havendo cenários reais, teremos todos cenários programados e preparados.<br>
<br>
Dito isso, considerar .env para as credenciais, oracle versao 12C e acesso nivel DBA.<br>
Github será utilizado. Portando uma esteira para carregar/atualizar os arquivos no github (.sh) deverá ser gerado.<br>
Por se tratar de um monitoramento contínuo, há a possibilidade de se repertir as queries de full table scan, <br>
neste cenário, apenas contabilizar o numero de vezes e indicar que a solução já foi produzida (query id, query-soluçao-json formato) ou algo mais efetivo<br>
A periodicidade deste monitoramento deverá ocorrer de 6 em 6 horas, iniciando-se sempre as 0.00h. No entanto, a primeira execução poderá ocorrer em qualquer momento. <br>
Para indicar que esta primeira execucao foi realizada com sucesso, um flag FIRST_RUN=1 deverá ser registro em execucao.ini, <br>
ou seja, estando =1 a proxima execução seguirá o fluxo 6 em 6 horas, iniciando e terminando às 0:00 h (0-6-12-18-0)<br>
<br>
Vamos decompor o problema e estruturar a solução de forma organizada, considerando todos os requisitos descritos. <br>
A solução será modular, com funções isoladas em arquivos Python independentes, e integrará as funcionalidades necessárias para monitorar continuamente <br>
queries que fazem full table scan (FTS) em um banco de dados Oracle 12C. <br>
Abaixo está uma visão geral da abordagem:<br>
<br>
<br>
### **Estrutura do Projeto**<br>
1. **Arquivos principais**:<br>
   - `main.py`: Script principal que coordena a execução das funções.<br>
   - `query_monitor.py`: Funções para identificar queries FTS.<br>
   - `table_analysis.py`: Funções para classificar tabelas (T1 e T2).<br>
   - `performance_improvement.py`: Funções para avaliar melhorias de performance.<br>
   - `script_generator.py`: Funções para gerar scripts SQL e outros complementares.<br>
   - `github_updater.sh`: Script shell para atualizar arquivos no GitHub.<br>
   - `.env`: Arquivo para armazenar credenciais de acesso ao banco de dados.<br>
   - `execucao.ini`: Arquivo para controlar a primeira execução e o fluxo subsequente.<br>
<br>
2. **Diretórios**:
   - `functions/`: Contém os módulos Python independentes (`query_monitor.py`, `table_analysis.py`, etc.).
   - `logs/`: Armazena logs de execução.
   - `output/`: Armazena resultados, como JSONs e scripts gerados.

3. **Requisitos de ambiente**:
   - Python 3.x
   - Bibliotecas: `cx_Oracle`, `dotenv`, `json`, `os`, `datetime`, etc.
   - Shell script para integração com GitHub.

---

### **Passo a Passo da Solução**

#### **1. Configuração Inicial**
- **Arquivo `.env`**:
  ```plaintext
  ORACLE_USER=your_username
  ORACLE_PASSWORD=your_password
  ORACLE_DSN=your_dsn
  GITHUB_TOKEN=your_github_token
  GITHUB_REPO=your_repo_name
  ```

- **Arquivo `execucao.ini`**:
  ```ini
  [Execution]
  FIRST_RUN=0
  LAST_RUN_TIMESTAMP=
  ```

#### **2. Identificação de Queries FTS**
- **Função**: `identify_fts_queries()`
  - Consulta as views do Oracle (`V$SQL`, `DBA_TABLES`, etc.) para identificar queries que realizam FTS.
  - Exemplo de query SQL:
    ```sql
    SELECT sql_id, sql_text, executions, elapsed_time
    FROM v$sql
    WHERE sql_text LIKE '%/*+ FULL(%' OR sql_text LIKE '%TABLE ACCESS FULL%';
    ```
  - Retorna um DataFrame ou lista com as queries FTS identificadas.

- **Saída**:
  - Lista de queries com informações como `sql_id`, `sql_text`, `executions`, `elapsed_time`.

#### **3. Classificação de Tabelas (T1 e T2)**
- **Função**: `classify_tables(queries)`
  - Para cada tabela envolvida nas queries FTS, verifica seu tamanho usando `DBA_SEGMENTS`.
  - Exemplo de query SQL:
    ```sql
    SELECT segment_name, bytes / 1024 / 1024 AS size_mb
    FROM dba_segments
    WHERE segment_type = 'TABLE';
    ```
  - Classifica tabelas:
    - T1: Tabelas pequenas (ex.: < 10MB).
    - T2: Tabelas grandes (ex.: >= 10MB).

- **Saída**:
  - Dicionário com duas listas: `{"T1": [...], "T2": [...]}`.

#### **4. Análise de Performance**
- **Para T1**:
  - Avalia opções como criação de índices ou refatoração da query.
  - Gera sugestões de melhoria.

- **Para T2**:
  - Realiza análise minuciosa para estimar ganho de performance.
  - Calcula percentual de melhoria com base em métricas como `elapsed_time` e `buffer_gets`.

- **Função**: `evaluate_performance(tables)`
  - Retorna um dicionário com sugestões e impacto estimado.

#### **5. Priorização**
- **Função**: `prioritize_queries(queries)`
  - Atribui prioridade com base na frequência de execução e tamanho das tabelas.
  - Exemplo de lógica:
    ```python
    priority = 1 if size_mb < 10 and executions < 10 else 5 if size_mb < 100 else 10
    ```

#### **6. Geração de Scripts**
- **Função**: `generate_scripts(solutions)`
  - Para cada query, gera scripts SQL para criar índices, refatorar queries, etc.
  - Salva scripts no diretório `output/`.

#### **7. Atualização no GitHub**
- **Script Shell**: `github_updater.sh`
  - Usa a API do GitHub para enviar arquivos modificados.
  - Exemplo:
    ```bash
    #!/bin/bash
    git add .
    git commit -m "Atualização automática de scripts"
    git push https://$GITHUB_TOKEN@github.com/$GITHUB_REPO.git
    ```

#### **8. Controle de Execução**
- **Função**: `check_first_run()`
  - Verifica o arquivo `execucao.ini` para determinar se é a primeira execução.
  - Atualiza o arquivo após a execução inicial.

- **Função**: `schedule_execution()`
  - Programa a próxima execução para ocorrer a cada 6 horas.

---

### **Exemplo de Código**

#### **Identificação de Queries FTS**
```python
import cx_Oracle
import os
from dotenv import load_dotenv

load_dotenv()

def identify_fts_queries():
    user = os.getenv("ORACLE_USER")
    password = os.getenv("ORACLE_PASSWORD")
    dsn = os.getenv("ORACLE_DSN")

    connection = cx_Oracle.connect(user, password, dsn)
    cursor = connection.cursor()

    query = """
    SELECT sql_id, sql_text, executions, elapsed_time
    FROM v$sql
    WHERE sql_text LIKE '%/*+ FULL(%' OR sql_text LIKE '%TABLE ACCESS FULL%';
    """
    cursor.execute(query)
    results = cursor.fetchall()

    cursor.close()
    connection.close()

    return [{"sql_id": row[0], "sql_text": row[1], "executions": row[2], "elapsed_time": row[3]} for row in results]
```

#### **Classificação de Tabelas**
```python
def classify_tables(queries):
    user = os.getenv("ORACLE_USER")
    password = os.getenv("ORACLE_PASSWORD")
    dsn = os.getenv("ORACLE_DSN")

    connection = cx_Oracle.connect(user, password, dsn)
    cursor = connection.cursor()

    tables = set()
    for query in queries:
        # Extrair tabelas da query (lógica simplificada)
        tables.update(extract_tables_from_query(query["sql_text"]))

    table_sizes = {}
    for table in tables:
        cursor.execute(f"""
        SELECT bytes / 1024 / 1024 AS size_mb
        FROM dba_segments
        WHERE segment_name = '{table}' AND segment_type = 'TABLE';
        """)
        size = cursor.fetchone()[0]
        table_sizes[table] = size

    cursor.close()
    connection.close()

    t1 = [table for table, size in table_sizes.items() if size < 10]
    t2 = [table for table, size in table_sizes.items() if size >= 10]

    return {"T1": t1, "T2": t2}
```

INCLUIR 
projeto basico: monitor_oracle_fts
indicar requeriments.txt .gitgnore?
script fts.sh para a criação das estruturas do projeto
neste banco de dados, há cerca de 300 esquemas, onde muitos destes esquemas referencia-se a respectiva aplicacao.
importante registrar o nome do esquema
login no banco de dados deve ficar em função a parte (e dentro do .gitignore)
varios scripts/funcoes mencionadas nao foram disponibilizadas. Revisar
---

### **Conclusão**
Este projeto modular permite monitorar continuamente queries FTS, classificar tabelas, avaliar melhorias de performance e gerar scripts de solução automaticamente. A integração com o GitHub garante que os scripts estejam sempre atualizados. A periodicidade de execução é controlada por um cron job ou agendador externo.
