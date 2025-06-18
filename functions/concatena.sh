#!/bin/bash

# Lista dos pares de arquivos (codigo + teste)
declare -a ARQUIVOS=(
    "query_monitor.py test_query_monitor.py"
    "table_analysis.py test_table_analysis.py"
    "utils.py test_utils.py"
    "db_utils.py test_db_utils.py"
    "report_generator.py test_report_generator.py"
    "script_generator.py test_script_generator.py"
    "performance_improvement.py test_performance_improvement.py"
)

# Nome do arquivo de saída
OUTPUT="codigo_completo.txt"

# Limpa ou cria o arquivo de saída
> "$OUTPUT"

# Percorre cada par e concatena conforme a estrutura desejada
for PAR in "${ARQUIVOS[@]}"; do
    CODIGO=$(echo $PAR | awk '{print $1}')
    TESTE=$(echo $PAR | awk '{print $2}')

    echo "# Arquivo: $CODIGO" >> "$OUTPUT"
    cat "$CODIGO" >> "$OUTPUT"
    echo -e "\n# Teste: $TESTE\n" >> "$OUTPUT"
    cat "$TESTE" >> "$OUTPUT"
    echo -e "\n\n" >> "$OUTPUT"
done

echo "Concatenação concluída! Arquivo gerado: $OUTPUT"
