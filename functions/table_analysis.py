def classify_tables(queries, connection):
    cursor = connection.cursor()
    tables = set()

    for query in queries:
        # Extrair tabelas da query (lógica simplificada)
        tables.update(extract_tables_from_query(query["sql_text"]))

    t1, t2 = [], []
    for table in tables:
        cursor.execute(f"""
        SELECT bytes / 1024 / 1024 AS size_mb
        FROM dba_segments
        WHERE segment_name = '{table}' AND segment_type = 'TABLE';
        """)
        size = cursor.fetchone()[0]
        if size < 10:
            t1.append(table)
        else:
            t2.append(table)

    cursor.close()
