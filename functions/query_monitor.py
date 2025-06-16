def identify_fts_queries(connection):
    cursor = connection.cursor()
    query = """
    SELECT sql_id, sql_text, executions, elapsed_time, parsing_schema_name
    FROM v$sql
    WHERE sql_text LIKE '%/*+ FULL(%' OR sql_text LIKE '%TABLE ACCESS FULL%';
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()

    return [
        {
            "sql_id": row[0],
            "sql_text": row[1],
            "executions": row[2],
            "elapsed_time": row[3],
            "schema": row[4]
        }
        for row in results
    ]