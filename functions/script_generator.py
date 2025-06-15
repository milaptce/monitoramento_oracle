def generate_scripts(solutions):
    for table, details in solutions.items():
        with open(f"output/generated_scripts/{table}_script.sql", "w") as f:
            if details["action"] == "create_index":
                f.write(f"CREATE INDEX idx_{table} ON {table}(column);\n")
            elif details["action"] == "refactor_query":
