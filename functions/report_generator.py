from jinja2 import Environment, FileSystemLoader
import os
from datetime import datetime

def generate_html_report(grouped_queries, classified_schemas):
    template_dir = os.path.dirname(os.path.abspath(__file__))
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template("report_template.html")

    output = template.render(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M"),
        queries=grouped_queries,
        schemas=classified_schemas
    )

    with open("output/reports/fts_report.html", "w") as f:
        f.write(output)

    print("✅ Relatório HTML gerado com sucesso.")