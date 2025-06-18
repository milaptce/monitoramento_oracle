# report_generation.py

from jinja2 import Environment, FileSystemLoader, TemplateNotFound
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

def generate_html_report(
    grouped_queries: Dict,
    classified_schemas: Dict,
    output_dir: str = "output/reports",
    template_name: str = "report_template.html",
    template_dir: Optional[str] = None
) -> str:
    """
    Generate HTML report from query and schema data

    Args:
        grouped_queries: Dictionary of categorized queries
        classified_schemas: Dictionary of schema classifications
        output_dir: Output directory path
        template_name: Name of the template file
        template_dir: Optional custom template directory path

    Returns:
        Path to the generated report

    Raises:
        ValueError: If input data is invalid
        RuntimeError: If report generation fails for other reasons
    """
    # Validate input data first (before any side effects)
    if not isinstance(grouped_queries, dict) or not isinstance(classified_schemas, dict):
        raise ValueError("Input data must be dictionaries")

    try:
        # Set up template environment
        if template_dir is None:
            template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
        
        env = Environment(loader=FileSystemLoader(template_dir))
        
        try:
            template = env.get_template(template_name)
        except TemplateNotFound:
            available = "\n".join(f" - {f}" for f in Path(template_dir).glob("*.html"))
            raise RuntimeError(
                f"Template '{template_name}' not found in {template_dir}\n"
                f"Available templates:\n{available}"
            )

        # Create output directory if it doesn't exist
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Generate output path with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = os.path.join(output_dir, f"fts_report_{timestamp}.html")
        
        # Render and save report
        output = template.render(
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M"),
            queries=grouped_queries,
            schemas=classified_schemas
        )
        
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(output)
            
        return report_path
        
    except Exception as e:
        raise RuntimeError(f"Failed to generate report: {str(e)}")