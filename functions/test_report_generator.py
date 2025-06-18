# test_report_generator.py
import pytest
from unittest.mock import patch, MagicMock
from functions.report_generator import generate_html_report
from datetime import datetime
import os
from pathlib import Path

class TestReportGeneration:
    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        """Setup test environment with template"""
        self.template_dir = tmp_path / "templates"
        self.template_dir.mkdir()
        
        # Create test template
        template_content = """<html><body>
            <p>{{ timestamp }}</p>
            {% for cat, items in queries.items() %}
            <h2>{{ cat }}</h2>
            <ul>{% for item in items %}<li>{{ item }}</li>{% endfor %}</ul>
            {% endfor %}
            {% for cat, items in schemas.items() %}
            <h2>{{ cat }}</h2>
            <ul>{% for item in items %}<li>{{ item }}</li>{% endfor %}</ul>
            {% endfor %}
        </body></html>"""
        
        (self.template_dir / "test_template.html").write_text(template_content)
        
        self.sample_data = {
            "grouped_queries": {
                "slow": ["SELECT * FROM large_table"],
                "fast": ["SELECT id FROM users"]
            },
            "classified_schemas": {
                "T1": ["app_schema"],
                "T2": ["reporting_schema"]
            }
        }

    def test_generate_html_report_success(self, tmp_path):
        """Test successful report generation"""
        output_dir = tmp_path / "reports"
        report_path = generate_html_report(
            grouped_queries=self.sample_data["grouped_queries"],
            classified_schemas=self.sample_data["classified_schemas"],
            output_dir=str(output_dir),
            template_name="test_template.html",
            template_dir=str(self.template_dir))
        
        assert Path(report_path).exists()
        content = Path(report_path).read_text()
        assert "SELECT * FROM large_table" in content
        assert "app_schema" in content

    def test_generate_html_report_creates_directory(self, tmp_path):
        """Test that missing directories are created"""
        output_dir = tmp_path / "new" / "reports"
        report_path = generate_html_report(
            grouped_queries=self.sample_data["grouped_queries"],
            classified_schemas=self.sample_data["classified_schemas"],
            output_dir=str(output_dir),
            template_name="test_template.html",
            template_dir=str(self.template_dir))
        
        assert output_dir.exists()

    def test_generate_html_report_missing_template(self, tmp_path):
        """Test missing template handling"""
        with pytest.raises(RuntimeError, match="Template 'missing.html' not found"):
            generate_html_report(
                grouped_queries=self.sample_data["grouped_queries"],
                classified_schemas=self.sample_data["classified_schemas"],
                template_name="missing.html",
                template_dir=str(self.template_dir))

    def test_generate_html_report_invalid_data(self, tmp_path):
        """Test with invalid input data"""
        # Test invalid grouped_queries
        with pytest.raises(ValueError, match="Input data must be dictionaries"):
            generate_html_report(
                grouped_queries="invalid",
                classified_schemas={},
                template_dir=str(self.template_dir))

        # Test invalid classified_schemas
        with pytest.raises(ValueError, match="Input data must be dictionaries"):
            generate_html_report(
                grouped_queries={},
                classified_schemas="invalid",
                template_dir=str(self.template_dir))
        
    @patch('builtins.open')
    def test_generate_html_report_write_error(self, mock_open, tmp_path):
        """Test file write failure"""
        mock_open.side_effect = IOError("Write error")
        with pytest.raises(RuntimeError, match="Failed to generate report"):
            generate_html_report(
                grouped_queries=self.sample_data["grouped_queries"],
                classified_schemas=self.sample_data["classified_schemas"],
                template_dir=str(self.template_dir))