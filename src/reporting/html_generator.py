import os
from jinja2 import Environment, FileSystemLoader
from src.config import Config

class HTMLGenerator:
    """Generates the final HTML report from collected JSON plots."""

    def __init__(self):
        self.template_dir = os.path.join(os.path.dirname(__file__), 'templates')
        self.env = Environment(loader=FileSystemLoader(self.template_dir))

    def generate_report(self, consolidated_data: dict):
        """
        Renders the Jinja template with the consolidated plot data.
        """
        template = self.env.get_template('report_template.html')
        html_content = template.render(all_data=consolidated_data)

        output_path = os.path.join(Config.REPORT_DIR, 'ai_training_report.html')
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)

        print(f"[SUCCESS] Report generated at: {output_path}")