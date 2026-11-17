"""Static smoke checks: imported notebook, cloud code syntax, and demo separation.

These checks do not validate IAM, cloud runtimes, or MWAA deployment.
"""
import ast
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_aws_notebook_valid_json_and_python():
    notebook = json.loads(
        (ROOT / "notebooks" / "aws_glue_banking_demo.ipynb").read_text()
    )
    assert notebook["nbformat"] == 4
    assert notebook["cells"]
    sources = []
    for index, cell in enumerate(notebook["cells"]):
        if cell["cell_type"] == "code":
            src = "".join(cell["source"])
            ast.parse(src, filename=f"cell_{index}")
            sources.append(src)
    all_code = "\n".join(sources)
    assert "Window.partitionBy" in all_code
    assert "row_number" in all_code
    assert "quarantine" in all_code
    assert "validation_report.json" in all_code


def test_aws_glue_and_airflow_python_syntax():
    paths = [
        "aws/glue_job_aws.py",
        "dags/mwaa_glue_banking_dag.py",
    ]
    for path in paths:
        source = (ROOT / path).read_text()
        ast.parse(source, filename=path)
