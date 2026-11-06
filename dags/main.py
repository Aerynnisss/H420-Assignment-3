from typing import cast

from airflow.sdk import dag, task

# from airflow.providers.standard.operators.python import PythonOperator
# from airflow.providers.standard.operators.bash import BashOperator
# from airflow.sdk.bases.operator import chain
from pymupdf4llm import to_markdown


@dag(schedule=None, description="test")
def my_dag():
  # Input: filename WITHOUT path OR suffix!
  # Example: convert("Assignment 3") -> this takes library/papers/Assignment 3.pdf
  # The library suggests installing RapidOCR, do so in the airflow venv.
  @task
  def convert(filename: str):
    markdown = cast(str, to_markdown(f"library/papers/{filename}.pdf"))  # Cast calms down the type checker

    with open(f"library/output/{filename}.md", "w", encoding="utf-8") as file:
      file.write(markdown)

  # convert >>

my_dag()
