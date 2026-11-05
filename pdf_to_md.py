import pymupdf4llm


# Input: filename WITHOUT path OR suffix!
# Example: convert("Assignment 3") -> this takes library/papers/Assignment 3.pdf
# The library suggests installing RapidOCR, do so in the airflow venv.
def convert(filename: str):
  markdown = pymupdf4llm.to_markdown(f"library/papers/{filename}.pdf")

  with open(f"library/output/{filename}.md", "w", encoding="utf-8") as file:
    file.write(markdown)

convert("Assignment 3")
