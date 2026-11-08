"""Ask a locally served LLM (Ollama) for the metadata of a research paper.

The LLM is called through Ollama's OpenAI-compatible API, so we can use the
official `openai` Python package with a local base URL.
"""

import json
import os
from typing import cast

from openai import OpenAI

# --- Configuration (can be changed with environment variables) -------------
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1")
LLM_MODEL = os.getenv("LLM_MODEL", "llama3.2:3b")
MAX_CHARS = 12000  # we only send the beginning of the paper (title, abstract, intro)

CATEGORIES = [
    "Data Engineering",
    "Machine Learning",
    "Artificial Intelligence",
    "Natural Language Processing",
    "Computer Vision",
    "Cybersecurity",
    "Databases",
    "Business Process Management",
    "Other",
]

# Expected fields and their basic Python type
REQUIRED_FIELDS = {"title": str, "summary": str, "keywords": list, "category": str}

SYSTEM_PROMPT = f"""You are an assistant that catalogues research papers.
Read the paper and answer ONLY with a JSON object with exactly these keys:
- "title": the title of the paper (string)
- "summary": a short summary of the paper in 3 to 5 sentences (string)
- "keywords": 3 to 6 keywords (list of strings)
- "category": one of {CATEGORIES} (string)
Do not add any text before or after the JSON."""


def _ask_llm(markdown: str) -> str:
    """Send the paper to the local LLM and return its raw text answer."""
    client = OpenAI(base_url=OLLAMA_BASE_URL, api_key="ollama")  # key is ignored by Ollama
    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": markdown[:MAX_CHARS]},
        ],
        response_format={"type": "json_object"},  # JSON mode
        temperature=0.2,  # low temperature = more stable answers
    )
    return cast(str, response.choices[0].message.content)


def _validate(data) -> dict:
    """Check that the required fields exist and have the expected basic type."""
    if not isinstance(data, dict):
        raise TypeError("the answer is not a JSON object")
    for field, expected_type in REQUIRED_FIELDS.items():
        if field not in data:
            raise ValueError(f"missing field '{field}'")
        if not isinstance(data[field], expected_type):
            raise TypeError(f"field '{field}' should be of type {expected_type.__name__}")
    if not all(isinstance(k, str) for k in data["keywords"]):
        raise ValueError("all keywords should be strings")
    # keep only the expected fields
    return {field: data[field] for field in REQUIRED_FIELDS}


def extract_metadata(filename: str, markdown: str) -> dict:
    """Return {"title", "summary", "keywords", "category"} for a paper in Markdown.
    """
    raw_answer = _ask_llm(markdown)
    try:
        data = json.loads(raw_answer)
        return _validate(data)
    except (json.JSONDecodeError, ValueError, TypeError) as error:
        print(f"Metadata extraction failed for {filename}: {error}")
        raise


if __name__ == "__main__":
    # Quick manual test: python dags/llm_metadata.py "library/output/Assignment 3.md"
    import sys

    with open(sys.argv[1], encoding="utf-8") as file:
        paper = file.read()
    print(json.dumps(extract_metadata(sys.argv[1], paper), indent=2, ensure_ascii=False))
