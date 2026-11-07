import logging
from datetime import timedelta
from pathlib import Path
from typing import cast

from airflow.sdk import dag, task
from helpers.db import get_connection, initialize_database, insert_paper
from helpers.llm_metadata import extract_metadata
from pymupdf4llm import to_markdown


@dag(dag_id="init_database", schedule=None)
def init_database():
    # Call "airflow standalone" for this project's repository.
    @task
    def init():
        try:
            initialize_database()
        except Exception as error:
            raise RuntimeError(f"Database initialization failed: {error}") from error
        print("Database created.")

    init()


@dag(dag_id="paper_processing", schedule=None)
def paper_processing():
    # Returns filenames not in database.
    # "library/papers/Assignment 3.pdf" -> "Assignment 3"
    @task
    def discover_pdfs():
        dir = Path("library/papers/")
        conn = get_connection()

        try:
            rows = conn.execute("SELECT filename FROM papers").fetchall()
            processed = {row[0] for row in rows}
            print(f"Found {len(processed)} papers in database.")
        except Exception as error:
            raise RuntimeError(f"Could not discover pdf files: {error}") from error
        finally:
            conn.close()

        result = [path.stem for path in dir.glob("*.pdf") if path.stem not in processed]
        print(f"Found {len(result)} new papers in library/papers.")
        return result

    # Input: filename WITHOUT path OR suffix!
    # Example: convert("Assignment 3") -> this takes library/papers/Assignment 3.pdf
    # The library suggests installing RapidOCR, do so in the airflow venv.
    @task
    def convert(filename: str) -> tuple[str, str]:
        try:
            markdown = cast(
                str, to_markdown(f"library/papers/{filename}.pdf")
            )  # Cast calms down the type checker
        except Exception as error:
            raise RuntimeError(f"Failed to convert {filename}: {error}") from error

        print(f"Converted {filename}.")
        return filename, markdown

    # Goto Admin -> Pools. Add pool "llm_pool" and set slots to 1.
    @task(pool="llm_pool", retries=1, retry_delay=timedelta(seconds=30))
    def llm_extract(converted: tuple[str, str]):
        filename, paper = converted
        metadata = extract_metadata(filename, paper)

        print(f"LLM Result for {filename}.")
        logger = logging.getLogger("llm_extract")
        logger.info(
            "\nTitle: %s\nCategory: %s\nKeywords: %s\n",
            metadata["title"],
            metadata["category"],
            metadata["keywords"],
        )
        metadata["filename"] = filename
        return metadata

    # Run individually to ensure inserts don't fail globally
    @task
    def store(paper: dict):
        conn = get_connection()
        try:
            with conn:
                insert_paper(conn, paper)
        finally:
            conn.close()

    filenames = discover_pdfs()
    converted = convert.expand(filename=filenames)
    metadata = llm_extract.expand(converted=converted)
    store.expand(paper=metadata)


init_database()
paper_processing()
