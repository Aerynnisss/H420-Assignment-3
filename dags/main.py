import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task, task_group
from helpers.db import generate_html, get_connection, initialize_database, insert_paper
from helpers.discord_notify import notify_discord
from helpers.llm_metadata import extract_metadata
from pymupdf4llm import to_markdown

# Folders shared with Google Drive (set PAPERS_DIR / OUTPUT_DIR to the synced folders).
# By default, the folders of this repository are used.
REPO_ROOT = Path(__file__).resolve().parent.parent
PAPERS_DIR = Path(os.getenv("PAPERS_DIR", REPO_ROOT / "library" / "papers"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", REPO_ROOT / "library" / "output"))

# A PDF modified less than this many seconds ago may still be syncing with Google Drive.
MIN_FILE_AGE_SECONDS = int(os.getenv("MIN_FILE_AGE_SECONDS", "60"))


@dag(
    dag_id="paper_processing",
    schedule="*/10 * * * *",  # look for new PDFs every 10 minutes
    start_date=datetime(2026, 11, 1),
    catchup=False,            # do not run for the past intervals since start_date
    max_active_runs=1,        # never two runs at the same time
)
def paper_processing():
    #########################
    ###       Tasks       ###
    #########################

    # Returns filenames not in database.
    # "library/papers/Assignment 3.pdf" -> "Assignment 3"
    @task
    def discover_pdfs():
        # Idempotent: creates the tables only if they do not exist yet.
        initialize_database().close()

        # Extract existing papers
        conn = get_connection()
        try:
            rows = conn.execute("SELECT filename FROM papers").fetchall()
            processed = {row[0] for row in rows}
            print(f"Found {len(processed)} papers in database.")
        except Exception as error:
            raise RuntimeError(f"Could not discover pdf files: {error}") from error
        finally:
            conn.close()

        # Select new papers to be processed
        now = time.time()
        result = []
        for path in sorted(PAPERS_DIR.glob("*.pdf")):
            if path.stem in processed:
                continue
            if now - path.stat().st_mtime < MIN_FILE_AGE_SECONDS:
                print(
                    f"{path.name} was modified recently (maybe still syncing), skipped for now."
                )
                continue
            result.append(path.stem)

        print(f"Found {len(result)} new papers in {PAPERS_DIR}.")
        return result

    # Input: filename WITHOUT path OR suffix!
    # Example: convert("Assignment 3") -> this takes PAPERS_DIR/Assignment 3.pdf
    # The library suggests installing RapidOCR, do so in the airflow venv.
    @task
    def convert(filename: str) -> tuple[str, str | list]:
        try:
            markdown = to_markdown(str(PAPERS_DIR / f"{filename}.pdf"))
        except Exception as error:
            raise RuntimeError(f"Failed to convert {filename}: {error}") from error

        print(f"Converted {filename}.")
        return filename, markdown

    # Only one LLM call at a time.
    # If the LLM answer cannot be parsed or validated, Airflow retries the task once.
    @task(max_active_tis_per_dagrun=1, retries=1, retry_delay=timedelta(seconds=30))
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
        return paper

    # Runs after all store tasks, even if they were skipped (no new PDF) or failed
    @task(trigger_rule="all_done")
    def regen_html():
        html = generate_html()
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_DIR / "index.html", "w", encoding="utf-8") as file:
            file.write(html)
        print(f"index.html written to {OUTPUT_DIR}.")

    # One Discord notification per new paper
    @task
    def notify(paper: dict):
        notify_discord(paper["filename"], paper)

    #########################
    ###       Setup       ###
    #########################

    # Enables depth first execution
    # Papers will store and notify as soon as llm_extract finishes
    # for the instance, instead of waiting for all llm_extracts to finish
    @task_group
    def process_paper(filename: str):
        converted = convert(filename)
        metadata = llm_extract(converted)
        stored = store(metadata)
        notify(paper=stored)
        return stored

    filenames = discover_pdfs()
    stored = process_paper.expand(filename=filenames)
    stored >> regen_html()


paper_processing()
