import logging
import sqlite3

from airflow.sdk import dag, task
from helpers.db import DATABASE


def get_connection():
    conn = sqlite3.connect(f"{DATABASE.as_uri()}?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


@dag(
    dag_id="inspect_paper_database",
    schedule=None,
    tags=["database", "inspection"],
)
def inspect_paper_database():

    @task
    def database_overview():
        logger = logging.getLogger("database_overview")
        conn = get_connection()

        try:
            paper_count = conn.execute("SELECT COUNT(*) FROM papers").fetchone()[0]

            category_counts = conn.execute("""
                SELECT category, COUNT(*) AS count
                FROM papers
                GROUP BY category
                ORDER BY count DESC
            """).fetchall()

            keyword_counts = conn.execute("""
                SELECT
                    j.value AS keyword,
                    COUNT(*) AS count
                FROM papers AS p,
                     json_each(p.keywords) AS j
                GROUP BY j.value
                ORDER BY count DESC
                LIMIT 20
            """).fetchall()

            logger.info("Total papers: %d", paper_count)

            logger.info("Papers by category:")
            for row in category_counts:
                logger.info("%s: %d", row["category"], row["count"])

            logger.info("Most frequent keywords:")
            for row in keyword_counts:
                logger.info("%s: %d", row["keyword"], row["count"])

        finally:
            conn.close()

    @task
    def list_papers():
        logger = logging.getLogger("list_papers")
        conn = get_connection()

        try:
            rows = conn.execute("""
                SELECT
                    p.id,
                    p.filename,
                    p.title,
                    p.category,
                    (
                        SELECT GROUP_CONCAT(j.value, ', ')
                        FROM json_each(p.keywords) AS j
                    ) AS keywords
                FROM papers AS p
                ORDER BY p.id
                LIMIT 50
            """).fetchall()

            logger.info("Showing up to 50 papers:")

            for row in rows:
                logger.info(
                    "\nID: %s\nFilename: %s\nTitle: %s\nCategory: %s\nKeywords: %s\n",
                    row["id"],
                    row["filename"],
                    row["title"],
                    row["category"],
                    row["keywords"],
                )

        finally:
            conn.close()

    database_overview()
    list_papers()


inspect_paper_database()
