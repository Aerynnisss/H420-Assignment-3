import json
import sqlite3
from typing import cast

DATABASE = "data/papers.db"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DATABASE)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def initialize_database() -> sqlite3.Connection:
    conn = get_connection()

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS papers (
            id        INTEGER PRIMARY KEY,
            filename  TEXT NOT NULL UNIQUE,
            title     TEXT NOT NULL,
            summary   TEXT NOT NULL,
            keywords  TEXT NOT NULL CHECK (
                CASE
                    WHEN json_valid(keywords)
                    THEN json_type(keywords) = 'array'
                    ELSE 0
                END
            ),
            category  TEXT NOT NULL
        );

        -- Separate table for keywords
        CREATE TABLE IF NOT EXISTS paper_keywords (
            paper_id      INTEGER NOT NULL,
            keyword       TEXT NOT NULL,

            PRIMARY KEY (paper_id, keyword),

            FOREIGN KEY (paper_id)
                REFERENCES papers(id)
                ON DELETE CASCADE
        ) WITHOUT ROWID;

        -- To improve performance when paper amounts start increasing,
        -- the following indexes are created
        CREATE INDEX IF NOT EXISTS idx_titles
        ON papers(title);

        CREATE INDEX IF NOT EXISTS idx_categories
        ON papers(category);

        CREATE INDEX IF NOT EXISTS idx_paper_keywords
        ON paper_keywords(keyword);
    """)

    conn.commit()
    return conn


def insert_paper(conn: sqlite3.Connection, paper: dict) -> int:
    keywords = {keyword.lower() for keyword in paper["keywords"]}

    keywords_json = json.dumps(sorted(keywords), ensure_ascii=False)

    cursor = conn.execute(
        """
        INSERT INTO papers (
            filename,
            title,
            summary,
            keywords,
            category
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            paper["filename"],
            paper["title"],
            paper["summary"],
            keywords_json,
            paper["category"],
        ),
    )

    paper_id = cast(int, cursor.lastrowid)

    conn.executemany(
        """
        INSERT INTO paper_keywords (paper_id, keyword)
        VALUES (?, ?)
        """,
        [(paper_id, keyword) for keyword in keywords],
    )

    return paper_id


# def prefix_get(conn: sqlite3.Connection, prefix: str):
#     conn.execute("SELECT * FROM papers WHERE title LIKE ?", (f"{prefix}",))
