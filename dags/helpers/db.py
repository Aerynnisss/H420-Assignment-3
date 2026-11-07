import json
import sqlite3
from html import escape
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


def generate_html():
    conn = get_connection()
    conn.row_factory = sqlite3.Row

    try:
        rows = conn.execute("""
            SELECT filename, title, summary, keywords, category
            FROM papers
            ORDER BY title
        """).fetchall()
    finally:
        conn.close()

    # Collect distinct categories for the dropdown.
    categories = sorted({row["category"] for row in rows}, key=str.casefold)

    category_options = "\n".join(
        f'<option value="{escape(category, quote=True)}">{escape(category)}</option>'
        for category in categories
    )

    cards = []

    for row in rows:
        keywords = json.loads(row["keywords"])

        # Include all relevant fields in the searchable text.
        search_text = " ".join(
            [
                row["filename"],
                row["title"],
                row["summary"],
                row["category"],
                *keywords,
            ]
        )

        keywords_html = ", ".join(escape(keyword) for keyword in keywords)

        cards.append(f"""
        <article class="paper"
                 data-category="{escape(row["category"], quote=True)}"
                 data-search="{escape(search_text, quote=True)}">

            <h2>{escape(row["title"])}</h2>

            <p class="meta">
                {escape(row["category"])}
                &middot;
                {escape(row["filename"])}
            </p>

            <p>{escape(row["summary"])}</p>

            <p class="keywords">
                <strong>Keywords:</strong> {keywords_html}
            </p>
        </article>
        """)

    cards_html = "\n".join(cards)

    if not cards:
        cards_html = "<p>No papers found in the database.</p>"

    return f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Paper Library</title>

    <style>
        body {{
            font-family: system-ui, sans-serif;
            max-width: 900px;
            margin: 40px auto;
            padding: 0 20px;
            line-height: 1.6;
            color: #222;
        }}

        h1 {{ margin-bottom: 25px; }}

        .filters {{
            display: flex;
            gap: 12px;
            margin-bottom: 25px;
        }}

        input, select {{
            padding: 10px;
            font: inherit;
            border: 1px solid #ccc;
            border-radius: 6px;
        }}

        input {{ flex: 1; min-width: 0; }}

        .paper {{
            border-bottom: 1px solid #ddd;
            padding: 18px 0;
        }}

        .paper h2 {{ margin: 0; }}

        .meta, .keywords {{
            color: #666;
            font-size: 0.9em;
        }}

        .paper[hidden] {{ display: none; }}
    </style>
</head>

<body>
    <h1>Paper Library</h1>

    <div class="filters">
        <input
            id="search"
            type="search"
            placeholder="Search papers..."
        >

        <select id="category">
            <option value="">All categories</option>
            {category_options}
        </select>
    </div>

    <p id="count"></p>

    <main id="papers">
        {cards_html}
    </main>

    <script>
        const searchInput = document.getElementById("search");
        const categorySelect = document.getElementById("category");
        const papers = [...document.querySelectorAll(".paper")];
        const count = document.getElementById("count");

        function filterPapers() {{
            const query = searchInput.value.trim().toLocaleLowerCase();
            const category = categorySelect.value;

            let visible = 0;

            for (const paper of papers) {{
                const matchesSearch =
                    paper.dataset.search.toLocaleLowerCase().includes(query);

                const matchesCategory =
                    !category || paper.dataset.category === category;

                paper.hidden = !(matchesSearch && matchesCategory);

                if (!paper.hidden) visible++;
            }}

            count.textContent = `${{visible}} of ${{papers.length}} papers`;
        }}

        searchInput.addEventListener("input", filterPapers);
        categorySelect.addEventListener("change", filterPapers);

        filterPapers();
    </script>
</body>
</html>
"""
