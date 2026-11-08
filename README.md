# H420 Assignment 3 – Research Paper Library with Airflow

This project is a small local pipeline that turns research PDFs into a searchable study library.
A scheduled Airflow DAG looks for new PDFs, converts them to Markdown, asks a **local LLM** (Ollama)
for a title, summary, keywords and category, stores the result in **SQLite**, regenerates a static
**HTML page** and sends a **Discord** notification.

```
discover_pdfs → convert[ ] → llm_extract[ ] → store[ ] ─┬→ regen_html
                                                         └→ notify[ ]
```
`[ ]` = dynamic task mapping: one task instance per new PDF.

## Project structure

```
dags/
├── main.py              # DAGs: paper_processing (scheduled) and init_database (optional)
├── inspect_db.py        # DAG to print the content of the database in the logs
└── helpers/
    ├── db.py            # SQLite storage + HTML generation
    ├── llm_metadata.py  # call to the local LLM (Ollama, OpenAI-compatible API)
    └── discord_notify.py
library/                 # default papers/ and output/ folders (used if Google Drive is not configured)
data/papers.db           # local SQLite database (created automatically, not on GitHub)
.env.example             # environment variables to configure
```

## How it works

- **Schedule:** `paper_processing` runs every 10 minutes (`*/10 * * * *`, `catchup=False`, one run at a time).
- **New PDFs:** `discover_pdfs` compares the PDFs in `PAPERS_DIR` with the filenames already in the
  database. PDFs modified less than 60 seconds ago are skipped until the next run (they may still be syncing).
- **LLM:** `llm_extract` sends the beginning of the paper to the local model in JSON mode, parses the
  answer with `json.loads()` and checks the fields and their types. If this fails, Airflow retries the task once
  (`retries=1`). Only one LLM call runs at a time (`max_active_tis_per_dagrun=1`).
- **Storage:** one row per paper in SQLite (`filename` is unique, keywords are stored as a JSON string).
- **HTML:** `regen_html` runs after all `store` tasks (`trigger_rule="all_done"`) and writes `index.html`
  with a search field and a category filter.
- **Discord:** one notification per new paper.
- **Google Drive:** `PAPERS_DIR` and `OUTPUT_DIR` point to the Drive-synced folders. The database stays
  on the local machine, outside the synced folder.

## Installation (Windows + WSL Ubuntu)

1. **Python environment and Airflow**
   ```bash
   python3 -m venv ~/airflow-venv
   source ~/airflow-venv/bin/activate
   AIRFLOW_VERSION=3.3.2
   PYTHON_VERSION="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
   pip install "apache-airflow==${AIRFLOW_VERSION}" \
     --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
   pip install pymupdf4llm openai requests
   ```

2. **Local LLM with Ollama**
   ```bash
   sudo apt-get install -y zstd
   curl -fsSL https://ollama.com/install.sh | sh
   ollama pull llama3.2:3b
   ```

3. **Google Drive** – install Google Drive for desktop on Windows in **"Mirror files"** mode and create:
   ```
   My Drive/ResearchLibrary/papers/
   My Drive/ResearchLibrary/output/
   ```

4. **Discord** – in a Discord channel: *Edit channel → Integrations → Webhooks → New webhook → Copy URL*.

5. **Environment variables** (see `.env.example`) – add them to `~/.bashrc`, then run `source ~/.bashrc`:
   ```bash
   export AIRFLOW_HOME=~/airflow
   export AIRFLOW__CORE__DAGS_FOLDER="/path/to/H420-Assignment-3/dags"
   export AIRFLOW__CORE__LOAD_EXAMPLES=False
   export PAPERS_DIR="/mnt/c/Users/<you>/My Drive/ResearchLibrary/papers"
   export OUTPUT_DIR="/mnt/c/Users/<you>/My Drive/ResearchLibrary/output"
   export DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/..."
   ```
   Without `PAPERS_DIR` / `OUTPUT_DIR`, the folders `library/papers` and `library/output` of the repository
   are used. Without `DISCORD_WEBHOOK_URL`, the pipeline works but no notification is sent.

## Run

```bash
source ~/airflow-venv/bin/activate
airflow standalone
```
1. Open http://localhost:8080 (the `admin` password is printed in the terminal).
2. Turn on the `paper_processing` DAG (it then runs every 10 minutes), or start it with the ▶ button.
3. Add a PDF to the `papers/` folder and wait for the next run.
4. Results: `index.html` in the output folder, a message in Discord, and the `inspect_paper_database`
   DAG to see the content of the database in the task logs.

Make sure Ollama is running (`ollama list`) and restart `airflow standalone` after changing environment variables.

## Test the helper functions alone

```bash
python dags/helpers/llm_metadata.py "library/output/Assignment 3.md"
python dags/helpers/discord_notify.py
```
