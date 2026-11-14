# H420 Assignment 3 – Research Paper Library with Airflow

This project turns research PDFs into a searchable study library.

Every 10 minutes, an Airflow DAG looks for new PDFs in a Google Drive-synced folder, converts them to Markdown,
asks a **local LLM** (Ollama) for a title, summary, keywords and category, stores the result in a local
**SQLite** database, sends a **Discord** notification and regenerates a static **HTML page**.

```
                 ┌──────────── process_paper [ ] ────────────┐
discover_pdfs ──→│ convert → llm_extract → store → notify    │──→ regen_html
                 └───────────────────────────────────────────┘
```
`[ ]` = dynamic task mapping: Airflow creates one `process_paper` group per new PDF.
Each paper goes through its 4 steps as soon as possible, without waiting for the other papers.

>  **A Discord webhook is required.** If `DISCORD_WEBHOOK_URL` is not set, the `notify` task fails
> (on purpose, so that a missing configuration is visible). See [3.6](#36-discord-webhook-required).

---

## 1. How it works

| Task | What it does |
|---|---|
| `discover_pdfs` | Creates the database tables if they do not exist yet. Lists the PDFs in `PAPERS_DIR` and keeps only the ones whose filename is not in the database. PDFs modified less than 60 seconds ago are skipped until the next run (they may still be syncing). |
| `process_paper.convert` | Converts one PDF to Markdown with PyMuPDF4LLM. |
| `process_paper.llm_extract` | Sends the first 12,000 characters of the paper to the local LLM in JSON mode, parses the answer with `json.loads()` and checks that `title`, `summary`, `keywords` and `category` exist with the right types. If it fails, Airflow retries the task once after 30 s (`retries=1`). Only one LLM call runs at a time (`max_active_tis_per_dagrun=1`). |
| `process_paper.store` | Saves one row per paper in SQLite (`filename` is unique, keywords are stored as a JSON array). |
| `process_paper.notify` | Sends one Discord message for the paper (title, summary, category, keywords). |
| `regen_html` | Runs after all `process_paper` groups (`trigger_rule="all_done"`, so also when there is no new paper or when one paper failed) and writes `index.html` with a search field and a category filter. |

A second DAG, `inspect_paper_database` (manual), prints the content of the database in the task logs.

**Where the data lives**

| What | Where |
|---|---|
| Input PDFs | `PAPERS_DIR` → Google Drive-synced folder `ResearchLibrary/papers/` |
| HTML page | `OUTPUT_DIR` → Google Drive-synced folder `ResearchLibrary/output/index.html` |
| Database | `data/papers.db` inside this repository, on the local machine (**not** in Google Drive, not on GitHub). Created automatically at the first run. |

## 2. Project structure

```
dags/
├── main.py              # DAG paper_processing (scheduled every 10 min)
├── inspect_db.py        # DAG inspect_paper_database (manual): prints the database in the logs
└── helpers/
    ├── db.py            # SQLite storage + HTML generation
    ├── llm_metadata.py  # call to the local LLM (Ollama, OpenAI-compatible API)
    └── discord_notify.py
library/
└── papers/
    └── Assignment 3.pdf # example paper to test the pipeline (see below)
.env.example             # list of the environment variables
requirements.txt
```

**About `library/` and the example PDF**

- `library/papers/` and `library/output/` are the **default folders**, used only when `PAPERS_DIR` / `OUTPUT_DIR`
  are not set (for example to test without Google Drive).
- `library/papers/Assignment 3.pdf` is an **example paper** included in the repository so that you can test the
  pipeline right after cloning:
  - **without Google Drive:** it is processed at the first run;
  - **with Google Drive:** copy it into your `ResearchLibrary/papers/` folder.
- The folder `library/` is in `.gitignore`: the PDFs you add and the generated `index.html` are **not** pushed
  to GitHub. Only the example PDF is tracked.

---

## 3. Installation (step by step)

The commands below are for **Linux** or **Windows with WSL (Ubuntu)**. Airflow does not run directly on Windows.

### 3.1 Windows only: install WSL

In PowerShell **as administrator**:
```powershell
wsl --install -d Ubuntu
```
Restart the computer, open "Ubuntu" from the Start menu and choose a Linux username and password.
**All the next commands are typed in the Ubuntu terminal.**

### 3.2 Get the code

```bash
git clone https://github.com/Aerynnisss/H420-Assignment-3.git
cd H420-Assignment-3
```
On Windows, you can also clone the repository in Windows and use it from WSL through `/mnt/c/...`.

### 3.3 Python environment and Airflow

Airflow 3.3.2 supports Python 3.10 to 3.14.
```bash
sudo apt update && sudo apt install -y python3-venv python3-pip
python3 -m venv ~/airflow/airflow-venv
source ~/airflow/airflow-venv/bin/activate

AIRFLOW_VERSION=3.3.2
PYTHON_VERSION="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install pymupdf4llm openai requests
```
Check: `airflow version` must print `3.3.2`.

Run `source ~/airflow/airflow-venv/bin/activate` again **in every new terminal**.

### 3.4 Local LLM with Ollama

```bash
sudo apt-get install -y zstd          # needed by the Ollama installer
curl -fsSL https://ollama.com/install.sh | sh
ollama pull llama3.2:3b
```
Check:
```bash
ollama run llama3.2:3b "Say hello in one sentence"
```
Ollama then runs in the background on `http://localhost:11434`. If `ollama list` says it cannot connect,
start it in a separate terminal with `ollama serve` and leave that terminal open.

`llama3.2:3b` needs about 8 GB of RAM. On a smaller computer, use `llama3.2:1b`
(`ollama pull llama3.2:1b` and `LLM_MODEL=llama3.2:1b`, see 3.7).

### 3.5 Google Drive folders

1. Install **Google Drive for desktop** (https://www.google.com/drive/download/) and sign in.
2. Click the Drive icon in the taskbar → ⚙️ **Preferences** → **Google Drive** → choose
   **"Mirror files"** (not "Stream files"). With "Mirror files", the files are real files in a normal folder,
   for example `C:\Users\<you>\My Drive` (`Mon Drive` in French), that WSL can read.
3. In this folder, create:
   ```
   My Drive/
   └── ResearchLibrary/
        ├── papers/     ← put the PDFs here
        └── output/     ← index.html is written here
   ```
4. Check that WSL sees the folder (replace `<you>` and `My Drive` by your own names):
   ```bash
   ls "/mnt/c/Users/<you>/My Drive/ResearchLibrary"
   ```
   You must see `papers` and `output`.

**Without Google Drive:** skip this step and do not set `PAPERS_DIR` / `OUTPUT_DIR`.
The pipeline then uses `library/papers/` and `library/output/` of the repository.

### 3.6 Discord webhook (required)

A webhook is a secret URL: everything sent to this URL is posted in a Discord channel.

1. Open Discord. If you do not have a server where you are administrator, create one:
   click **+** in the left column → **Create My Own** → **For me and my friends** → give it a name.
2. Right-click on a text channel (for example `#general`) → **Edit Channel**.
3. Go to **Integrations** → **Webhooks** → **New Webhook**.
4. Click on the new webhook, give it a name (for example `Paper Library`), then click **Copy Webhook URL**.
   The URL looks like `https://discord.com/api/webhooks/123456789/AbCdEf...`
5. Click **Save Changes**.


**This variable is required.** Without it, every `notify` task fails with
`ValueError: DISCORD_WEBHOOK_URL is not set`. The paper is still stored and the HTML page is still
generated, but the DAG run is marked as failed.

### 3.7 Environment variables

All the variables are listed in `.env.example`. Add them to `~/.bashrc` so they are loaded in every terminal.
Replace the values between `< >` with your own:

```bash
cat >> ~/.bashrc <<'VARS'
# --- Airflow ---
export AIRFLOW_HOME=~/airflow
export AIRFLOW__CORE__DAGS_FOLDER="<absolute path to>/H420-Assignment-3/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
# --- Google Drive folders (optional) ---
export PAPERS_DIR="/mnt/c/Users/<you>/My Drive/ResearchLibrary/papers"
export OUTPUT_DIR="/mnt/c/Users/<you>/My Drive/ResearchLibrary/output"
# --- Discord (required) ---
export DISCORD_WEBHOOK_URL="<your webhook URL>"
VARS
source ~/.bashrc
source ~/airflow/airflow-venv/bin/activate
```
To get the absolute path of the `dags` folder, run `pwd` inside the `dags` folder.

Optional variables (default values in brackets): `OLLAMA_BASE_URL` (`http://localhost:11434/v1`),
`LLM_MODEL` (`llama3.2:3b`), `MIN_FILE_AGE_SECONDS` (`60`).

Check that the variables are loaded:
```bash
echo $AIRFLOW__CORE__DAGS_FOLDER
echo $PAPERS_DIR
echo $DISCORD_WEBHOOK_URL
```

---

## 4. Run the pipeline

1. Make sure Ollama is running: `ollama list` must show `llama3.2:3b`.
2. Start Airflow (leave this terminal open):
   ```bash
   source ~/airflow/airflow-venv/bin/activate
   airflow standalone
   ```
  Airflow only sees the environment variables that exist **when it starts**. If you change a variable,
   stop Airflow (`Ctrl + C`), run `source ~/.bashrc`, activate the venv again and restart it.
3. Open http://localhost:8080 and log in with `admin` and the password printed in the terminal.
   You can also find it with:
   ```bash
   cat ~/airflow/simple_auth_manager_passwords.json.generated
   ```
4. In the **Dags** list, turn on the **`paper_processing`** DAG with its toggle. It now runs every 10 minutes.
   To run it immediately, click the ▶ (Trigger) button.
5. Copy one or more PDFs into the `papers` folder (for a first test: the example `library/papers/Assignment 3.pdf`)
   and wait at least 1 minute (new files are skipped for 60 seconds).

### Where to see the results

| Result | Where |
|---|---|
| Status of each task | Airflow → `paper_processing` → grid view (one column per run, one row per task) |
| What a task printed | Click on a **square** of the grid → **Logs** |
| Value returned by a task (e.g. the JSON of the LLM) | Square of `process_paper.llm_extract` → **Mapped Tasks** → index → **XCom** |
| HTML library | `ResearchLibrary/output/index.html` (or `library/output/index.html`): open it in a web browser |
| Discord | One message per new paper in the channel of the webhook |
| Database content | Trigger the `inspect_paper_database` DAG → square of `list_papers` → **Logs** |

## 5. Test the helper functions alone

From the root of the repository, with the venv activated:

```bash
# Discord (needs DISCORD_WEBHOOK_URL): sends a test message
python dags/helpers/discord_notify.py

# LLM (needs Ollama): convert the example PDF to Markdown, then extract the metadata
python -c "from pymupdf4llm import to_markdown; open('/tmp/example.md', 'w').write(to_markdown('library/papers/Assignment 3.pdf'))"
python dags/helpers/llm_metadata.py /tmp/example.md
```

