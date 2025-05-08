# db_tools

This directory contains tools for managing and analyzing the arXiv papers PostgreSQL database.

## Contents

- `import_arxiv.py`: Import arXiv paper data from a JSON file into the PostgreSQL database.
- `database_overview.py`: Print a summary and statistics of the current database (tables, paper counts, top categories/authors, etc.).
- `README.md`: This file.

---

## 1. `import_arxiv.py`

**Purpose:**  
Bulk import arXiv paper metadata from a JSON file into the database, including papers, authors, categories, and their relationships.

**Usage:**
```bash
python import_arxiv.py path/to/arxiv_papers.json # save to default database
python import_arxiv.py path/to/arxiv_papers.json [--host HOST] [--port PORT] [--dbname DBNAME] [--user USER] [--password PASSWORD] [--batch-size N]
```

- The script expects a JSON file containing a list of paper objects.
- Database connection parameters can be set via environment variables or command-line arguments.
- Uses batch insertion for efficiency.
- Handles duplicate entries gracefully.

**Environment Variables (optional):**
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`

**Dependencies:**
- `psycopg2`
- `tqdm`
- `python-dotenv`

---

## 2. `database_overview.py`

**Purpose:**  
Display a summary of the arXiv papers database, including:

- Database size
- Table row counts and sizes
- Total number of papers and authors
- Paper counts by year and by top categories
- Top authors by paper count
- Recent papers

**Usage:**
```bash
python database_overview.py
```

- Reads database connection info from environment variables.
- Outputs formatted tables and statistics to the console.

**Environment Variables (optional):**
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`

**Dependencies:**
- `psycopg2`
- `prettytable`
- `python-dotenv`

---

## Setup

Install dependencies (in your virtual environment):

```bash
pip install psycopg2-binary tqdm prettytable python-dotenv
```

Set up your `.env` file in the project root (optional, for convenience):

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=arxiv_papers
DB_USER=arxiv_user
DB_PASSWORD=arxiv_password
```

---

## Notes

- Ensure your PostgreSQL database is set up with the required schema before importing data.
- Both scripts are designed to be run from the command line.
- For large imports, adjust the `--batch-size` parameter for optimal performance.

---

Feel free to modify this README as your tools evolve!
