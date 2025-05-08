# arxiv_fetcher_db.py
import arxiv
import datetime
import json # Though not directly used for saving, kept for consistency if needed elsewhere
import time
import os
import sys
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from tqdm import tqdm
from dotenv import load_dotenv
import argparse # New import for argument parsing

# Load environment variables from .env file
load_dotenv()

# --- Database Configuration ---
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "172.20.0.2"),
    "port": int(os.environ.get("DB_PORT", 5432)),
    "dbname": os.environ.get("DB_NAME", "arxiv_papers"),
    "user": os.environ.get("DB_USER", "arxiv_user"),
    "password": os.environ.get("DB_PASSWORD", "arxiv_password")
}

# --- ArXiv Categories (same as before) ---
HIGH_VOLUME_CATEGORIES = ["cs.AI", "cs.CV", "cs.LG", "cs.CL"]
MEDIUM_VOLUME_CATEGORIES = ["cs.CR", "cs.NE", "cs.RO", "cs.IR", "cs.SE", "cs.SI", "cs.HC", "cs.DB"]
LOW_VOLUME_CATEGORIES = [
    "cs.AR", "cs.CC", "cs.CE", "cs.CG", "cs.CY", "cs.DC", "cs.DL", "cs.DM",
    "cs.DS", "cs.ET", "cs.FL", "cs.GL", "cs.GR", "cs.GT", "cs.IT", "cs.LO",
    "cs.MA", "cs.MM", "cs.MS", "cs.NA", "cs.NI", "cs.OH", "cs.OS",
    "cs.PF", "cs.PL", "cs.SC", "cs.SD", "cs.SY"
]
ALL_CS_CATEGORIES = HIGH_VOLUME_CATEGORIES + MEDIUM_VOLUME_CATEGORIES + LOW_VOLUME_CATEGORIES

# --- Database Helper Functions ---

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL database: {e}", file=sys.stderr)
        sys.exit(1)
def insert_paper_data(conn, paper_result):
    """
    Inserts a single paper's data into the database.
    Handles authors, categories, and the main paper entry.
    Returns True if successful, False otherwise.
    Includes conditional debugging prints for values exceeding 50 characters.
    """
    paper_id = paper_result.entry_id.split("/")[-1] # e.g., "2401.12345v1"
    
    # Conditional Debug Print for paper_id
    if len(paper_id) > 50:
        print(f"DEBUG_ALERT: Paper ID '{paper_id}' is TOO LONG for VARCHAR(50) (Length: {len(paper_id)})", file=sys.stderr)

    with conn.cursor() as cur:
        try:
            # 1. Insert/Get Primary Category
            primary_cat_code = paper_result.primary_category
            if primary_cat_code: # Ensure primary_category is not None
                # Conditional Debug Print for primary_cat_code
                if len(primary_cat_code) > 50:
                    print(f"DEBUG_ALERT: Paper {paper_id} - Primary Category '{primary_cat_code}' is TOO LONG for VARCHAR(50) (Length: {len(primary_cat_code)})", file=sys.stderr)
                
                cur.execute(
                    """
                    INSERT INTO categories_meta (category_code, description)
                    VALUES (%s, %s)
                    ON CONFLICT (category_code) DO NOTHING;
                    """,
                    (primary_cat_code, f"Category - {primary_cat_code.split('.')[-1] if '.' in primary_cat_code else primary_cat_code}")
                )

            # 2. Insert Paper
            cur.execute(
                """
                INSERT INTO papers (
                    id, title, abstract, primary_category_code, pdf_url,
                    arxiv_published_at, arxiv_updated_at,
                    journal_ref, doi
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    title = EXCLUDED.title,
                    abstract = EXCLUDED.abstract,
                    primary_category_code = EXCLUDED.primary_category_code,
                    pdf_url = EXCLUDED.pdf_url,
                    arxiv_published_at = EXCLUDED.arxiv_published_at,
                    arxiv_updated_at = EXCLUDED.arxiv_updated_at,
                    journal_ref = EXCLUDED.journal_ref,
                    doi = EXCLUDED.doi;
                """,
                (
                    paper_id,
                    paper_result.title,
                    paper_result.summary.replace("\n", " "),
                    primary_cat_code,
                    paper_result.pdf_url,
                    paper_result.published,
                    paper_result.updated,
                    paper_result.journal_ref,
                    paper_result.doi
                )
            )

            # 3. Insert/Get Authors and link to Paper
            author_ids_order = []
            for i, author_obj in enumerate(paper_result.authors):
                author_name = author_obj.name
                # Author names go into VARCHAR(512), so >50 is not an overflow for this specific field.
                # If you still want to check for very long author names for other reasons:
                # if len(author_name) > 50: # Or another threshold like 255
                #     print(f"DEBUG_WARN: Paper {paper_id} - Author {i} name is long: '{author_name}' (Length: {len(author_name)})", file=sys.stderr)
                cur.execute(
                    """
                    INSERT INTO authors (name) VALUES (%s)
                    ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                    RETURNING author_id;
                    """,
                    (author_name,)
                )
                author_id_tuple = cur.fetchone()
                if author_id_tuple:
                    author_ids_order.append({'paper_id': paper_id, 'author_id': author_id_tuple[0], 'author_order': i + 1})
                else:
                    cur.execute("SELECT author_id FROM authors WHERE name = %s;", (author_name,))
                    author_id_tuple = cur.fetchone()
                    if author_id_tuple:
                        author_ids_order.append({'paper_id': paper_id, 'author_id': author_id_tuple[0], 'author_order': i + 1})
                    else:
                        print(f"Warning: Could not get author_id for {author_name} on paper {paper_id}", file=sys.stderr)
                        continue

            if author_ids_order:
                execute_values(
                    cur,
                    """
                    INSERT INTO paper_authors (paper_id, author_id, author_order)
                    VALUES %s
                    ON CONFLICT (paper_id, author_id) DO UPDATE SET
                        author_order = EXCLUDED.author_order;
                    """,
                    [(d['paper_id'], d['author_id'], d['author_order']) for d in author_ids_order]
                )

            # 4. Insert all Categories and link to Paper
            all_category_codes = paper_result.categories
            if all_category_codes:
                cat_meta_data = []
                for i, cat_code in enumerate(all_category_codes):
                    if not cat_code: continue
                    # Conditional Debug Print for each cat_code
                    if len(cat_code) > 50:
                        print(f"DEBUG_ALERT: Paper {paper_id} - Associated Category {i} '{cat_code}' is TOO LONG for VARCHAR(50) (Length: {len(cat_code)})", file=sys.stderr)
                    
                    desc = f"Category - {cat_code.split('.')[-1] if '.' in cat_code else cat_code}"
                    cat_meta_data.append((cat_code, desc))

                if cat_meta_data:
                    execute_values(
                        cur,
                        """
                        INSERT INTO categories_meta (category_code, description)
                        VALUES %s
                        ON CONFLICT (category_code) DO NOTHING;
                        """,
                        cat_meta_data
                    )

                paper_cat_data = [(paper_id, cat_code) for cat_code in all_category_codes if cat_code]
                if paper_cat_data:
                    execute_values(
                        cur,
                        """
                        INSERT INTO paper_categories (paper_id, category_code)
                        VALUES %s
                        ON CONFLICT (paper_id, category_code) DO NOTHING;
                        """,
                        paper_cat_data
                    )
            conn.commit()
            return True
        except psycopg2.Error as e:
            conn.rollback()
            print(f"Database error for paper {paper_id}: {e}", file=sys.stderr)
            # The DEBUG_ALERT prints (if any) for this paper_id would have appeared before this error.
            return False
        except Exception as e:
            conn.rollback()
            print(f"General error processing paper {paper_id}: {e}", file=sys.stderr)
            return False# --- ArXiv Fetching Logic (Modified to use DB) ---

def fetch_papers_by_category_to_db(category, date_str, arxiv_client, db_conn, batch_size=100, max_total=None): # batch_size currently unused here
    """
    Fetches papers for a specific category and date, inserting them directly into the database.
    Returns the number of papers successfully inserted.
    """
    date_query_start = f"{date_str}000000"
    date_query_end = f"{date_str}235959"
    query = f"submittedDate:[{date_query_start} TO {date_query_end}] AND cat:{category}"

    inserted_count = 0
    failures = 0
    max_api_call_failures = 3

    while failures < max_api_call_failures:
        try:
            search_instance = arxiv.Search(
                query=query,
                max_results=max_total if max_total else float('inf'),
                sort_by=arxiv.SortCriterion.SubmittedDate
            )
            
            results_generator = arxiv_client.results(search_instance)
            
            # CORRECTED PART: Materialize the results into a list.
            results_list = list(results_generator) # This consumes the generator.
            
            num_papers_retrieved = len(results_list)

            print(f"API returned {num_papers_retrieved} papers for {category} on {date_str}. Processing...")

            if num_papers_retrieved == 0:
                print(f"No papers to process for {category} on {date_str}.")
                return 0 # No papers inserted for this category

            papers_processed_in_this_attempt = 0
            # CORRECTED PART: Iterate over results_list, not results_generator.
            # Added total=num_papers_retrieved for better tqdm output.
            for result in tqdm(results_list, desc=f"Processing {category}", unit="paper", total=num_papers_retrieved):
                # The 'if max_total and current_paper_index >= max_total:' check,
                # which was present in the original code at this location,
                # is not strictly necessary here because results_list is already limited
                # by max_results (derived from max_total) in arxiv.Search.
                # The current_paper_index variable from the original function was also removed
                # as its role is simplified by iterating directly over the appropriately sized results_list.

                if insert_paper_data(db_conn, result):
                    inserted_count += 1
                papers_processed_in_this_attempt += 1
            
            print(f"\nFinished fetching for {category}. Attempted to process {papers_processed_in_this_attempt} papers. Successfully inserted {inserted_count} papers.")
            return inserted_count # Success for this category call

        except arxiv.HTTPError as e:
            print(f"\nHTTPError fetching category {category}: {str(e)}", file=sys.stderr)
            failures += 1
            if failures < max_api_call_failures:
                print(f"Retrying in 15 seconds... ({failures}/{max_api_call_failures})", file=sys.stderr)
                time.sleep(15)
            else:
                print(f"Max retries reached for API call to category {category}.", file=sys.stderr)
                return inserted_count
        except Exception as e:
            print(f"\nUnexpected error fetching category {category}: {str(e)}", file=sys.stderr)
            return inserted_count
            
    print(f"Failed to fetch papers for {category} after {max_api_call_failures} attempts due to repeated errors.", file=sys.stderr)
    return inserted_count


def fetch_daily_papers_to_db(target_date_dt):
    """
    Fetches all CS papers for a given date (datetime object) and stores them in the database.
    Returns total number of papers inserted.
    """
    date_str_format = target_date_dt.strftime('%Y%m%d') # For query
    print(f"Fetching papers submitted on {target_date_dt.strftime('%Y-%m-%d')} (UTC) into the database...")

    arxiv_client = arxiv.Client(page_size=100, delay_seconds=5.0, num_retries=5) # Increased delay
    db_conn = get_db_connection()

    total_inserted_papers = 0

    category_processing_order = [
        (HIGH_VOLUME_CATEGORIES, "HIGH", 50, 500, 5),    # batch_size, max_total, sleep_time
        (MEDIUM_VOLUME_CATEGORIES, "MEDIUM", 100, 300, 3),
        (LOW_VOLUME_CATEGORIES, "LOW", 100, 150, 2)
    ]

    for categories, vol_type, batch_s, max_t, sleep_t in category_processing_order:
        if vol_type == "MEDIUM":
            groups = [categories[i:i+2] for i in range(0, len(categories), 2)]
        elif vol_type == "LOW":
            groups = [categories[i:i+5] for i in range(0, len(categories), 5)]
        else: # HIGH volume, process one by one
            groups = [[cat] for cat in categories]

        for category_group in groups:
            for category in category_group:
                print(f"\nFetching {vol_type} volume category: {category} ")
                count = fetch_papers_by_category_to_db(
                    category, date_str_format, arxiv_client, db_conn,
                    batch_size=batch_s, # This batch_size is passed but not used in fetch_papers_by_category_to_db
                    max_total=max_t
                )
                total_inserted_papers += count
                print(f"{category}: Inserted {count} papers in this call.")
            if len(category_group) > 0 :
                print(f"Pausing for {sleep_t}s after group: {', '.join(category_group)}...")
                time.sleep(sleep_t)

    print(f"\nTotal papers (considering unique insertions via ON CONFLICT) for {target_date_dt.strftime('%Y-%m-%d')}: {total_inserted_papers}")
    generate_db_statistics(db_conn, target_date_dt)

    if db_conn:
        db_conn.close()
    return total_inserted_papers


def generate_db_statistics(conn, target_date_dt):
    """Queries the database and prints statistics for the given date."""
    date_start_dt = datetime.datetime(target_date_dt.year, target_date_dt.month, target_date_dt.day, 0, 0, 0, tzinfo=datetime.timezone.utc)
    date_end_dt = datetime.datetime(target_date_dt.year, target_date_dt.month, target_date_dt.day, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc)
    
    print("\n--- Paper Statistics from Database ---")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT primary_category_code, COUNT(id)
            FROM papers
            WHERE arxiv_published_at >= %s AND arxiv_published_at <= %s
            GROUP BY primary_category_code
            ORDER BY COUNT(id) DESC;
            """,
            (date_start_dt, date_end_dt)
        )
        results = cur.fetchall()
        if results:
            print(f"\nPapers published on {target_date_dt.strftime('%Y-%m-%d')} by primary category:")
            total_published_today = 0
            for row in results:
                print(f"{row[0] if row[0] else 'N/A'}: {row[1]} papers")
                total_published_today += row[1]
            print(f"Total published on {target_date_dt.strftime('%Y-%m-%d')}: {total_published_today} papers")
        else:
            print(f"No papers found in DB with arxiv_published_at on {target_date_dt.strftime('%Y-%m-%d')}.")

        cur.execute("SELECT COUNT(*) FROM papers;")
        total_papers_in_db = cur.fetchone()[0]
        print(f"\nTotal papers currently in the database: {total_papers_in_db}")


def main():
    default_date_str = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d')

    parser = argparse.ArgumentParser(
        description="Fetch arXiv papers for a specific date and store them in a PostgreSQL database. "
                    "The script queries for papers *submitted* on the given UTC date.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog=f"""Example commands:
  %(prog)s                      # Fetch papers for today (UTC, currently {default_date_str})
  %(prog)s 20240115              # Fetch papers for January 15, 2024 (UTC)
  %(prog)s 20231231              # Fetch papers for December 31, 2023 (UTC)
"""
    )
    parser.add_argument(
        "date",
        nargs="?",
        default=default_date_str,
        help="Target date to fetch papers for, in YYYYMMDD format (e.g., 20240115). "
             "Interpreted as a UTC date. Defaults to the current UTC date."
    )
    args = parser.parse_args()
    date_input_str = args.date
    target_date_dt = None

    try:
        if not (len(date_input_str) == 8 and date_input_str.isdigit()):
            raise ValueError("Date format must be YYYYMMDD.")
        year = int(date_input_str[:4])
        month = int(date_input_str[4:6])
        day = int(date_input_str[6:])
        target_date_dt = datetime.datetime(year, month, day, tzinfo=datetime.timezone.utc)
        print(f"Using target date (UTC): {target_date_dt.strftime('%Y-%m-%d')}")
    except ValueError as e:
        print(f"Error: Invalid date input '{date_input_str}'. {e}", file=sys.stderr)
        parser.print_help()
        sys.exit(1)

    try:
        conn = get_db_connection()
        print("Successfully connected to the database.")
        conn.close()
    except Exception as e:
        print(f"Failed to connect to database. Please check configuration and DB status. Error: {e}", file=sys.stderr)
        sys.exit(1)

    fetch_daily_papers_to_db(target_date_dt)
    print("\nProcess completed.")

if __name__ == "__main__":
    print("Attempting to pre-populate known CS categories in categories_meta...")
    conn_init = None
    try:
        conn_init = get_db_connection()
        with conn_init.cursor() as cur:
            category_data = []
            for cat_code in ALL_CS_CATEGORIES:
                desc_suffix = cat_code.split('.')[-1] if '.' in cat_code else cat_code
                description = f"Computer Science - {desc_suffix}"
                category_data.append((cat_code, description))
            
            if category_data:
                execute_values(
                    cur,
                    "INSERT INTO categories_meta (category_code, description) VALUES %s ON CONFLICT (category_code) DO NOTHING;",
                    category_data
                )
                conn_init.commit()
                print(f"Pre-populated/verified {len(category_data)} CS categories.")
    except psycopg2.Error as e:
        print(f"Database error during pre-population: {e}", file=sys.stderr)
        if conn_init: conn_init.rollback()
    except Exception as e:
        print(f"General error during pre-population: {e}", file=sys.stderr)
    finally:
        if conn_init:
            conn_init.close()
    
    main()