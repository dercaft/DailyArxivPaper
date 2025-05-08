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
import argparse

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
                else: # Should not happen with RETURNING if insert/update was successful
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
            return False
        except Exception as e:
            conn.rollback()
            print(f"General error processing paper {paper_id}: {e}", file=sys.stderr)
            return False

# --- ArXiv Fetching Logic (Modified to use DB) ---

def fetch_papers_by_category_to_db(category, date_str, arxiv_client, db_conn, batch_size=100, max_total=None):
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
            results_list = list(results_generator) # Materialize generator
            num_papers_retrieved = len(results_list)

            print(f"API returned {num_papers_retrieved} papers for {category} on {date_str}. Processing...")

            if num_papers_retrieved == 0:
                print(f"No papers to process for {category} on {date_str}.")
                return 0 

            papers_processed_in_this_attempt = 0
            for result in tqdm(results_list, desc=f"Processing {category}", unit="paper", total=num_papers_retrieved, leave=False):
                if insert_paper_data(db_conn, result):
                    inserted_count += 1
                papers_processed_in_this_attempt += 1
            
            # No newline needed if tqdm(leave=False), but can add for clarity if run outside a loop
            # print(f"\nFinished fetching for {category}. Attempted to process {papers_processed_in_this_attempt} papers. Successfully inserted {inserted_count} papers.")
            return inserted_count 

        except arxiv.HTTPError as e:
            print(f"\nHTTPError fetching category {category}: {str(e)}", file=sys.stderr)
            failures += 1
            if failures < max_api_call_failures:
                print(f"Retrying in 15 seconds... ({failures}/{max_api_call_failures})", file=sys.stderr)
                time.sleep(15)
            else:
                print(f"Max retries reached for API call to category {category}.", file=sys.stderr)
                return inserted_count # Return count so far
        except Exception as e:
            print(f"\nUnexpected error fetching category {category}: {str(e)}", file=sys.stderr)
            return inserted_count # Return count so far
            
    print(f"Failed to fetch papers for {category} after {max_api_call_failures} attempts due to repeated errors.", file=sys.stderr)
    return inserted_count


def fetch_daily_papers_to_db(target_date_dt):
    """
    Fetches all CS papers for a given date (datetime object) and stores them in the database.
    Returns total number of papers inserted for that day.
    """
    date_str_format = target_date_dt.strftime('%Y%m%d') # For query
    print(f"Fetching papers submitted on {target_date_dt.strftime('%Y-%m-%d')} (UTC) into the database...")

    arxiv_client = arxiv.Client(page_size=100, delay_seconds=5.0, num_retries=5) 
    db_conn = get_db_connection() # Get a new connection for each day potentially

    total_inserted_papers_for_day = 0

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
                    batch_size=batch_s, 
                    max_total=max_t
                )
                total_inserted_papers_for_day += count
                print(f"{category}: Inserted {count} new papers in this call.")
            if len(category_group) > 0 : # Check to prevent printing if group was empty
                print(f"Pausing for {sleep_t}s after group: {', '.join(category_group)}...")
                time.sleep(sleep_t)

    print(f"\nTotal new papers (considering unique insertions via ON CONFLICT) for {target_date_dt.strftime('%Y-%m-%d')}: {total_inserted_papers_for_day}")
    generate_db_statistics(db_conn, target_date_dt)

    if db_conn:
        db_conn.close()
    return total_inserted_papers_for_day


def generate_db_statistics(conn, target_date_dt):
    """Queries the database and prints statistics for the given date."""
    # Ensure target_date_dt is timezone-aware (UTC) for comparison
    if target_date_dt.tzinfo is None:
        target_date_dt = target_date_dt.replace(tzinfo=datetime.timezone.utc)

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
    script_run_utc_datetime = datetime.datetime.now(datetime.timezone.utc)
    
    parser = argparse.ArgumentParser(
        description="Fetch arXiv papers for a specific date range and store them in a PostgreSQL database. "
                    "The script queries for papers *submitted* on each UTC date within the specified range.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog="""Example commands:
  %(prog)s 20240115                  # Fetch papers for January 15, 2024 (UTC) only.
  %(prog)s 20240110 --end_date 20240112 # Fetch papers from Jan 10, 2024 to Jan 12, 2024 (UTC).
  %(prog)s 20240120 --end_date today   # Fetch papers from Jan 20, 2024 (UTC) up to the current UTC date.
"""
    )
    parser.add_argument(
        "start_date",
        help="Target start date to fetch papers from, in YYYYMMDD format (e.g., 20240115). "
             "Interpreted as a UTC date."
    )
    parser.add_argument(
        "--end_date",
        nargs="?", # Makes it optional
        default=None, # Default to None, will be handled to mean start_date or today
        help="Optional target end date to fetch papers up to (inclusive), in YYYYMMDD format (e.g., 20240120). "
             "If not provided, defaults to the start_date (for a single day fetch). "
             "Use 'today' to fetch up to the current UTC date."
    )
    args = parser.parse_args()
    start_date_input_str = args.start_date
    end_date_input_str = args.end_date

    target_dates_dt_list = []

    try:
        def parse_date_str(date_str, date_description="date"):
            if not (isinstance(date_str, str) and len(date_str) == 8 and date_str.isdigit()):
                raise ValueError(f"{date_description} format must be YYYYMMDD (e.g., 20240115). Got: '{date_str}'")
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:])
            # Create datetime objects as UTC
            return datetime.datetime(year, month, day, tzinfo=datetime.timezone.utc)

        start_dt = parse_date_str(start_date_input_str, "Start date")

        if end_date_input_str is None:
            # If no end_date is provided, fetch only for the start_date
            end_dt = start_dt
            print(f"No end date provided. Fetching papers for a single day: {start_dt.strftime('%Y-%m-%d')} (UTC)")
        elif end_date_input_str.lower() == 'today':
            # If end_date is 'today', set it to the current UTC date
            end_dt = script_run_utc_datetime.replace(hour=0, minute=0, second=0, microsecond=0) # ensure it's start of day UTC
            print(f"End date set to 'today': {end_dt.strftime('%Y-%m-%d')} (UTC)")
        else:
            end_dt = parse_date_str(end_date_input_str, "End date")

        if start_dt > end_dt:
            print(f"Error: Start date ({start_dt.strftime('%Y-%m-%d')}) cannot be after end date ({end_dt.strftime('%Y-%m-%d')}).", file=sys.stderr)
            parser.print_help()
            sys.exit(1)

        # Generate list of dates to process
        current_dt = start_dt
        while current_dt <= end_dt:
            target_dates_dt_list.append(current_dt)
            current_dt += datetime.timedelta(days=1)

        if not target_dates_dt_list:
            print("Error: No dates to process based on the inputs.", file=sys.stderr)
            sys.exit(1)
        
        print(f"Planning to process papers for dates from {target_dates_dt_list[0].strftime('%Y-%m-%d')} to {target_dates_dt_list[-1].strftime('%Y-%m-%d')} (UTC).")

    except ValueError as e:
        print(f"Error: Invalid date input. {e}", file=sys.stderr)
        parser.print_help()
        sys.exit(1)
    except Exception as e: # Catch any other parsing related errors
        print(f"Error processing date arguments: {e}", file=sys.stderr)
        parser.print_help()
        sys.exit(1)

    # --- Database Connection Check ---
    try:
        conn_check = get_db_connection()
        print("Successfully connected to the database for initial check.")
        conn_check.close()
    except Exception as e:
        print(f"Failed to connect to database. Please check configuration and DB status. Error: {e}", file=sys.stderr)
        sys.exit(1)
    
    # --- Process each date ---
    total_inserted_across_all_days = 0
    for i, target_date_dt_loop_var in enumerate(target_dates_dt_list):
        print(f"\n===== Processing for date: {target_date_dt_loop_var.strftime('%Y-%m-%d')} ({i+1}/{len(target_dates_dt_list)}) =====")
        inserted_for_day = fetch_daily_papers_to_db(target_date_dt_loop_var)
        total_inserted_across_all_days += inserted_for_day
        print(f"===== Finished processing for {target_date_dt_loop_var.strftime('%Y-%m-%d')}. Inserted {inserted_for_day} new papers this day. =====")
        
        # Optional: Add a pause between processing different days, especially if the range is large
        if len(target_dates_dt_list) > 1 and i < len(target_dates_dt_list) - 1:
            inter_day_pause_seconds = 10 # Pause for 10 seconds
            print(f"Pausing for {inter_day_pause_seconds} seconds before processing the next day...")
            time.sleep(inter_day_pause_seconds)

    print(f"\n\n========= Overall Process Completed =========")
    if target_dates_dt_list:
        print(f"Processed dates from {target_dates_dt_list[0].strftime('%Y-%m-%d')} to {target_dates_dt_list[-1].strftime('%Y-%m-%d')}.")
    print(f"Total new papers inserted across all processed dates: {total_inserted_across_all_days}")
    print("===========================================")
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
                description = f"Computer Science - {desc_suffix}" # Generic description
                category_data.append((cat_code, description))
            
            if category_data:
                execute_values(
                    cur,
                    "INSERT INTO categories_meta (category_code, description) VALUES %s ON CONFLICT (category_code) DO NOTHING;",
                    category_data
                )
                conn_init.commit()
                print(f"Pre-populated/verified {len(category_data)} CS categories in categories_meta.")
    except psycopg2.Error as e:
        print(f"Database error during pre-population of categories_meta: {e}", file=sys.stderr)
        if conn_init: conn_init.rollback()
    except Exception as e: # Catch other potential errors like connection issues here
        print(f"General error during pre-population of categories_meta: {e}", file=sys.stderr)
    finally:
        if conn_init:
            conn_init.close()
    
    main()