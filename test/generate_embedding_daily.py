import requests
import numpy as np
import os
from typing import List, Optional
import psycopg2
from psycopg2.extras import execute_batch
from tqdm import tqdm
import argparse
import datetime

def get_embeddings(texts: List[str], model: str, 
                   endpoint: str = "embed", 
                   batch_size: int = 10) -> Optional[List[List[float]]]:
    """
    Get embeddings for a list of texts
    
    Args:
        texts: List of strings to embed
        model: Model name to use
        endpoint: API endpoint to use (embeddings or embed)
        batch_size: Number of texts to process in each batch
        
    Returns:
        List of embedding vectors or None if failed
    """
    # Get base URL from environment variable, with a default fallback
    base_url = os.getenv("OLLAMA_BASE_URL", "http://10.24.116.15:8998")
    
    if not texts:
        return []
    
    all_embeddings = []
    
    # Process in batches
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        
        try:
            url = f"{base_url}/api/{endpoint}"
            
            # Prepare payload based on endpoint
            if endpoint == "embeddings":
                # For single text input
                if len(batch) == 1:
                    payload = {
                        "model": model,
                        "prompt": batch[0],
                    }
                # For multiple texts
                else:
                    payload = {
                        "model": model,
                        "prompt": batch,
                    }
            elif endpoint == "embed":
                # For single text input
                if len(batch) == 1:
                    payload = {
                        "model": model,
                        "input": batch[0],
                    }
                # For multiple texts
                else:
                    payload = {
                        "model": model,
                        "input": batch,
                    }
            
            response = requests.post(url, json=payload, timeout=60)
            response.raise_for_status()
            result = response.json()
            
            # Handle the response format
            if "embeddings" in result:
                batch_embeddings = result["embeddings"]
                all_embeddings.extend(batch_embeddings)
            elif "embedding" in result:
                all_embeddings.append(result["embedding"])
            else:
                return None
            
        except Exception as e:
            print(f"Error generating embeddings: {e}")
            return None
    
    return all_embeddings if all_embeddings else None

def generate_and_update_embeddings_for_date(target_date_str, model="bge-m3", batch_size=10):
    # DB config (reuse from fetcher.py or set here)
    DB_CONFIG = {
        "host": os.environ.get("DB_HOST", "172.20.0.2"),
        "port": int(os.environ.get("DB_PORT", 5432)),
        "dbname": os.environ.get("DB_NAME", "arxiv_papers"),
        "user": os.environ.get("DB_USER", "arxiv_user"),
        "password": os.environ.get("DB_PASSWORD", "arxiv_password")
    }
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Parse date
    try:
        year = int(target_date_str[:4])
        month = int(target_date_str[4:6])
        day = int(target_date_str[6:])
        date_start = datetime.datetime(year, month, day, 0, 0, 0, tzinfo=datetime.timezone.utc)
        date_end = datetime.datetime(year, month, day, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc)
    except Exception as e:
        print(f"Invalid date format: {target_date_str}. Use YYYYMMDD. Error: {e}")
        return

    # Fetch papers for the date
    cur.execute(
        """
        SELECT id, title, abstract, summary_ai, detailed_review_ai
        FROM papers
        WHERE arxiv_published_at >= %s AND arxiv_published_at <= %s
        """,
        (date_start, date_end)
    )
    papers = cur.fetchall()
    print(f"Found {len(papers)} papers for {target_date_str}.")

    update_data = []
    skipped = 0
    for paper in tqdm(papers, desc="Embedding & updating", unit="paper"):
        paper_id, title, abstract, summary_ai, detailed_review_ai = paper
        # Only skip if title or abstract is missing
        if not title or not abstract:
            skipped += 1
            continue
        try:
            # Title+Abstract embedding
            title_abstract_text = f"{title.strip()}\n\n{abstract.strip()}"
            title_abstract_emb = get_embeddings([title_abstract_text], model=model, batch_size=1)
            if not title_abstract_emb:
                skipped += 1
                continue
            # Summary+Review embedding (if either present)
            summary_review_text = None
            if summary_ai and detailed_review_ai:
                summary_review_text = f"{summary_ai.strip()}\n\n{detailed_review_ai.strip()}"
            elif summary_ai:
                summary_review_text = summary_ai.strip()
            elif detailed_review_ai:
                summary_review_text = detailed_review_ai.strip()
            summary_review_emb = None
            if summary_review_text:
                summary_review_embs = get_embeddings([summary_review_text], model=model, batch_size=1)
                summary_review_emb = summary_review_embs[0] if summary_review_embs else None
            update_data.append((title_abstract_emb[0], summary_review_emb, paper_id))
        except Exception as e:
            print(f"Error embedding paper {paper_id}: {e}")
            skipped += 1
            continue

    # Update DB in batch
    if update_data:
        try:
            execute_batch(
                cur,
                """
                UPDATE papers SET
                    title_abstract_embedding = %s,
                    summary_review_embedding = %s
                WHERE id = %s
                """,
                update_data,
                page_size=20
            )
            conn.commit()
            print(f"Updated {len(update_data)} papers with embeddings.")
        except Exception as e:
            print(f"Error updating database: {e}")
            conn.rollback()
    else:
        print("No papers to update.")
    print(f"Skipped {skipped} papers due to missing fields or errors.")
    cur.close()
    conn.close()

def main():
    parser = argparse.ArgumentParser(description="Generate and update embeddings for arXiv papers for a given date.")
    parser.add_argument("date", help="Target date in YYYYMMDD format (e.g., 20240115)")
    args = parser.parse_args()
    generate_and_update_embeddings_for_date(args.date)

if __name__ == "__main__":
    main()

# Example usage:
# 1. First, set the environment variable (or in your .env file)
# os.environ["OLLAMA_BASE_URL"] = "http://your-ollama-server:8998"
# 
# 2. Then call the function
# texts = ["The sky is blue", "Water is transparent"]
# embeddings = get_embeddings(texts, model="bge-m3")
