import json
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import argparse
import os
import sys
from tqdm import tqdm  # 进度条，需要pip install tqdm
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 从环境变量获取数据库配置
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "172.20.0.2"),
    "port": int(os.environ.get("DB_PORT", 5432)),
    "dbname": os.environ.get("DB_NAME", "arxiv_papers"),
    "user": os.environ.get("DB_USER", "arxiv_user"),
    "password": os.environ.get("DB_PASSWORD", "arxiv_password")
}

def parse_datetime(dt_str):
    """尝试将字符串解析为日期时间对象"""
    if not dt_str:
        return None
    try:
        # 处理可能的ISO格式或其他日期时间格式
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except Exception as e:
        try:
            # 尝试使用多种日期格式
            formats = [
                "%Y-%m-%dT%H:%M:%S.%fZ",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d"
            ]
            for fmt in formats:
                try:
                    return datetime.strptime(dt_str, fmt)
                except:
                    continue
            print(f"Warning: Could not parse datetime '{dt_str}': {e}")
            return None
        except:
            print(f"Warning: Could not parse datetime '{dt_str}': {e}")
            return None

def batch_insert(cur, table, columns, values, batch_size=1000):
    """批量插入数据，提高性能"""
    for i in range(0, len(values), batch_size):
        batch = values[i:i+batch_size]
        execute_values(
            cur, 
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING",
            batch
        )

def main():
    # 命令行参数解析
    parser = argparse.ArgumentParser(description='Import arXiv papers from JSON to PostgreSQL')
    parser.add_argument('json_file', help='Path to the JSON file containing arXiv papers')
    parser.add_argument('--host', help='Database host', default=DB_CONFIG['host'])
    parser.add_argument('--port', type=int, help='Database port', default=DB_CONFIG['port'])
    parser.add_argument('--dbname', help='Database name', default=DB_CONFIG['dbname'])
    parser.add_argument('--user', help='Database user', default=DB_CONFIG['user'])
    parser.add_argument('--password', help='Database password', default=DB_CONFIG['password'])
    parser.add_argument('--batch-size', type=int, help='Batch size for inserts', default=1000)
    
    args = parser.parse_args()
    
    # 更新数据库配置
    db_config = {
        "host": args.host,
        "port": args.port,
        "dbname": args.dbname,
        "user": args.user,
        "password": args.password
    }
    
    # 确认文件存在
    if not os.path.isfile(args.json_file):
        print(f"Error: File '{args.json_file}' not found.")
        sys.exit(1)
    
    # 加载JSON
    print(f"Loading JSON data from {args.json_file}...")
    try:
        with open(args.json_file, "r", encoding="utf-8") as f:
            papers = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in file '{args.json_file}': {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: Could not read file '{args.json_file}': {e}")
        sys.exit(1)
    
    if not isinstance(papers, list):
        print(f"Error: Expected a list of papers in JSON, but got {type(papers)}")
        sys.exit(1)
    
    print(f"Found {len(papers)} papers in JSON file.")
    
    # 连接数据库
    print(f"Connecting to database at {db_config['host']}:{db_config['port']}...")
    try:
        conn = psycopg2.connect(**db_config)
        conn.autocommit = False  # 使用事务
    except psycopg2.OperationalError as e:
        print(f"Error: Could not connect to database: {e}")
        sys.exit(1)
    
    cur = conn.cursor()
    
    try:
        # 缓存以避免重复插入
        print("Loading existing authors and categories...")
        author_name_to_id = {}
        category_code_set = set()
        
        # 预加载现有作者和分类
        cur.execute("SELECT author_id, name FROM authors")
        for author_id, name in cur.fetchall():
            author_name_to_id[name] = author_id
        
        cur.execute("SELECT category_code FROM categories_meta")
        for (code,) in cur.fetchall():
            category_code_set.add(code)
        
        # 开始导入
        print("Starting import process...")
        
        # 准备批量插入
        new_categories = []
        new_authors = []
        new_paper_authors = []
        new_paper_categories = []
        
        # 处理每篇论文
        for i, paper in enumerate(tqdm(papers, desc="Importing papers")):
            # 必需字段检查
            if "id" not in paper or not paper["id"]:
                print(f"Warning: Skipping paper at index {i} - missing ID")
                continue
                
            if "title" not in paper or not paper["title"]:
                print(f"Warning: Paper {paper.get('id', 'unknown')} has no title, setting to empty string")
                paper["title"] = ""
            
            # 插入categories_meta
            for cat in paper.get("categories", []):
                if cat and cat not in category_code_set:
                    new_categories.append((cat,))
                    category_code_set.add(cat)
            
            # 插入authors
            author_ids = []
            for author in paper.get("authors", []):
                if not author:  # 跳过空作者名
                    continue
                    
                if author not in author_name_to_id:
                    new_authors.append((author,))
                    
                    # 此时我们还不知道作者ID，将在后面获取
                    author_name_to_id[author] = None
                
                # 添加到作者ID列表（即使是None）
                author_ids.append(author)
            
            # 插入论文
            try:
                cur.execute("""
                    INSERT INTO papers (
                        id, title, abstract, primary_category_code, pdf_url,
                        arxiv_published_at, arxiv_updated_at, summary_ai, detailed_review_ai,
                        journal_ref, doi
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (id) DO NOTHING
                    RETURNING id
                """, (
                    paper["id"],
                    paper["title"],
                    paper.get("abstract"),
                    paper.get("primary_category_code"),
                    paper.get("pdf_url"),
                    parse_datetime(paper.get("arxiv_published_at")),
                    parse_datetime(paper.get("arxiv_updated_at")),
                    paper.get("summary_ai"),
                    paper.get("detailed_review_ai"),
                    paper.get("journal_ref"),
                    paper.get("doi"),
                ))
                
                paper_inserted = cur.fetchone() is not None
                
                # 只有当论文被插入时，才添加作者关系和分类关系
                if paper_inserted or True:  # 即使论文已存在，也更新作者和分类关系
                    # 存储论文-分类关系，稍后批量插入
                    for cat in paper.get("categories", []):
                        if cat:
                            new_paper_categories.append((paper["id"], cat))
                
                # 每处理100篇论文，执行一次批量插入并提交事务
                if (i + 1) % 100 == 0:
                    # 批量插入分类
                    if new_categories:
                        execute_values(
                            cur, 
                            "INSERT INTO categories_meta (category_code) VALUES %s ON CONFLICT DO NOTHING",
                            new_categories
                        )
                        new_categories = []
                    
                    # 批量插入作者
                    if new_authors:
                        execute_values(
                            cur, 
                            "INSERT INTO authors (name) VALUES %s ON CONFLICT (name) DO NOTHING",
                            new_authors
                        )
                        new_authors = []
                        
                        # 更新作者ID缓存
                        for author in author_name_to_id:
                            if author_name_to_id[author] is None:
                                cur.execute("SELECT author_id FROM authors WHERE name=%s", (author,))
                                result = cur.fetchone()
                                if result:
                                    author_name_to_id[author] = result[0]
                    
                    # 准备论文-作者关系数据
                    for paper_idx, paper in enumerate(papers[max(0, i-99):i+1]):
                        if "id" not in paper or not paper["id"]:
                            continue
                            
                        for order, author in enumerate(paper.get("authors", []), 1):
                            if not author or author not in author_name_to_id:
                                continue
                                
                            author_id = author_name_to_id[author]
                            if author_id is not None:
                                new_paper_authors.append((paper["id"], author_id, order))
                    
                    # 批量插入论文-作者关系
                    if new_paper_authors:
                        execute_values(
                            cur, 
                            "INSERT INTO paper_authors (paper_id, author_id, author_order) VALUES %s ON CONFLICT DO NOTHING",
                            new_paper_authors
                        )
                        new_paper_authors = []
                    
                    # 批量插入论文-分类关系
                    if new_paper_categories:
                        execute_values(
                            cur, 
                            "INSERT INTO paper_categories (paper_id, category_code) VALUES %s ON CONFLICT DO NOTHING",
                            new_paper_categories
                        )
                        new_paper_categories = []
                    
                    # 提交事务
                    conn.commit()
                    
            except psycopg2.Error as e:
                print(f"Error processing paper {paper.get('id', 'unknown')}: {e}")
                conn.rollback()  # 回滚事务
        
        # 处理剩余的批量插入
        if new_categories:
            execute_values(
                cur, 
                "INSERT INTO categories_meta (category_code) VALUES %s ON CONFLICT DO NOTHING",
                new_categories
            )
        
        if new_authors:
            execute_values(
                cur, 
                "INSERT INTO authors (name) VALUES %s ON CONFLICT (name) DO NOTHING",
                new_authors
            )
            
            # 更新作者ID缓存
            for author in author_name_to_id:
                if author_name_to_id[author] is None:
                    cur.execute("SELECT author_id FROM authors WHERE name=%s", (author,))
                    result = cur.fetchone()
                    if result:
                        author_name_to_id[author] = result[0]
        
        # 准备最后一批论文-作者关系
        for paper in papers:
            if "id" not in paper or not paper["id"]:
                continue
                
            for order, author in enumerate(paper.get("authors", []), 1):
                if not author or author not in author_name_to_id:
                    continue
                    
                author_id = author_name_to_id[author]
                if author_id is not None:
                    new_paper_authors.append((paper["id"], author_id, order))
        
        if new_paper_authors:
            execute_values(
                cur, 
                "INSERT INTO paper_authors (paper_id, author_id, author_order) VALUES %s ON CONFLICT DO NOTHING",
                new_paper_authors
            )
        
        if new_paper_categories:
            execute_values(
                cur, 
                "INSERT INTO paper_categories (paper_id, category_code) VALUES %s ON CONFLICT DO NOTHING",
                new_paper_categories
            )
        
        # 最终提交
        conn.commit()
        print("Import complete.")
        
    except Exception as e:
        conn.rollback()  # 回滚事务
        print(f"Error during import: {e}")
    finally:
        cur.close()
        conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()