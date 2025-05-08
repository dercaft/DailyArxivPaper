import os
import psycopg2
from prettytable import PrettyTable
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 从环境变量获取数据库配置
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", 5432)),
    "dbname": os.environ.get("DB_NAME", "arxiv_papers"),
    "user": os.environ.get("DB_USER", "arxiv_user"),
    "password": os.environ.get("DB_PASSWORD", "arxiv_password")
}

def get_db_summary():
    """获取数据库摘要信息"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    print("===== ArXiv Papers Database Summary =====\n")
    
    # 数据库大小
    cur.execute("SELECT pg_size_pretty(pg_database_size(%s)) as size", (DB_CONFIG["dbname"],))
    db_size = cur.fetchone()[0]
    print(f"Database Size: {db_size}\n")
    
    # 表统计 - 修复了ambiguous column错误
    print("Table Statistics:")
    table_stats = PrettyTable()
    table_stats.field_names = ["Table", "Rows", "Size"]
    
    # 修正查询，明确指定表名
    cur.execute("""
        SELECT 
            t.relname as table_name,
            t.n_live_tup as row_count,
            pg_size_pretty(pg_total_relation_size(t.relid)) as total_size
        FROM 
            pg_stat_user_tables t
        JOIN 
            pg_statio_user_tables s ON t.relid = s.relid
        ORDER BY 
            t.n_live_tup DESC
    """)
    
    for table, rows, size in cur.fetchall():
        table_stats.add_row([table, rows, size])
    
    print(table_stats)
    print()
    
    # 论文统计
    print("Papers Statistics:")
    cur.execute("SELECT COUNT(*) FROM papers")
    total_papers = cur.fetchone()[0]
    print(f"Total Papers: {total_papers}")
    
    # 发布年份分布
    cur.execute("""
        SELECT 
            EXTRACT(YEAR FROM arxiv_published_at) as year, 
            COUNT(*) as count
        FROM 
            papers 
        WHERE 
            arxiv_published_at IS NOT NULL
        GROUP BY 
            year 
        ORDER BY 
            year DESC
    """)
    
    years_table = PrettyTable()
    years_table.field_names = ["Year", "Papers Count"]
    
    for year, count in cur.fetchall():
        years_table.add_row([int(year), count])
    
    print("\nPapers by Year:")
    print(years_table)
    print()
    
    # 顶级分类
    cur.execute("""
        SELECT 
            pc.category_code, 
            COUNT(*) as paper_count
        FROM 
            paper_categories pc
        GROUP BY 
            pc.category_code
        ORDER BY 
            paper_count DESC
        LIMIT 10
    """)
    
    categories_table = PrettyTable()
    categories_table.field_names = ["Category", "Papers Count"]
    
    for category, count in cur.fetchall():
        categories_table.add_row([category, count])
    
    print("Top 10 Categories:")
    print(categories_table)
    print()
    
    # 作者统计
    cur.execute("SELECT COUNT(*) FROM authors")
    total_authors = cur.fetchone()[0]
    print(f"Total Authors: {total_authors}")
    
    # 顶级作者
    cur.execute("""
        SELECT 
            a.name, 
            COUNT(pa.paper_id) as paper_count
        FROM 
            authors a
        JOIN 
            paper_authors pa ON a.author_id = pa.author_id
        GROUP BY 
            a.name
        ORDER BY 
            paper_count DESC
        LIMIT 10
    """)
    
    authors_table = PrettyTable()
    authors_table.field_names = ["Author", "Papers Count"]
    
    for author, count in cur.fetchall():
        authors_table.add_row([author, count])
    
    print("\nTop 10 Authors by Paper Count:")
    print(authors_table)
    
    # 最近导入的论文
    cur.execute("""
        SELECT id, title, arxiv_published_at 
        FROM papers 
        ORDER BY arxiv_published_at DESC 
        LIMIT 5
    """)
    
    print("\nRecent Papers:")
    for paper_id, title, date in cur.fetchall():
        print(f"{paper_id} - {title} ({date})")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    get_db_summary()