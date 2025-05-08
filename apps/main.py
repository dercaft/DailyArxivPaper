import os
import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor
# from sentence_transformers import SentenceTransformer # util is not used directly
# import numpy as np # Not explicitly used
from dotenv import load_dotenv
from streamlit_searchbox import st_searchbox # 导入 streamlit-searchbox
import requests

# 加载环境变量
load_dotenv()

# 从环境变量中获取数据库连接信息
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "bge-m3")
EMBEDDING_API_BASE = os.getenv("OLLAMA_BASE_URL", "http://10.24.116.15:8998")
EMBEDDING_API_ENDPOINT = os.getenv("EMBEDDING_API_ENDPOINT", "embed")

# 初始化PostgreSQL连接 (使用Streamlit缓存)
@st.cache_resource
def init_postgres():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    return conn

# # 加载语义搜索模型 (使用Streamlit缓存)
# @st.cache_resource
# def load_model():
#     # 警告：'all-MiniLM-L6-v2' 生成384维向量。
#     # 如果您的数据库 'summary_ai_embedding' 列存储的是1536维向量，这将不匹配。
#     # 请确保模型与数据库中的向量维度一致。
#     return SentenceTransformer('all-MiniLM-L6-v2')

# 通用SQL查询函数
def fetch_papers_general(sql_query, params=None):
    conn = init_postgres()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql_query, params)
        papers = cur.fetchall()
    return papers

def safe_format_tsquery_input(query_text: str, prefix_match: bool = True) -> str:
    """
    安全地将用户输入文本转换为PostgreSQL to_tsquery函数可以理解的字符串。
    处理特殊字符，并将单词用 '&' (AND) 连接。
    支持对每个单词进行前缀匹配 (e.g., 'exampl:*')。
    """
    words = query_text.strip().split()
    if not words:
        return ''
    
    processed_words = []
    for word in words:
        # 移除或转义可能干扰tsquery的字符。这里简化处理，只保留字母和数字。
        # 更复杂的场景可能需要更完善的清理逻辑。
        safe_word = ''.join(char for char in word if char.isalnum())
        if safe_word:
            if prefix_match:
                processed_words.append(safe_word + ":*")
            else:
                processed_words.append(safe_word)
    
    if not processed_words:
        return ''
        
    # 用 AND (&) 连接所有处理过的词语
    return " & ".join(processed_words)

# 为搜索框获取建议的函数
def fetch_search_suggestions(searchterm: str) -> list[str]:
    """
    当用户在streamlit-searchbox中输入时，此函数被调用以获取建议。
    """
    if not searchterm or len(searchterm) < 3: # 至少输入2个字符才开始建议
        return []
    conn = init_postgres()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 基于标题进行建议，使用全文搜索的前缀匹配获取更相关的建议
            # ts_query_str = searchterm.strip() + ":*" # 简单前缀匹配
            ts_query_str = safe_format_tsquery_input(searchterm, prefix_match=True)
            if not ts_query_str:
                return []

            # 我们主要根据标题或者AI总结来提供建议
            sql = """
                SELECT title
                FROM papers
                WHERE fts_document @@ to_tsquery('english', %s)
                ORDER BY ts_rank(fts_document, to_tsquery('english', %s)) DESC
                LIMIT 7; 
            """
            # 如果只想在标题中搜索建议：
            # WHERE to_tsvector('english', title) @@ to_tsquery('english', %s)
            # ORDER BY ts_rank(to_tsvector('english', title), to_tsquery('english', %s)) DESC
            
            cur.execute(sql, (ts_query_str, ts_query_str))
            suggestions = cur.fetchall()
            return [s['title'] for s in suggestions]
    except Exception as e:
        print(f"获取搜索建议时出错: {e}") # 在后台打印错误
        return []

def get_embedding_via_api(text: str, model: str = EMBEDDING_MODEL, endpoint: str = EMBEDDING_API_ENDPOINT) -> list[float]:
    url = f"{EMBEDDING_API_BASE}/api/{endpoint}"
    payload = {"model": model, "input": text}
    try:
        resp = requests.post(url, json=payload, timeout=60)
        resp.raise_for_status()
        result = resp.json()
        if "embedding" in result:
            return result["embedding"]
        elif "embeddings" in result and isinstance(result["embeddings"], list):
            return result["embeddings"][0]
        else:
            st.error("Embedding API 返回格式异常")
            return None
    except Exception as e:
        st.error(f"获取 embedding 失败: {e}")
        return None

# 使用pgvector进行语义搜索
def semantic_search_db(query_text: str, top_k=10):
    conn = init_postgres()
    query_embedding = get_embedding_via_api(query_text)
    if not query_embedding:
        return []
    sql = """
    SELECT id, title, abstract, primary_category_code, pdf_url, summary_ai,
           detailed_review_ai, arxiv_published_at,
           (title_abstract_embedding <=> %s) AS distance 
    FROM papers
    WHERE title_abstract_embedding IS NOT NULL
    ORDER BY distance ASC
    LIMIT %s;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (query_embedding, top_k))
        results = cur.fetchall()
    return results

# Streamlit 应用主函数
def main():
    st.set_page_config(layout="wide", page_title="arXiv Paper Search", page_icon="📚")
    st.title("📚 arXiv 论文智能检索")
    st.write("使用关键词或语义搜索 arXiv 论文。在下方搜索框中输入内容即可看到建议。")

    # # 加载模型 (数据库连接在需要时由函数内部获取)
    # model = load_model()

    # --- 侧边栏 ---
    with st.sidebar:
        st.header("搜索选项")
        search_type = st.selectbox(
            "选择搜索类型:",
            ["关键词搜索", "语义搜索"],
            key="search_type_selector"
        )

        # 使用 st_searchbox 替代 st.text_input
        # `key` 对于 st_searchbox 是必需的且必须唯一
        # `default_use_searchterm=True` 表示如果用户输入文本但未选择任何建议，则返回用户输入的文本
        selected_search_term = st_searchbox(
            search_function=fetch_search_suggestions, # 获取建议的函数
            key="paper_searchbox_sidebar",
            placeholder="输入论文标题或关键词...",
            label="搜索查询:",
            default_use_searchterm=True, 
            clear_on_submit=False,      # 选择后不清空输入框，便于用户看到当前搜索词
            rerun_on_update=True,       # 当建议更新时重新运行
            debounce=600,               # 延迟600ms执行回调，避免输入过快时频繁请求
        )
        
        if selected_search_term:
            st.caption(f"当前搜索: '{selected_search_term}'")
        
        st.markdown("---")
        st.info("提示：语义搜索更擅长理解句子含义，关键词搜索则精确匹配字词。")

    # --- 结果展示区域 ---
    results_container = st.container()

    if selected_search_term:
        if search_type == "关键词搜索":
            results_container.subheader(f"🔍 \"{selected_search_term}\" 的关键词搜索结果")
            
            # 将用户输入转换为适合to_tsquery的格式
            ts_query_formatted = safe_format_tsquery_input(selected_search_term, prefix_match=False) # 非前缀全词匹配

            if not ts_query_formatted:
                results_container.warning("请输入有效的关键词。")
                return # 提前退出，不执行搜索

            # 使用fts_document进行全文检索
            sql = """
            SELECT id, title, abstract, primary_category_code, pdf_url, summary_ai, 
                   detailed_review_ai, arxiv_published_at,
                   ts_rank(fts_document, to_tsquery('english', %s)) as rank
            FROM papers
            WHERE fts_document @@ to_tsquery('english', %s)
            ORDER BY rank DESC, arxiv_published_at DESC
            LIMIT 10;
            """
            papers = fetch_papers_general(sql, (ts_query_formatted, ts_query_formatted))
            
            if papers:
                for i, paper in enumerate(papers):
                    with results_container.expander(f"**{i+1}. {paper['title']}** (相关度: {paper.get('rank',0):.2f})"):
                        st.markdown(f"**AI 一句话总结**: {paper.get('summary_ai', 'N/A')}")
                        st.markdown(f"**AI 泛读报告摘要**: _{paper.get('detailed_review_ai', 'N/A')}..._") # 显示部分泛读报告
                        st.markdown(f"**ArXiv 摘要**: _{paper.get('abstract', 'N/A')}..._") # 显示部分摘要
                        col1, col2, col3 = st.columns(3)
                        col1.metric("主要分类", paper.get('primary_category_code', 'N/A'))
                        col2.metric("发表日期", paper.get('arxiv_published_at').strftime('%Y-%m-%d') if paper.get('arxiv_published_at') else 'N/A')
                        if paper.get('pdf_url'):
                            col3.link_button("阅读PDF原文", paper['pdf_url'], use_container_width=True)
            else:
                results_container.info("没有找到符合条件的论文。")
        elif search_type == "语义搜索":
            results_container.subheader(f"🧠 \"{selected_search_term}\" 的语义搜索结果")
            results = semantic_search_db(selected_search_term, top_k=10)
            if results:
                for i, paper in enumerate(results):
                    similarity_score = 1 - paper.get('distance', 1.0)
                    with results_container.expander(f"**{i+1}. {paper['title']}** (语义相似度: {similarity_score:.3f})"):
                        st.markdown(f"**AI 一句话总结**: {paper.get('summary_ai', 'N/A')}")
                        st.markdown(f"**AI 泛读报告摘要**: _{paper.get('detailed_review_ai', 'N/A')[:300]}..._")
                        st.markdown(f"**ArXiv 摘要**: _{paper.get('abstract', 'N/A')[:300]}..._")
                        col1, col2, col3 = st.columns(3)
                        col1.metric("主要分类", paper.get('primary_category_code', 'N/A'))
                        col2.metric("发表日期", paper.get('arxiv_published_at').strftime('%Y-%m-%d') if paper.get('arxiv_published_at') else 'N/A')
                        if paper.get('pdf_url'):
                            col3.link_button("阅读PDF原文", paper['pdf_url'], use_container_width=True)
            else:
                results_container.info("没有找到语义上相似的论文。")
    else:
        results_container.info("请在左侧边栏输入搜索词开始检索。")

if __name__ == "__main__":
    main()