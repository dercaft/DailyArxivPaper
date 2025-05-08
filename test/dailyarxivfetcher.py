# arxiv_fetcher.py
import arxiv
import datetime
import json
import time
import os
from tqdm import tqdm

# CS领域的所有子类别，按照预估的日发布量分组
HIGH_VOLUME_CATEGORIES = ["cs.AI", "cs.CV", "cs.LG", "cs.CL"]  # 高流量领域
MEDIUM_VOLUME_CATEGORIES = ["cs.CR", "cs.NE", "cs.RO", "cs.IR", "cs.SE", "cs.SI", "cs.HC", "cs.DB"]  # 中流量领域
LOW_VOLUME_CATEGORIES = [  # 低流量领域
    "cs.AR", "cs.CC", "cs.CE", "cs.CG", "cs.CY", "cs.DC", "cs.DL", "cs.DM", 
    "cs.DS", "cs.ET", "cs.FL", "cs.GL", "cs.GR", "cs.GT", "cs.IT", "cs.LO", 
    "cs.MA", "cs.MM", "cs.MS", "cs.NA", "cs.NI", "cs.OH", "cs.OS", 
    "cs.PF", "cs.PL", "cs.SC", "cs.SD", "cs.SY"
]

# 确保所有CS领域被覆盖
ALL_CS_CATEGORIES = HIGH_VOLUME_CATEGORIES + MEDIUM_VOLUME_CATEGORIES + LOW_VOLUME_CATEGORIES

def fetch_papers_by_category(category, date_str, client, batch_size=100, max_total=None):
    """抓取特定类别的论文
    
    Args:
        category: 要抓取的类别
        date_str: 日期字符串 YYYYMMDD
        client: arxiv客户端
        batch_size: 每批次抓取论文数
        max_total: 该类别最大抓取总数，None表示不限
        
    Returns:
        该类别的论文列表
    """
    # 构建查询
    date_query = f"submittedDate:[{date_str}000000 TO {date_str}235959]"
    query = f"({date_query}) AND (cat:{category})"
    
    papers = []
    failures = 0
    max_failures = 3  # 最大失败重试次数
    
    try:
        # 创建搜索
        search = arxiv.Search(
            query=query,
            max_results=max_total,
            sort_by=arxiv.SortCriterion.SubmittedDate
        )
        
        # 分批获取
        results = client.results(search)
        
        # 使用tqdm进度条，但可能无法准确显示总数
        batch = []
        for i, result in enumerate(results):
            batch.append(result)
            
            if len(batch) >= batch_size or max_total and i+1 >= max_total:
                # 处理当前批次
                for paper_result in tqdm(batch, desc=f"处理 {category} 批次"):
                    paper = {
                        "id": paper_result.entry_id.split("/")[-1],
                        "title": paper_result.title,
                        "authors": [author.name for author in paper_result.authors],
                        "abstract": paper_result.summary.replace("\n", " "),
                        "categories": paper_result.categories,
                        "primary_category": paper_result.primary_category,
                        "pdf_url": paper_result.pdf_url,
                        "published": paper_result.published.strftime("%Y-%m-%d"),
                        "updated": paper_result.updated.strftime("%Y-%m-%d"),
                        "summary": None,
                        "detailed_review": None
                    }
                    papers.append(paper)
                
                # 清空批次
                batch = []
                # 批次间暂停
                time.sleep(3)
                
                # 如果达到最大数量，退出
                if max_total and i+1 >= max_total:
                    break
        
        # 处理最后一个不完整批次
        if batch:
            for paper_result in tqdm(batch, desc=f"处理 {category} 最后批次"):
                paper = {
                    "id": paper_result.entry_id.split("/")[-1],
                    "title": paper_result.title,
                    "authors": [author.name for author in paper_result.authors],
                    "abstract": paper_result.summary.replace("\n", " "),
                    "categories": paper_result.categories,
                    "primary_category": paper_result.primary_category,
                    "pdf_url": paper_result.pdf_url,
                    "published": paper_result.published.strftime("%Y-%m-%d"),
                    "updated": paper_result.updated.strftime("%Y-%m-%d"),
                    "summary": None,
                    "detailed_review": None
                }
                papers.append(paper)
                
    except Exception as e:
        print(f"抓取类别 {category} 时出错: {str(e)}")
        failures += 1
        if failures < max_failures:
            print(f"尝试重试... ({failures}/{max_failures})")
            time.sleep(10)  # 失败后等待更长时间
            # 递归重试，但减少目标数量
            if max_total:
                max_retry = max(50, max_total // 2)  # 重试时减少抓取数量
            else:
                max_retry = 200
            retry_papers = fetch_papers_by_category(
                category, date_str, client, batch_size, max_retry
            )
            papers.extend(retry_papers)
    
    return papers

def fetch_daily_papers(target_date=None):
    """抓取给定日期更新的所有CS领域论文
    
    Args:
        target_date: 目标日期，默认为昨天
        
    Returns:
        包含论文信息的列表
    """
    # 确定目标日期
    if target_date is None:
        target_date = datetime.datetime.now() - datetime.timedelta(days=1)
    
    date_str = target_date.strftime('%Y%m%d')
    print(f"获取 {date_str} 更新的论文...")
    
    # 创建客户端，调整参数适应大量请求
    client = arxiv.Client(page_size=100, delay_seconds=3.0)
    
    all_papers = []
    temp_dir = "temp_papers"
    os.makedirs(temp_dir, exist_ok=True)
    
    # 1. 先处理高流量类别，逐个处理，使用更小的批次和更多暂停
    for category in HIGH_VOLUME_CATEGORIES:
        print(f"\n正在抓取高流量类别: {category}")
        # 针对高流量领域，直接限制最大数量为500，防止过度请求
        papers = fetch_papers_by_category(category, date_str, client, batch_size=50, max_total=500)
        all_papers.extend(papers)
        print(f"{category}: 获取到 {len(papers)} 篇论文")
        
        # 保存临时文件，以防中途失败
        temp_file = os.path.join(temp_dir, f"{date_str}_{category}.json")
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(papers, f, ensure_ascii=False, indent=2)
        
        # 高流量类别之间暂停更长时间
        time.sleep(10)
    
    # 2. 处理中流量类别，可以2-3个一组处理
    for i in range(0, len(MEDIUM_VOLUME_CATEGORIES), 2):
        batch = MEDIUM_VOLUME_CATEGORIES[i:i+2]
        for category in batch:
            print(f"\n正在抓取中流量类别: {category}")
            papers = fetch_papers_by_category(category, date_str, client, batch_size=100, max_total=300)
            all_papers.extend(papers)
            print(f"{category}: 获取到 {len(papers)} 篇论文")
            
            # 保存临时文件
            temp_file = os.path.join(temp_dir, f"{date_str}_{category}.json")
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(papers, f, ensure_ascii=False, indent=2)
        
        # 每组之间暂停
        time.sleep(8)
    
    # 3. 处理低流量类别，可以5个一组处理
    for i in range(0, len(LOW_VOLUME_CATEGORIES), 5):
        batch = LOW_VOLUME_CATEGORIES[i:i+5]
        batch_papers = []
        
        for category in batch:
            print(f"\n正在抓取低流量类别: {category}")
            papers = fetch_papers_by_category(category, date_str, client, batch_size=100, max_total=None)
            batch_papers.extend(papers)
            all_papers.extend(papers)
            print(f"{category}: 获取到 {len(papers)} 篇论文")
        
        # 每组保存一次临时文件
        batch_name = "_".join(batch)
        temp_file = os.path.join(temp_dir, f"{date_str}_batch_{i//5}.json")
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(batch_papers, f, ensure_ascii=False, indent=2)
        
        # 每组之间暂停
        time.sleep(5)
    
    # 去重处理
    unique_papers = {}
    for paper in all_papers:
        if paper["id"] not in unique_papers:
            unique_papers[paper["id"]] = paper
    
    result_papers = list(unique_papers.values())
    print(f"\n总共获取到 {len(result_papers)} 篇唯一论文 (去重前: {len(all_papers)})")
    return result_papers

def save_papers_to_file(papers, filename=None):
    """将论文保存到JSON文件"""
    if filename is None:
        today = datetime.datetime.now().strftime('%Y%m%d')
        filename = f"arxiv_papers_{today}.json"
    
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(papers, f, ensure_ascii=False, indent=2)
    print(f"已保存到 {filename}")
    
    # 生成统计信息
    categories_count = {}
    for paper in papers:
        primary = paper.get("primary_category")
        if primary in categories_count:
            categories_count[primary] += 1
        else:
            categories_count[primary] = 1
    
    print("\n各类别论文数量:")
    for cat, count in sorted(categories_count.items(), key=lambda x: x[1], reverse=True):
        print(f"{cat}: {count}篇")

def merge_temp_papers(temp_dir="temp_papers", target_date=None):
    """合并临时文件中的论文数据"""
    if target_date is None:
        target_date = datetime.datetime.now() - datetime.timedelta(days=1)
    
    date_str = target_date.strftime('%Y%m%d')
    
    all_papers = []
    for filename in os.listdir(temp_dir):
        if date_str in filename and filename.endswith(".json"):
            file_path = os.path.join(temp_dir, filename)
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    papers = json.load(f)
                    all_papers.extend(papers)
                    print(f"从 {filename} 加载了 {len(papers)} 篇论文")
            except Exception as e:
                print(f"处理文件 {filename} 时出错: {str(e)}")
    
    # 去重处理
    unique_papers = {}
    for paper in all_papers:
        if paper["id"] not in unique_papers:
            unique_papers[paper["id"]] = paper
    
    result_papers = list(unique_papers.values())
    print(f"\n总共从临时文件中加载并合并了 {len(result_papers)} 篇唯一论文")
    return result_papers

def main():
    # 获取命令行参数，如果有的话
    import sys
    
    # 默认使用昨天的日期
    target_date = datetime.datetime.now() - datetime.timedelta(days=1)
    
    # 允许指定日期，格式为YYYYMMDD
    if len(sys.argv) > 1 and len(sys.argv[1]) == 8:
        try:
            year = int(sys.argv[1][:4])
            month = int(sys.argv[1][4:6])
            day = int(sys.argv[1][6:])
            target_date = datetime.datetime(year, month, day)
            print(f"使用指定日期: {target_date.strftime('%Y-%m-%d')}")
        except ValueError:
            print("日期格式错误，使用默认日期(昨天)")
    
    # 执行抓取
    papers = fetch_daily_papers(target_date)
    
    # 如果抓取失败或数量太少，尝试从临时文件合并
    if len(papers) < 50:
        print("抓取结果过少，尝试从临时文件合并...")
        papers = merge_temp_papers(target_date=target_date)
    
    # 保存结果
    date_str = target_date.strftime('%Y%m%d')
    save_papers_to_file(papers, f"arxiv_papers_{date_str}.json")

if __name__ == "__main__":
    main()