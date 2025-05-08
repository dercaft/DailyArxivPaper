# arxiv_pdf_downloader.py
import os
import time
import json
import requests
import concurrent.futures
import argparse
from urllib.parse import urlparse
from tqdm import tqdm

def download_pdf(paper_id, output_dir="pdfs", max_retries=3):
    """
    根据arXiv ID下载论文PDF
    
    Args:
        paper_id: arXiv论文ID (例如: "2105.12345")
        output_dir: PDF保存目录
        max_retries: 最大重试次数
    
    Returns:
        成功返回文件路径，失败返回None
    """
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 构建PDF URL
    pdf_url = f"https://arxiv.org/pdf/{paper_id}.pdf"
    output_path = os.path.join(output_dir, f"{paper_id}.pdf")
    
    # 如果文件已存在且大小正常，跳过下载
    if os.path.exists(output_path) and os.path.getsize(output_path) > 10000:
        return output_path
    
    # 下载PDF文件
    for attempt in range(max_retries):
        try:
            # 使用requests下载，添加合理的User-Agent
            headers = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
            }
            
            # 使用流式下载，避免内存问题
            with requests.get(pdf_url, headers=headers, stream=True, timeout=30) as response:
                response.raise_for_status()  # 确保请求成功
                
                # 获取内容长度，如果有
                total_size = int(response.headers.get('content-length', 0))
                
                # 使用tqdm创建进度条
                with open(output_path, 'wb') as f, tqdm(
                    desc=f"下载 {paper_id}",
                    total=total_size,
                    unit='B',
                    unit_scale=True,
                    unit_divisor=1024,
                    disable=total_size == 0,  # 如果不知道大小则禁用进度条
                ) as bar:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:  # 过滤keep-alive新块
                            f.write(chunk)
                            bar.update(len(chunk))
            
            # 验证下载的PDF大小
            if os.path.getsize(output_path) < 10000:  # 小于10KB可能是错误页面
                raise Exception(f"下载的PDF过小，可能不是有效文件: {output_path}")
                
            return output_path
            
        except Exception as e:
            print(f"下载 {paper_id} 第 {attempt+1}/{max_retries} 次尝试失败: {str(e)}")
            
            # 最后一次尝试失败
            if attempt == max_retries - 1:
                print(f"下载 {paper_id} 彻底失败，放弃")
                # 如果文件存在但可能不完整，删除它
                if os.path.exists(output_path):
                    os.remove(output_path)
                return None
            
            # 等待时间随重试次数增加
            time.sleep(5 * (attempt + 1))
    
    return None

def extract_paper_id(pdf_url):
    """从PDF URL中提取论文ID"""
    # 处理完整URL
    if pdf_url.startswith('http'):
        path = urlparse(pdf_url).path
        # 从路径中获取ID
        if '/pdf/' in pdf_url:
            paper_id = path.split('/pdf/')[1].replace('.pdf', '')
        elif '/abs/' in pdf_url:
            paper_id = path.split('/abs/')[1]
        else:
            # 尝试直接从路径获取ID
            paper_id = os.path.basename(path).replace('.pdf', '')
    else:
        # 可能只是ID本身
        paper_id = pdf_url.replace('.pdf', '')
    
    # 清理ID
    paper_id = paper_id.strip()
    
    return paper_id

def download_papers_from_json(json_file, output_dir="pdfs", max_workers=5, max_retries=3):
    """
    从JSON文件批量下载论文PDF
    
    Args:
        json_file: 包含论文信息的JSON文件
        output_dir: PDF保存目录
        max_workers: 并行下载的最大线程数
        max_retries: 每篇论文最大重试次数
    
    Returns:
        下载成功的论文数量和失败的论文ID列表
    """
    # 读取JSON文件
    with open(json_file, 'r', encoding='utf-8') as f:
        papers = json.load(f)
    
    print(f"从 {json_file} 加载了 {len(papers)} 篇论文")
    
    # 准备下载任务
    download_tasks = []
    for paper in papers:
        # 获取论文ID
        if 'id' in paper:
            paper_id = paper['id']
        elif 'pdf_url' in paper:
            paper_id = extract_paper_id(paper['pdf_url'])
        else:
            print(f"跳过缺少ID或PDF URL的论文: {paper.get('title', 'Unknown')}")
            continue
        
        download_tasks.append(paper_id)
    
    # 使用线程池并行下载
    successful = 0
    failed = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有下载任务
        future_to_id = {
            executor.submit(download_pdf, paper_id, output_dir, max_retries): paper_id
            for paper_id in download_tasks
        }
        
        # 等待任务完成并处理结果
        for future in tqdm(
            concurrent.futures.as_completed(future_to_id),
            total=len(future_to_id),
            desc="总体进度"
        ):
            paper_id = future_to_id[future]
            try:
                file_path = future.result()
                if file_path:
                    successful += 1
                else:
                    failed.append(paper_id)
            except Exception as e:
                print(f"下载 {paper_id} 时发生异常: {str(e)}")
                failed.append(paper_id)
    
    # 输出下载结果
    print(f"\n下载完成: 成功 {successful}/{len(download_tasks)} 篇论文")
    
    if failed:
        # 将失败的ID写入文件
        failed_file = f"failed_downloads_{os.path.basename(json_file).replace('.json', '')}.txt"
        with open(failed_file, 'w', encoding='utf-8') as f:
            for paper_id in failed:
                f.write(f"{paper_id}\n")
        print(f"失败的论文ID已保存到 {failed_file}")
    
    return successful, failed

def download_papers_from_id_list(id_file, output_dir="pdfs", max_workers=5, max_retries=3):
    """
    从包含ID列表的文件批量下载论文PDF
    
    Args:
        id_file: 包含论文ID的文本文件，每行一个ID
        output_dir: PDF保存目录
        max_workers: 并行下载的最大线程数
        max_retries: 每篇论文最大重试次数
    
    Returns:
        下载成功的论文数量和失败的论文ID列表
    """
    # 读取ID文件
    with open(id_file, 'r', encoding='utf-8') as f:
        paper_ids = [line.strip() for line in f if line.strip()]
    
    print(f"从 {id_file} 加载了 {len(paper_ids)} 个论文ID")
    
    # 使用线程池并行下载
    successful = 0
    failed = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有下载任务
        future_to_id = {
            executor.submit(download_pdf, paper_id, output_dir, max_retries): paper_id
            for paper_id in paper_ids
        }
        
        # 等待任务完成并处理结果
        for future in tqdm(
            concurrent.futures.as_completed(future_to_id),
            total=len(future_to_id),
            desc="总体进度"
        ):
            paper_id = future_to_id[future]
            try:
                file_path = future.result()
                if file_path:
                    successful += 1
                else:
                    failed.append(paper_id)
            except Exception as e:
                print(f"下载 {paper_id} 时发生异常: {str(e)}")
                failed.append(paper_id)
    
    # 输出下载结果
    print(f"\n下载完成: 成功 {successful}/{len(paper_ids)} 篇论文")
    
    if failed:
        # 将失败的ID写入文件
        failed_file = f"failed_downloads_{os.path.basename(id_file).replace('.txt', '')}.txt"
        with open(failed_file, 'w', encoding='utf-8') as f:
            for paper_id in failed:
                f.write(f"{paper_id}\n")
        print(f"失败的论文ID已保存到 {failed_file}")
    
    return successful, failed

def main():
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='下载arXiv论文PDF')
    
    # 添加命令行参数
    parser.add_argument('-j', '--json', help='包含论文信息的JSON文件')
    parser.add_argument('-i', '--ids', help='包含论文ID的文本文件，每行一个ID')
    parser.add_argument('-s', '--single', help='单个论文ID')
    parser.add_argument('-o', '--output', default='pdfs', help='PDF保存目录')
    parser.add_argument('-w', '--workers', type=int, default=5, help='并行下载的最大线程数')
    parser.add_argument('-r', '--retries', type=int, default=3, help='每篇论文最大重试次数')
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 确保至少提供了一种输入
    if not (args.json or args.ids or args.single):
        parser.error("必须提供JSON文件(-j)、ID列表文件(-i)或单个ID(-s)中的一个")
    
    # 处理不同类型的输入
    if args.json:
        download_papers_from_json(args.json, args.output, args.workers, args.retries)
    elif args.ids:
        download_papers_from_id_list(args.ids, args.output, args.workers, args.retries)
    elif args.single:
        output_path = download_pdf(args.single, args.output, args.retries)
        if output_path:
            print(f"成功下载到 {output_path}")
        else:
            print(f"下载失败: {args.single}")

if __name__ == "__main__":
    main()