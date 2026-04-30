#!/usr/bin/env python3
"""
nyaa.si 全量元数据爬虫 v1.5 自动停止版
核心功能：
1. 自动扫描目录所有历史CSV，一键全量合并去重
2. 合并后再进行增量爬取，保证总表完整
3. 自动备份历史文件，避免数据丢失
4. 无限翻页、代理支持、断点续爬
5. ★ 新增：爬取数据与已有数据接上后自动停止
"""
import requests
from bs4 import BeautifulSoup
import csv
import time
import sys
import os
import re
import shutil
from datetime import datetime
from urllib.parse import urljoin
from glob import glob

# ─────────────────────────── 全局配置 ───────────────────────────
SITE_NAME = "nyaa.si"
BASE_URL = "https://nyaa.si"
CSV_FILE_PATTERN = "nyaa_data_*.csv"  # 匹配所有历史CSV
FULL_CSV_NAME = "nyaa_data_full.csv"  # 最终合并总表
BACKUP_FOLDER = "csv_backup"  # 历史文件备份目录
FIELD_NAMES = ["name", "info_hash", "magnet", "size", "date", "detail_url"]
UNIQUE_KEY = "detail_url"  # 去重唯一标识

# ─────────────────────────── 颜色输出 ───────────────────────────
def c(text, color):
    codes = {
        "red": "\033[91m", "green": "\033[92m", "yellow": "\033[93m",
        "blue": "\033[94m", "cyan": "\033[96m", "bold": "\033[1m", "reset": "\033[0m"
    }
    return f"{codes.get(color,'')}{text}{codes['reset']}"

# ─────────────────────────── Banner ─────────────────────────────
def print_banner():
    print(c("""
╔══════════════════════════════════════════════════════════╗
║        nyaa.si 全量元数据爬虫  v1.5 自动停止版           ║
║  核心特性：全量历史合并 | 自动去重 | 增量爬取 | 自动备份 ║
║  ★ 新特性：数据衔接自动停止 | 无效翻页检测               ║
║  爬取字段：名称 | InfoHash | Magnet | 大小 | 日期        ║
╚══════════════════════════════════════════════════════════╝
""", "cyan"))

# ─────────────────────────── 核心1：全量合并历史CSV ───────────────────────────
def merge_all_historical_csv():
    """
    扫描目录所有历史CSV，合并成一个无重复的总表
    自动备份旧文件，返回合并后的总表数据和去重集合
    """
    # 1. 扫描所有历史CSV
    csv_files = glob(CSV_FILE_PATTERN)
    # 排除总表本身，避免循环读取
    if FULL_CSV_NAME in csv_files:
        csv_files.remove(FULL_CSV_NAME)

    if not csv_files:
        print(c("\n[ 历史数据扫描 ] 未找到需要合并的历史CSV文件", "yellow"))
        # 直接读取总表（如果存在）
        if os.path.exists(FULL_CSV_NAME):
            print(c(f"  读取已存在的总表 {FULL_CSV_NAME}", "blue"))
            return load_csv_data(FULL_CSV_NAME)
        return [], set()

    print(c(f"\n[ 全量合并历史数据 ]", "bold"))
    print(f"  找到 {len(csv_files)} 个历史CSV文件，开始合并...")

    # 2. 创建备份目录
    if not os.path.exists(BACKUP_FOLDER):
        os.makedirs(BACKUP_FOLDER)
        print(c(f"  ✔ 创建备份目录 {BACKUP_FOLDER}", "green"))

    # 3. 合并所有CSV数据
    merged_data = []
    unique_set = set()
    total_rows = 0
    duplicate_rows = 0

    for file_path in csv_files:
        if not os.path.isfile(file_path):
            continue
        file_size = os.path.getsize(file_path) / 1024
        print(f"  正在读取 {file_path}（{file_size:.2f}KB）...", end="\r", flush=True)

        try:
            with open(file_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                # 校验列
                if UNIQUE_KEY not in reader.fieldnames:
                    print(c(f"\n  ⚠ 跳过 {file_path}：缺少{UNIQUE_KEY}列，格式不匹配", "yellow"))
                    continue

                # 读取并去重
                file_row_count = 0
                for row in reader:
                    unique_value = row.get(UNIQUE_KEY, "").strip()
                    if not unique_value:
                        continue
                    if unique_value not in unique_set:
                        unique_set.add(unique_value)
                        merged_data.append(row)
                        file_row_count += 1
                    else:
                        duplicate_rows += 1

                total_rows += file_row_count
                print(c(f"  ✔ 读取完成 {file_path} | 新增有效条目：{file_row_count} | 重复条目：{duplicate_rows}", "green"))

                # 4. 备份历史文件
                backup_path = os.path.join(BACKUP_FOLDER, f"{os.path.basename(file_path)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
                shutil.copy2(file_path, backup_path)
                print(c(f"  ✔ 已备份到 {backup_path}", "green"))

                # 5. 删除原文件（可选，保留则注释掉）
                # os.remove(file_path)
                # print(c(f"  ✔ 已删除原文件 {file_path}", "green"))

        except Exception as e:
            print(c(f"\n  ✗ 读取 {file_path} 失败：{str(e)}，已跳过", "red"))
            continue

    # 6. 写入合并后的总表
    print(c(f"\n  合并完成：总有效条目 {len(merged_data)} | 去重条目 {duplicate_rows}", "blue"))
    print(f"  正在写入总表 {FULL_CSV_NAME}...", end="\r", flush=True)

    with open(FULL_CSV_NAME, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELD_NAMES)
        writer.writeheader()
        writer.writerows(merged_data)

    print(c(f"  ✔ 总表写入完成！{FULL_CSV_NAME} 大小：{os.path.getsize(FULL_CSV_NAME)/1024:.2f}KB", "green"))
    return merged_data, unique_set

# ─────────────────────────── 辅助：读取CSV数据 ───────────────────────────
def load_csv_data(file_path):
    """读取单个CSV文件，返回数据列表和去重集合"""
    data = []
    unique_set = set()
    if not os.path.exists(file_path):
        return data, unique_set

    with open(file_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            unique_value = row.get(UNIQUE_KEY, "").strip()
            if unique_value:
                unique_set.add(unique_value)
                data.append(row)
    return data, unique_set

# ─────────────────────────── 代理配置 ───────────────────────────
def setup_proxy():
    print(c("\n[ 代理配置 ]", "bold"))
    print("  1. 不使用代理（直连）")
    print("  2. HTTP / HTTPS 代理")
    print("  3. SOCKS5 代理")
    choice = input(c("请选择 [1/2/3]: ", "yellow")).strip()
    proxies = None
    if choice == "2":
        host = input("HTTP 代理主机 (如 127.0.0.1): ").strip()
        port = input("HTTP 代理端口 (如 7890): ").strip()
        proxies = {
            "http":  f"http://{host}:{port}",
            "https": f"http://{host}:{port}",
        }
        print(c(f"✔ 已设置 HTTP 代理: {host}:{port}", "green"))
    elif choice == "3":
        host = input("SOCKS5 代理主机 (如 127.0.0.1): ").strip()
        port = input("SOCKS5 代理端口 (如 1080): ").strip()
        user = input("用户名 (无则回车): ").strip()
        pwd  = input("密码   (无则回车): ").strip()
        if user and pwd:
            proxies = {
                "http":  f"socks5://{user}:{pwd}@{host}:{port}",
                "https": f"socks5://{user}:{pwd}@{host}:{port}",
            }
        else:
            proxies = {
                "http":  f"socks5://{host}:{port}",
                "https": f"socks5://{host}:{port}",
            }
        print(c(f"✔ 已设置 SOCKS5 代理: {host}:{port}", "green"))
    else:
        print(c("✔ 直连模式（不使用代理）", "green"))
    return proxies

# ─────────────────────────── 爬取参数配置 ───────────────────────
def setup_params():
    print(c("\n[ 爬取参数配置 ]", "bold"))
    # 分类筛选
    print("""
分类代码（留空=全部）:
0_0  全部     1_0  动画(Anime)  2_0  音频
3_0  文学     4_0  图片        5_0  软件
6_0  影视     """)
    cat = input(c("分类代码 [默认 0_0]: ", "yellow")).strip() or "0_0"
    # 过滤选项
    print("""
过滤选项:
0  无过滤   1  无转载   2  仅精品 """)
    filt = input(c("过滤 [默认 0]: ", "yellow")).strip() or "0"
    # 关键词
    query = input(c("搜索关键词 [默认留空=全部]: ", "yellow")).strip()
    # 页数范围
    start_page = input(c("起始页 [默认 1]: ", "yellow")).strip()
    start_page = int(start_page) if start_page.isdigit() else 1
    end_page = input(c("结束页 [默认 999999=爬至无内容或数据衔接]: ", "yellow")).strip()
    end_page = int(end_page) if end_page.isdigit() else 999999
    # 请求间隔
    delay = input(c("每页请求间隔秒数 [默认 1.5]: ", "yellow")).strip()
    try:
        delay = float(delay)
    except Exception:
        delay = 1.5

    return {
        "cat": cat, "filter": filt, "q": query,
        "start": start_page, "end": end_page,
        "delay": delay, "out": FULL_CSV_NAME,
    }

# ─────────────────────────── 解析列表页 ─────────────────────────
def parse_list_page(soup):
    """返回该页所有条目的字典列表，每个字典包含 name, detail_url, size, date, magnet"""
    rows = soup.select("table.torrent-list tbody tr")
    items = []
    for row in rows:
        try:
            name_td = row.select_one("td:nth-child(2)")
            if not name_td:
                continue
            links = name_td.select("a:not(.comments)")
            if not links:
                continue
            a = links[-1]
            name = a.get("title") or a.get_text(strip=True)
            detail_url = urljoin(BASE_URL, a.get("href", ""))

            # 提取大小 —— 尝试多种列位置（适配不同版本的 nyaa 页面结构）
            size = ""
            for sel in ["td:nth-child(4)", "td:nth-child(3)"]:
                size_td = row.select_one(sel)
                if size_td:
                    size_text = size_td.get_text(strip=True)
                    # 大小列通常含数字+单位（如 1.5 GiB），以此区分
                    if re.search(r'[\d.]+\s*[KMGT]?i?B', size_text, re.IGNORECASE):
                        size = size_text
                        break

            # 提取日期 —— 尝试多种列位置
            date = ""
            for sel in ["td:nth-child(6)", "td:nth-child(5)"]:
                date_td = row.select_one(sel)
                if date_td:
                    date_text = date_td.get_text(strip=True)
                    # 日期列通常含 年-月-日 或 月-日 格式
                    if re.search(r'\d{4}-\d{2}-\d{2}', date_text) or re.search(r'\d{2}-\d{2}', date_text):
                        date = date_text
                        break

            # 尝试从列表页直接提取 magnet 链接
            magnet = ""
            magnet_links = row.select("a[href^='magnet:']")
            if magnet_links:
                magnet = magnet_links[0].get("href", "")

            # 提取 info_hash（从 magnet 链接中解析）
            info_hash = extract_info_hash(magnet)

            items.append({
                "name": name,
                "info_hash": info_hash,
                "magnet": magnet,
                "size": size,
                "date": date,
                "detail_url": detail_url,
            })
        except Exception:
            continue
    return items

# ─────────────────────────── 提取 InfoHash ─────────────────────────
def extract_info_hash(magnet):
    """从 magnet URI 中提取 info_hash（支持 SHA-1 40位 和 Base32 32位）"""
    if not magnet:
        return ""
    # SHA-1 十六进制（40位）
    match = re.search(r'btih:([a-fA-F0-9]{40})', magnet, re.IGNORECASE)
    if match:
        return match.group(1).upper()
    # Base32（32位）
    match = re.search(r'btih:([A-Z2-7]{32})', magnet)
    if match:
        return match.group(1)
    return ""

# ─────────────────────────── 解析详情页 ─────────────────────────
def parse_detail_page(session, detail_url, proxies=None):
    """从详情页提取 magnet 和 info_hash（备用方案，当列表页未提取到时使用）"""
    try:
        resp = session.get(detail_url, timeout=30, proxies=proxies)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        magnet = ""
        magnet_a = soup.select_one("a[href^='magnet:']")
        if magnet_a:
            magnet = magnet_a.get("href", "")

        info_hash = extract_info_hash(magnet)

        # 备用：尝试从页面文本中提取 info_hash
        if not info_hash:
            page_text = soup.get_text()
            match = re.search(r'(?:info.?hash[:\s]*)([a-fA-F0-9]{40})', page_text, re.IGNORECASE)
            if match:
                info_hash = match.group(1).upper()

        return info_hash, magnet
    except Exception:
        return "", ""

# ─────────────────────────── 核心2：增量爬取（含自动停止） ──────────────────────
def run_scraper(params, existing_data, unique_set, proxies):
    """
    从第1页开始增量爬取，当检测到新爬数据与已有数据衔接时自动停止。

    自动停止逻辑：
    - nyaa.si 按时间倒序排列（第1页最新，越往后越旧）
    - 当某一页的【所有条目】都已在 unique_set 中存在时，
      说明已经爬到了上次已有的数据边界，无需继续翻页
    - 同时记录连续"全重复页"的次数，只有连续 2 页全重复才确认停止，
      避免因单页恰好全是旧数据而误判
    """
    session = requests.Session()
    if proxies:
        session.proxies = proxies
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    })

    start_page = params["start"]
    end_page = params["end"]
    delay = params["delay"]
    has_existing_data = len(existing_data) > 0

    new_items = []
    total_new = 0
    total_dup = 0
    total_pages_scraped = 0
    consecutive_all_dup_pages = 0  # 连续全重复页计数
    CONSECUTIVE_THRESHOLD = 2       # 连续 N 页全重复才确认停止

    print(c(f"\n{'='*60}", "cyan"))
    print(c(f"  开始增量爬取", "bold"))
    print(c(f"{'='*60}", "cyan"))
    print(f"  已有历史数据：{len(existing_data)} 条")
    print(f"  起始页：{start_page} | 最大结束页：{end_page}")
    if has_existing_data:
        print(c(f"  ★ 自动停止已启用（连续 {CONSECUTIVE_THRESHOLD} 页全重复时停止）", "green"))
    else:
        print(c(f"  ⚠ 无历史数据，将爬至列表末尾或达到最大页数", "yellow"))
    print(c(f"{'='*60}\n", "cyan"))

    for page in range(start_page, end_page + 1):
        # 构建请求 URL
        url = f"{BASE_URL}/?p={page}"
        if params.get("cat", "0_0") != "0_0":
            url += f"&c={params['cat']}"
        if params.get("filter", "0") != "0":
            url += f"&f={params['filter']}"
        if params.get("q"):
            url += f"&q={params['q']}"

        # 请求页面
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            print(c(f"  ✗ 第 {page} 页请求失败: {e}", "red"))
            # 连续失败则停止
            consecutive_all_dup_pages += 1
            if consecutive_all_dup_pages >= 3:
                print(c(f"\n  ✗ 连续 3 页请求失败，停止爬取", "red"))
                break
            time.sleep(delay)
            continue

        # 解析页面
        soup = BeautifulSoup(resp.text, "html.parser")
        items = parse_list_page(soup)

        # 空页 = 到达列表末尾
        if not items:
            print(c(f"  ✔ 第 {page:4d} 页无数据，已到达列表末尾", "green"))
            break

        # 统计本页新增/重复
        page_new = 0
        page_dup = 0

        for item in items:
            detail_url = item.get("detail_url", "").strip()
            if not detail_url:
                continue

            if detail_url in unique_set:
                # 已存在，跳过
                page_dup += 1
            else:
                # 新条目
                page_new += 1
                new_items.append(item)
                unique_set.add(detail_url)

                # 如果列表页未提取到 magnet/info_hash，尝试从详情页获取
                if not item.get("info_hash") and not item.get("magnet"):
                    info_hash, magnet = parse_detail_page(session, detail_url, proxies)
                    if info_hash:
                        item["info_hash"] = info_hash
                    if magnet:
                        item["magnet"] = magnet
                    # 详情页请求后稍微多等一会儿
                    time.sleep(delay * 0.5)

        total_new += page_new
        total_dup += page_dup
        total_pages_scraped += 1

        # 输出本页统计
        status_icon = "🆕" if page_new > 0 else "🔄"
        print(f"  {status_icon} 第 {page:4d} 页 | 新增: {page_new:3d} | 重复: {page_dup:3d} | "
              f"累计新增: {total_new:5d} | 累计重复: {total_dup:5d}")

        # ── ★ 自动停止判断 ──
        if has_existing_data and page_dup > 0 and page_new == 0:
            consecutive_all_dup_pages += 1
            print(c(f"    └─ 连续全重复页: {consecutive_all_dup_pages}/{CONSECUTIVE_THRESHOLD}", "yellow"))

            if consecutive_all_dup_pages >= CONSECUTIVE_THRESHOLD:
                print(c(f"\n{'='*60}", "green"))
                print(c(f"  ★ 自动停止触发！", "bold"))
                print(c(f"  连续 {CONSECUTIVE_THRESHOLD} 页所有条目均已在已有数据中", "green"))
                print(c(f"  新旧数据已成功衔接，无需继续爬取更旧的数据", "green"))
                print(c(f"{'='*60}", "green"))
                break
        else:
            # 本页有新数据，重置连续计数
            consecutive_all_dup_pages = 0

        # 每 10 页自动保存进度（防止中断丢失数据）
        if total_pages_scraped % 10 == 0 and new_items:
            save_data(existing_data, new_items, params["out"])
            print(c(f"    └─ 💾 进度已自动保存", "blue"))

        time.sleep(delay)

    return new_items, total_new, total_dup, total_pages_scraped

# ─────────────────────────── 保存数据 ─────────────────────────
def save_data(existing_data, new_items, output_file):
    """将新数据和已有数据合并写入 CSV（新数据在前，已有数据在后）"""
    all_data = new_items + existing_data
    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELD_NAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_data)
    return len(all_data)

# ─────────────────────────── 主函数 ─────────────────────────────
def main():
    print_banner()

    # 步骤1：合并历史CSV数据
    print(c("\n[ 步骤 1/3 ] 合并历史数据", "bold"))
    existing_data, unique_set = merge_all_historical_csv()

    # 步骤2：配置代理和参数
    print(c("\n[ 步骤 2/3 ] 配置爬取参数", "bold"))
    proxies = setup_proxy()
    params = setup_params()

    # 步骤3：增量爬取（含自动停止）
    print(c("\n[ 步骤 3/3 ] 开始增量爬取", "bold"))
    new_items, total_new, total_dup, total_pages = run_scraper(
        params, existing_data, unique_set, proxies
    )

    # 最终保存
    print(c(f"\n[ 保存数据 ]", "bold"))
    total_count = save_data(existing_data, new_items, params["out"])
    file_size = os.path.getsize(params["out"]) / 1024

    # 最终统计
    print(c(f"\n{'='*60}", "cyan"))
    print(c(f"  爬取完成！", "bold"))
    print(c(f"{'='*60}", "cyan"))
    print(f"  爬取页数：{total_pages}")
    print(f"  新增条目：{c(str(total_new), 'green')}")
    print(f"  重复跳过：{c(str(total_dup), 'yellow')}")
    print(f"  数据总量：{c(str(total_count), 'bold')}")
    print(f"  文件大小：{file_size:.2f} KB")
    print(c(f"  ✔ 数据已保存到 {params['out']}", "green"))
    print(c(f"{'='*60}\n", "cyan"))

if __name__ == "__main__":
    main()
