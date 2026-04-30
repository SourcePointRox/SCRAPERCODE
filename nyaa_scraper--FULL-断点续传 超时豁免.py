#!/usr/bin/env python3
"""
nyaa.si 全量元数据爬虫  v2.1 直链遍历版（增强断点续爬）
策略变更：废弃列表页翻页，改为直接遍历 /view/{id} 链接爬取全站数据
自动检测最新条目ID，从最早(15)遍历到最新，支持断点续爬
爬取资源名称、信息哈希值、Magnet链接、资源大小，存储为 CSV
支持交互式配置代理（HTTP/HTTPS/SOCKS5）

v2.1 新增：启动时自动扫描运行目录下已有的 CSV 文件，检测爬取进度（最后序号），
        一键续爬，新数据自动追加合并进原文件。
"""
import requests
from bs4 import BeautifulSoup
import csv
import time
import sys
import os
import re
import glob
import base64
from datetime import datetime
from urllib.parse import urljoin

# ─────────────────────────── 颜色输出 ───────────────────────────
def c(text, color):
    codes = {"red": "\033[91m", "green": "\033[92m", "yellow": "\033[93m",
             "blue": "\033[94m", "cyan": "\033[96m", "bold": "\033[1m", "reset": "\033[0m"}
    return f"{codes.get(color,'')}{text}{codes['reset']}"

# ─────────────────────────── Banner ─────────────────────────────
def print_banner():
    print(c("""
╔══════════════════════════════════════════════════════════════╗
║     nyaa.si 全量元数据爬虫  v2.1 直链遍历版（增强断点续爬）   ║
║  策略: 自动检测最新ID → /view/{id} 直连遍历 → 全站爬取      ║
║  爬取: 资源名称 | InfoHash | Magnet | 大小 | 日期            ║
║  支持: 自动断点续爬 | 代理 | 自定义ID范围                    ║
╚══════════════════════════════════════════════════════════════╝
""", "cyan"))

# ─────────────────────────── 自动检测断点续爬 ───────────────────────
def auto_detect_resume(csv_pattern):
    """
    扫描运行目录下是否存在匹配的 CSV 文件，自动检测每个文件的爬取进度。
    返回按修改时间倒序排列的候选文件列表。
    每个候选元素: {"path", "records", "max_id", "min_id", "modified"}
    """
    files = sorted(glob.glob(csv_pattern), key=os.path.getmtime, reverse=True)
    if not files:
        return []

    candidates = []
    for filepath in files:
        try:
            max_id = 0
            min_id = float("inf")
            record_count = 0
            with open(filepath, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    record_count += 1
                    row_id_str = row.get("id", "")
                    if row_id_str:
                        try:
                            rid = int(row_id_str)
                            if rid > max_id:
                                max_id = rid
                            if rid < min_id:
                                min_id = rid
                        except ValueError:
                            pass
            if record_count > 0 and min_id == float("inf"):
                min_id = 0
            candidates.append({
                "path": filepath,
                "records": record_count,
                "max_id": max_id,
                "min_id": min_id,
                "modified": datetime.fromtimestamp(os.path.getmtime(filepath)).strftime("%Y-%m-%d %H:%M:%S"),
            })
        except Exception:
            continue

    return candidates


def setup_resume(candidates, default_start_id):
    """
    展示检测到的断点文件列表，让用户选择是否续爬。
    返回 (resume_file_path_or_None, effective_start_id)
    """
    print(c("\n[ 断点续爬检测 ]", "bold"))
    print(c(f"  在当前目录下检测到 {len(candidates)} 个已有的 CSV 文件:\n", "yellow"))

    for i, info in enumerate(candidates):
        print(f"  {c(f'[{i+1}]', 'cyan')} {info['path']}")
        print(f"       已有记录: {info['records']} 条 | "
              f"ID 范围: {info['min_id']} ~ {info['max_id']} | "
              f"修改时间: {info['modified']}")

    print(f"\n  {c('[0]', 'cyan')} 创建新文件，从头开始爬取")
    print()

    choice = input(c("请选择 [默认 1 续爬 / 0 新建]: ", "yellow")).strip()

    if choice == "0":
        print(c("  ✔ 将创建新文件，从头开始爬取", "green"))
        return None, default_start_id

    # 解析选择
    idx = int(choice) - 1 if choice.isdigit() and choice != "0" else 0
    if idx < 0 or idx >= len(candidates):
        idx = 0

    selected = candidates[idx]
    resume_start = selected["max_id"] + 1

    print(c(f"\n  ✔ 已选择续爬文件: {selected['path']}", "green"))
    print(c(f"    已有 {selected['records']} 条有效记录", "green"))
    print(c(f"    最后爬取的 ID: {selected['max_id']}", "green"))
    print(c(f"    将从 ID {resume_start} 继续向后爬取", "green"))
    print()

    return selected["path"], resume_start

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
    """
    配置爬取参数。启动时自动扫描目录下 nyaa_data_*.csv 文件，
    检测到已有文件时提供一键续爬选项。
    """
    DEFAULT_START_ID = 15
    CSV_PATTERN = "nyaa_data_*.csv"
    CSV_PREFIX = "nyaa_data"

    print(c("\n[ 爬取参数配置 ]", "bold"))

    # ── 第一步：自动断点续爬检测 ──
    candidates = auto_detect_resume(CSV_PATTERN)
    resume_file = None
    resume_start = None

    if candidates:
        resume_file, resume_start = setup_resume(candidates, DEFAULT_START_ID)

    # ── 第二步：配置参数 ──
    if resume_file:
        # 续爬模式：自动填入起始ID和输出文件，用户仍可修改
        start_id_input = input(c(f"起始条目ID [默认 {resume_start}，从断点继续]: ", "yellow")).strip()
        start_id = int(start_id_input) if start_id_input.isdigit() else resume_start

        out_file_input = input(c(f"输出文件名 [默认 {resume_file}，续爬合并]: ", "yellow")).strip()
        out_file = out_file_input if out_file_input else resume_file
    else:
        # 新建模式
        start_id = input(c(f"起始条目ID [默认 {DEFAULT_START_ID}]: ", "yellow")).strip()
        start_id = int(start_id) if start_id.isdigit() else DEFAULT_START_ID

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_out = f"{CSV_PREFIX}_{ts}.csv"
        out_file = input(c(f"输出文件名 [默认 {default_out}]: ", "yellow")).strip() or default_out

    # 结束ID（auto=自动检测）
    end_id_input = input(c("结束条目ID [默认 auto=自动检测最新]: ", "yellow")).strip().lower()
    if end_id_input == "auto" or end_id_input == "":
        end_id = "auto"
    else:
        end_id = int(end_id_input) if end_id_input.isdigit() else "auto"

    # 请求间隔
    delay = input(c("每次请求间隔秒数 [默认 1.0]: ", "yellow")).strip()
    try:
        delay = float(delay)
    except Exception:
        delay = 1.0

    # 连续失败多少次后暂停（防封）
    max_consecutive_fail = input(c("连续失败多少次后暂停等待 [默认 100]: ", "yellow")).strip()
    max_consecutive_fail = int(max_consecutive_fail) if max_consecutive_fail.isdigit() else 100

    # 暂停等待秒数
    pause_wait = input(c("暂停等待秒数 [默认 300]: ", "yellow")).strip()
    try:
        pause_wait = float(pause_wait)
    except Exception:
        pause_wait = 300

    # 请求超时时间（秒）
    timeout_input = input(c("请求超时时间（秒）[默认 60]: ", "yellow")).strip()
    try:
        timeout_val = float(timeout_input)
    except Exception:
        timeout_val = 60

    # 超时/连接错误重试次数
    retry_input = input(c("超时/连接错误重试次数 [默认 3]: ", "yellow")).strip()
    retry_count = int(retry_input) if retry_input.isdigit() else 3

    return {
        "start_id": start_id,
        "end_id": end_id,
        "delay": delay,
        "max_consecutive_fail": max_consecutive_fail,
        "pause_wait": pause_wait,
        "timeout": timeout_val,
        "retry_count": retry_count,
        "out": out_file,
        "is_resume": resume_file is not None,
    }

# ─────────────────────────── 带重试的HTTP请求 ──────────────────────
def request_with_retry(session, url, timeout=60, retry_count=3, **kwargs):
    """带重试机制的HTTP GET请求，对超时和连接错误自动重试"""
    last_error = None
    for attempt in range(retry_count + 1):
        try:
            return session.get(url, timeout=timeout, **kwargs)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_error = e
            if attempt < retry_count:
                wait_backoff = 5 * (2 ** attempt)
                err_type = "超时" if isinstance(e, requests.exceptions.Timeout) else "连接错误"
                print(c(f"\n  ⚠ 请求{err_type}, 第{attempt+1}/{retry_count}次重试, 等待{wait_backoff}秒...", "yellow"), flush=True)
                time.sleep(wait_backoff)
    raise last_error

# ─────────────────────────── 自动检测最新条目ID ──────────────────
def detect_latest_id(session, base_url, timeout=60):
    """访问首页，按ID降序排列，提取第一条目的ID作为最新ID"""
    print(c("\n[ 自动检测最新条目ID ]", "bold"))
    try:
        # 访问首页，按ID降序
        r = session.get(base_url, params={"s": "id", "o": "desc"}, timeout=timeout)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        rows = soup.select("table.torrent-list tbody tr")
        if rows:
            # 从第一行提取详情链接中的ID
            first_row = rows[0]
            links = first_row.select("td:nth-child(2) a:not(.comments)")
            if links:
                href = links[-1].get("href", "")
                m = re.search(r"/view/(\d+)", href)
                if m:
                    latest_id = int(m.group(1))
                    # 再从所有行中找最大值（更保险）
                    max_id = latest_id
                    for row in rows:
                        row_links = row.select("td:nth-child(2) a:not(.comments)")
                        for link in row_links:
                            row_href = link.get("href", "")
                            row_m = re.search(r"/view/(\d+)", row_href)
                            if row_m:
                                row_id = int(row_m.group(1))
                                if row_id > max_id:
                                    max_id = row_id
                    print(c(f"✔ 检测到最新条目ID: {max_id} (首页可见范围)", "green"))
                    return max_id
        # 备用方案：尝试直接访问一个较大ID确认是否存在
        print(c("⚠ 首页解析失败，尝试备用检测方法...", "yellow"))
        # 尝试二分查找法快速定位最新ID
        test_id = 5000000  # 从一个较大值开始
        latest = None
        for _ in range(20):  # 最多二分20次
            r = session.get(f"{base_url}/view/{test_id}", timeout=timeout, allow_redirects=False)
            if r.status_code == 200:
                latest = test_id
                test_id += 500000
            elif r.status_code in (301, 302, 404):
                if latest is None:
                    test_id = test_id // 2
                else:
                    break
            else:
                break
        if latest:
            # 精确查找：从latest开始递增直到找到不存在的
            for offset in range(0, 100000):
                check_id = latest + offset
                r = session.get(f"{base_url}/view/{check_id}", timeout=timeout, allow_redirects=False)
                if r.status_code == 200:
                    latest = check_id
                else:
                    break
            print(c(f"✔ 备用检测到最新条目ID: {latest}", "green"))
            return latest
        print(c("✗ 无法自动检测最新ID，请手动输入", "red"))
        manual = input(c("请手动输入最新条目ID: ", "yellow")).strip()
        return int(manual) if manual.isdigit() else 2093718
    except Exception as e:
        print(c(f"✗ 自动检测失败: {e}", "red"))
        manual = input(c("请手动输入最新条目ID: ", "yellow")).strip()
        return int(manual) if manual.isdigit() else 2093718

# ─────────────────────────── 解析详情页（全字段） ─────────────────
def parse_detail_page(soup, detail_url):
    """从详情页提取全部信息: 名称、InfoHash、Magnet、大小、日期"""
    name = ""
    info_hash = ""
    magnet = ""
    size = ""
    date = ""

    # ── 名称：优先从页面标题提取 ──
    title_tag = soup.find("title")
    if title_tag:
        title_text = title_tag.get_text(strip=True)
        # nyaa详情页标题格式通常是: "名称 | nyaa"
        name = re.sub(r"\s*\|.*$", "", title_text).strip()
    # 备用：从 h3.panel-title 或 .torrent-name 提取
    if not name:
        h3 = soup.select_one("h3.panel-title")
        if h3:
            name = h3.get_text(strip=True)
    if not name:
        # 尝试从 meta og:title
        og_title = soup.find("meta", property="og:title")
        if og_title and og_title.get("content"):
            name = og_title["content"].strip()

    # ── InfoHash：<kbd> 标签 ──
    kbd = soup.select_one("kbd")
    if kbd:
        info_hash = kbd.get_text(strip=True)

    # ── Magnet 链接 ──
    mag_a = soup.find("a", href=re.compile(r"^magnet:"))
    if mag_a:
        magnet = mag_a["href"]

    # 备用：从 magnet 链接中提取 xt=urn:btih:HASH
    if not info_hash and magnet:
        m = re.search(r"xt=urn:btih:([a-fA-F0-9]{40})", magnet)
        if m:
            info_hash = m.group(1).lower()
        else:
            # 也可能是 Base32 编码的 hash
            m = re.search(r"xt=urn:btih:([A-Z2-7]{32})", magnet)
            if m:
                try:
                    b32 = m.group(1)
                    info_hash = base64.b16encode(base64.b32decode(b32)).decode().lower()
                except Exception:
                    pass

    # ── 大小和日期：从详情面板的 div.row 中提取 ──
    rows_divs = soup.select("div.panel-body div.row")
    for row_div in rows_divs:
        text = row_div.get_text(strip=True)
        # 提取大小
        if not size:
            size_match = re.search(r"((?:\d+\.?\d*)\s*(?:Bytes?|KB|MB|GB|TB|PiB))", text, re.IGNORECASE)
            if size_match:
                # 排除种子文件大小（.torrent 文件大小），只取总大小
                if "Total" in text or "总" in text or "Size" in text or "大小" in text or "File size" in text.lower():
                    size = size_match.group(1).strip()
                elif not re.search(r"\.torrent", text, re.IGNORECASE):
                    size = size_match.group(1).strip()
        # 提取日期
        if not date:
            date_match = re.search(r"(\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}:\d{2})?)", text)
            if date_match:
                date = date_match.group(1)
            else:
                # 备用：查找带 data-timestamp 的元素
                ts_elem = row_div.select_one("[data-timestamp]")
                if ts_elem:
                    ts = ts_elem.get("data-timestamp", "")
                    if ts:
                        try:
                            date = datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
                        except Exception:
                            pass

    # 备用大小提取：遍历所有文本节点
    if not size:
        body = soup.select_one("div.panel-body")
        if body:
            body_text = body.get_text()
            # 通常 "Total size" 或文件列表中有大小信息
            size_match = re.search(r"(?:Total\s+size|文件大小|总大小)[:\s]*((?:\d+\.?\d*)\s*(?:Bytes?|KB|MB|GB|TB|PiB))", body_text, re.IGNORECASE)
            if size_match:
                size = size_match.group(1).strip()
            else:
                # 取最大的那个尺寸值
                all_sizes = re.findall(r"((?:\d+\.?\d*)\s*(?:Bytes?|KB|MB|GB|TB|PiB))", body_text, re.IGNORECASE)
                if all_sizes:
                    for s in reversed(all_sizes):
                        if re.search(r"(TB|GB|PiB)", s, re.IGNORECASE):
                            size = s.strip()
                            break
                    if not size:
                        size = all_sizes[-1].strip()

    # 备用日期提取
    if not date:
        # 从页面其他位置查找日期
        for elem in soup.select("[data-timestamp]"):
            ts = elem.get("data-timestamp", "")
            if ts:
                try:
                    date = datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
                    break
                except Exception:
                    pass

    return name, info_hash, magnet, size, date

# ─────────────────────────── 加载已爬取ID集合（断点续爬） ──────────
def load_existing_ids(out_file):
    """从CSV中读取已爬取的detail_url，提取ID集合用于跳过"""
    ids = set()
    if os.path.isfile(out_file):
        try:
            with open(out_file, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    url = row.get("detail_url", "")
                    m = re.search(r"/view/(\d+)", url)
                    if m:
                        ids.add(int(m.group(1)))
                    # 备用：也从 id 列读取
                    row_id = row.get("id", "")
                    if row_id:
                        try:
                            ids.add(int(row_id))
                        except ValueError:
                            pass
        except Exception:
            pass
    return ids

# ─────────────────────────── 主爬取逻辑 ─────────────────────────
def scrape(proxies, params):
    BASE = "https://nyaa.si"
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "Referer": BASE,
        "Accept-Language": "zh-CN,zh;q=0.9,ja;q=0.8,en;q=0.7"
    })
    if proxies:
        session.proxies.update(proxies)

    out_file = params["out"]
    is_resume = params.get("is_resume", False)
    fieldnames = ["id", "name", "info_hash", "magnet", "size", "date", "detail_url"]

    # ── 1. 检测连通性 ──
    print(c("\n正在测试连接...", "blue"))
    try:
        r = session.get(BASE, timeout=params["timeout"])
        r.raise_for_status()
        print(c(f"✔ 连接成功 (HTTP {r.status_code})", "green"))
    except Exception as e:
        print(c(f"✗ 连接失败: {e}", "red"))
        sys.exit(1)

    # ── 2. 确定ID范围 ──
    start_id = params["start_id"]
    end_id = params["end_id"]

    if end_id == "auto":
        end_id = detect_latest_id(session, BASE, timeout=params["timeout"])
    else:
        print(c(f"\n使用手动指定的结束ID: {end_id}", "yellow"))

    if start_id > end_id:
        print(c(f"\n⚠ 起始ID ({start_id}) 已超过结束ID ({end_id})，无需继续爬取", "yellow"))
        print(c(f"  上次已爬取到 {end_id}，数据已保存在 {out_file}", "green"))
        return

    total_range = end_id - start_id + 1
    print(c(f"\n[ 爬取范围 ]", "bold"))
    print(f"  起始ID: {start_id}")
    print(f"  结束ID: {end_id}")
    print(f"  本次待爬: {total_range:,} 个ID")
    if is_resume:
        print(c(f"  模式: 续爬（新数据将追加到 {out_file}）", "green"))
    print(f"  (注: 不是所有ID都存在，实际有效条目会少于该数字)")

    confirm = input(c("\n确认开始爬取? [Y/n]: ", "yellow")).strip().lower()
    if confirm == "n":
        print(c("已取消。", "yellow"))
        sys.exit(0)

    # ── 3. 断点续爬：加载已爬取的ID（去重保护） ──
    existing_ids = load_existing_ids(out_file)
    if existing_ids:
        skipped_count = len(existing_ids)
        print(c(f"\n✔ 去重保护: 已加载 {skipped_count} 条已有记录的ID，将自动跳过重复项", "green"))
    else:
        skipped_count = 0

    # ── 4. 开始遍历爬取 ──
    total_items = 0
    total_not_found = 0
    total_failed = 0
    consecutive_fail = 0
    start_time = time.time()

    file_exists = os.path.isfile(out_file)

    with open(out_file, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()

        current_id = start_id
        while current_id <= end_id:
            # 断点续爬：跳过已爬取的ID（去重保护）
            if current_id in existing_ids:
                current_id += 1
                continue

            detail_url = f"{BASE}/view/{current_id}"
            progress = current_id - start_id
            elapsed = time.time() - start_time
            speed = progress / elapsed if elapsed > 0 else 0
            eta_seconds = (end_id - current_id) / speed if speed > 0 else 0
            eta_str = f"{int(eta_seconds // 3600)}h{int((eta_seconds % 3600) // 60)}m" if eta_seconds < 86400 else f"{int(eta_seconds // 86400)}d{int((eta_seconds % 86400) // 3600)}h"

            sys.stdout.write(
                f"\r  [{progress:,}/{total_range:,}] "
                f"ID={current_id} | "
                f"有效={total_items} 不存在={total_not_found} 失败={total_failed} | "
                f"速度={speed:.0f}id/s ETA={eta_str}   "
            )
            sys.stdout.flush()

            try:
                r = request_with_retry(session, detail_url, timeout=params["timeout"], retry_count=params.get("retry_count", 3), allow_redirects=False)
                status = r.status_code

                # 处理重定向（通常表示ID不存在）
                if status in (301, 302, 303, 307, 308):
                    location = r.headers.get("Location", "")
                    # 如果重定向到首页或其他页面，说明该ID不存在
                    if "/view/" not in location:
                        total_not_found += 1
                        consecutive_fail += 1
                    else:
                        # 重定向到另一个 /view/ 页面，说明当前ID被合并或替换
                        total_not_found += 1
                        consecutive_fail += 1
                    current_id += 1
                    time.sleep(params["delay"] * 0.2)
                    continue

                if status == 404:
                    total_not_found += 1
                    consecutive_fail += 1
                    current_id += 1
                    time.sleep(params["delay"] * 0.2)
                    continue

                if status != 200:
                    total_failed += 1
                    consecutive_fail += 1
                    current_id += 1
                    time.sleep(params["delay"] * 0.5)
                    continue

                # 解析页面内容
                soup = BeautifulSoup(r.text, "html.parser")

                # 检查是否是有效的详情页（有torrent信息）
                # 如果页面没有 torrent 相关内容（如被重定向到首页），跳过
                kbd = soup.select_one("kbd")
                mag_a = soup.find("a", href=re.compile(r"^magnet:"))
                panel = soup.select_one("div.panel.panel-default")

                if not panel and not kbd and not mag_a:
                    # 可能不是有效详情页
                    total_not_found += 1
                    consecutive_fail += 1
                    current_id += 1
                    time.sleep(params["delay"] * 0.2)
                    continue

                # 有效条目，解析详情
                name, info_hash, magnet, size, date = parse_detail_page(soup, detail_url)

                writer.writerow({
                    "id": current_id,
                    "name": name,
                    "info_hash": info_hash,
                    "magnet": magnet,
                    "size": size,
                    "date": date,
                    "detail_url": detail_url,
                })
                f.flush()
                total_items += 1
                consecutive_fail = 0  # 重置连续失败计数

            except KeyboardInterrupt:
                print(c(f"\n\n⚠ 用户中断 (Ctrl+C)", "yellow"))
                print(c(f"  本次新增 {total_items} 条有效数据", "green"))
                print(c(f"  数据已保存到 {out_file}", "green"))
                print(c(f"  当前进度: ID {current_id} / {end_id}", "cyan"))
                print(c(f"  下次运行本脚本会自动检测该文件并从断点继续", "cyan"))
                break

            except requests.exceptions.ConnectionError as e:
                total_failed += 1
                consecutive_fail += 1
                print(c(f"\n  ✗ 连接错误 (ID {current_id}): {e}, 重试已耗尽", "red"))

            except requests.exceptions.Timeout:
                total_failed += 1
                consecutive_fail += 1
                print(c(f"\n  ✗ 请求超时 (ID {current_id}), 重试已耗尽", "red"))

            except Exception as e:
                total_failed += 1
                consecutive_fail += 1
                print(c(f"\n  ✗ 出错 (ID {current_id}): {e}", "red"))

            current_id += 1

            # 连续失败过多 → 暂停等待
            if consecutive_fail >= params["max_consecutive_fail"]:
                wait_sec = params["pause_wait"]
                print(c(f"\n\n⚠ 连续 {consecutive_fail} 次失败，暂停 {wait_sec} 秒后继续...", "yellow"), flush=True)
                time.sleep(wait_sec)
                consecutive_fail = 0
                print(c("  ✔ 恢复爬取", "green"))
                continue

            # 正常请求间隔
            time.sleep(params["delay"])

    # ── 5. 爬取完成统计 ──
    elapsed_total = time.time() - start_time
    hours = int(elapsed_total // 3600)
    mins = int((elapsed_total % 3600) // 60)
    secs = int(elapsed_total % 60)

    # 统计文件总记录数
    final_total = 0
    if os.path.isfile(out_file):
        try:
            with open(out_file, "r", encoding="utf-8-sig") as f:
                final_total = sum(1 for _ in csv.DictReader(f))
        except Exception:
            final_total = total_items

    mode_str = "续爬" if is_resume else "新建"
    print(c(f"""
╔════════════════════════════════════════════════╗
║  爬取完成！                                     ║
║  模式       : {mode_str:<36}║
║  本次新增   : {total_items:<36}║
║  文件总记录 : {final_total:<36}║
║  ID 范围   : {start_id} ~ {end_id:<24}║
║  不存在ID  : {total_not_found:<36}║
║  请求失败  : {total_failed:<36}║
║  去重跳过  : {skipped_count:<36}║
║  耗时      : {hours}h {mins}m {secs}s{' ' * (25 - len(f'{hours}h {mins}m {secs}s'))}║
║  输出文件  : {out_file[:30]:<34}║
╚════════════════════════════════════════════════╝
""", "green"))

# ─────────────────────────── 入口 ───────────────────────────────
def main():
    print_banner()
    proxies = setup_proxy()
    params  = setup_params()
    print(c("\n配置完成，开始爬取...\n", "bold"))
    scrape(proxies, params)

if __name__ == "__main__":
    main()
