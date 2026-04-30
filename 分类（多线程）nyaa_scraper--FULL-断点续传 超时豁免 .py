#!/usr/bin/env python3
"""
nyaa.si 全量元数据爬虫  v3.1 多线程并行版（自适应速率+断点续爬）
策略变更：废弃列表页翻页，改为直接遍历 /view/{id} 链接爬取全站数据
自动检测最新条目ID，从最早(15)遍历到最新，支持断点续爬
爬取资源名称、信息哈希值、Magnet链接、资源大小、分类，存储为 CSV
支持交互式配置代理（HTTP/HTTPS/SOCKS5）

v2.1 新增：启动时自动扫描运行目录下已有的 CSV 文件，检测爬取进度
v3.0 新增：多线程并行爬取
v3.1 修复：
        - 【致命bug】"不存在"不再计入连续失败计数，避免线程误暂停
        - 新增全局令牌桶速率限制器，精确控制总体 QPS
        - 新增自适应退避：失败率飙升时自动降速，恢复后自动加速
        - 新增请求间隔随机抖动 (jitter)，避免同步请求触发反爬
        - 新增 User-Agent 随机轮换
        - 新增 HTTP 429/503 专用的指数退避处理
        - 修复连续失败只暂停当前线程、不影响其他线程
        - 优化默认参数（更保守，更稳定）
"""
import requests
from bs4 import BeautifulSoup
import csv
import time
import sys
import os
import re
import glob
import threading
import queue
import base64
import random
from datetime import datetime
from urllib.parse import urljoin

# ─────────────────────────── UA 池 ─────────────────────────────
UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

# ─────────────────────────── 颜色输出 ───────────────────────────
def c(text, color):
    codes = {"red": "\033[91m", "green": "\033[92m", "yellow": "\033[93m",
             "blue": "\033[94m", "cyan": "\033[96m", "bold": "\033[1m", "reset": "\033[0m"}
    return f"{codes.get(color,'')}{text}{codes['reset']}"

# ─────────────────────────── Banner ─────────────────────────────
def print_banner():
    print(c("""
╔══════════════════════════════════════════════════════════════╗
║     nyaa.si 全量元数据爬虫  v3.1 多线程自适应版              ║
║  策略: 自动检测最新ID → /view/{id} 直连遍历 → 全站爬取      ║
║  爬取: 资源名称 | InfoHash | Magnet | 大小 | 日期 | 分类        ║
║  支持: 多线程 | 全局限速 | 自适应退避 | 断点续爬 | 代理      ║
╚══════════════════════════════════════════════════════════════╝
""", "cyan"))

# ─────────────────────────── 自动检测断点续爬 ───────────────────────
def auto_detect_resume(csv_pattern):
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
    DEFAULT_START_ID = 15
    CSV_PATTERN = "nyaa_data_*.csv"
    CSV_PREFIX = "nyaa_data"
    print(c("\n[ 爬取参数配置 ]", "bold"))

    candidates = auto_detect_resume(CSV_PATTERN)
    resume_file = None
    resume_start = None
    if candidates:
        resume_file, resume_start = setup_resume(candidates, DEFAULT_START_ID)

    if resume_file:
        start_id_input = input(c(f"起始条目ID [默认 {resume_start}，从断点继续]: ", "yellow")).strip()
        start_id = int(start_id_input) if start_id_input.isdigit() else resume_start
        out_file_input = input(c(f"输出文件名 [默认 {resume_file}，续爬合并]: ", "yellow")).strip()
        out_file = out_file_input if out_file_input else resume_file
    else:
        start_id = input(c(f"起始条目ID [默认 {DEFAULT_START_ID}]: ", "yellow")).strip()
        start_id = int(start_id) if start_id.isdigit() else DEFAULT_START_ID
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_out = f"{CSV_PREFIX}_{ts}.csv"
        out_file = input(c(f"输出文件名 [默认 {default_out}]: ", "yellow")).strip() or default_out

    end_id_input = input(c("结束条目ID [默认 auto=自动检测最新]: ", "yellow")).strip().lower()
    end_id = "auto"
    if end_id_input and end_id_input != "auto":
        end_id = int(end_id_input) if end_id_input.isdigit() else "auto"

    # 线程数（降低默认值）
    threads_input = input(c("并行线程数 [默认 8]: ", "yellow")).strip()
    num_threads = int(threads_input) if threads_input.isdigit() and int(threads_input) > 0 else 8

    # 全局最大QPS（每秒最大请求数，所有线程共享）
    qps_input = input(c("全局最大请求速率 QPS [默认 10]: ", "yellow")).strip()
    max_qps = float(qps_input) if qps_input.strip() else 10.0

    # 超时时间
    timeout_input = input(c("请求超时时间（秒）[默认 30]: ", "yellow")).strip()
    try:
        timeout_val = float(timeout_input)
    except Exception:
        timeout_val = 30

    # 重试次数
    retry_input = input(c("超时/连接错误重试次数 [默认 3]: ", "yellow")).strip()
    retry_count = int(retry_input) if retry_input.isdigit() else 3

    return {
        "start_id": start_id,
        "end_id": end_id,
        "timeout": timeout_val,
        "retry_count": retry_count,
        "out": out_file,
        "is_resume": resume_file is not None,
        "num_threads": num_threads,
        "max_qps": max_qps,
    }

# ─────────────────────────── 带重试的HTTP请求 ──────────────────────
def request_with_retry(session, url, timeout=30, retry_count=3, **kwargs):
    """带指数退避重试的HTTP GET请求"""
    last_error = None
    for attempt in range(retry_count + 1):
        try:
            r = session.get(url, timeout=timeout, **kwargs)
            # HTTP 429 Too Many Requests → 指数退避
            if r.status_code == 429:
                wait = 10 * (2 ** attempt) + random.uniform(0, 5)
                err = f"HTTP 429 被限流, 退避{wait:.1f}s"
                print(c(f"\n  ⚠ [{threading.current_thread().name}] {err}, 重试{attempt+1}/{retry_count}", "yellow"), flush=True)
                time.sleep(wait)
                continue
            # HTTP 503 Service Unavailable → 指数退避
            if r.status_code == 503:
                wait = 8 * (2 ** attempt) + random.uniform(0, 3)
                print(c(f"\n  ⚠ [{threading.current_thread().name}] HTTP 503 服务不可用, 退避{wait:.1f}s, 重试{attempt+1}/{retry_count}", "yellow"), flush=True)
                time.sleep(wait)
                continue
            return r
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_error = e
            if attempt < retry_count:
                wait_backoff = 3 * (2 ** attempt) + random.uniform(0, 2)
                err_type = "超时" if isinstance(e, requests.exceptions.Timeout) else "连接错误"
                print(c(f"\n  ⚠ [{threading.current_thread().name}] {err_type}, 退避{wait_backoff:.1f}s, 重试{attempt+1}/{retry_count}", "yellow"), flush=True)
                time.sleep(wait_backoff)
    raise last_error

# ─────────────────────────── 自动检测最新条目ID ──────────────────
def detect_latest_id(session, base_url, timeout=30):
    print(c("\n[ 自动检测最新条目ID ]", "bold"))
    try:
        r = session.get(base_url, params={"s": "id", "o": "desc"}, timeout=timeout)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        rows = soup.select("table.torrent-list tbody tr")
        if rows:
            first_row = rows[0]
            links = first_row.select("td:nth-child(2) a:not(.comments)")
            if links:
                href = links[-1].get("href", "")
                m = re.search(r"/view/(\d+)", href)
                if m:
                    latest_id = int(m.group(1))
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
        print(c("⚠ 首页解析失败，尝试备用检测方法...", "yellow"))
        test_id = 5000000
        latest = None
        for _ in range(20):
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
    name = ""
    info_hash = ""
    magnet = ""
    size = ""
    date = ""
    category = ""

    title_tag = soup.find("title")
    if title_tag:
        title_text = title_tag.get_text(strip=True)
        name = re.sub(r"\s*\|.*$", "", title_text).strip()
    if not name:
        h3 = soup.select_one("h3.panel-title")
        if h3:
            name = h3.get_text(strip=True)
    if not name:
        og_title = soup.find("meta", property="og:title")
        if og_title and og_title.get("content"):
            name = og_title["content"].strip()

    kbd = soup.select_one("kbd")
    if kbd:
        info_hash = kbd.get_text(strip=True)

    mag_a = soup.find("a", href=re.compile(r"^magnet:"))
    if mag_a:
        magnet = mag_a["href"]

    if not info_hash and magnet:
        m = re.search(r"xt=urn:btih:([a-fA-F0-9]{40})", magnet)
        if m:
            info_hash = m.group(1).lower()
        else:
            m = re.search(r"xt=urn:btih:([A-Z2-7]{32})", magnet)
            if m:
                try:
                    b32 = m.group(1)
                    info_hash = base64.b16encode(base64.b32decode(b32)).decode().lower()
                except Exception:
                    pass

    rows_divs = soup.select("div.panel-body div.row")
    for row_div in rows_divs:
        text = row_div.get_text(strip=True)
        if not size:
            size_match = re.search(r"((?:\d+\.?\d*)\s*(?:Bytes?|KB|MB|GB|TB|PiB))", text, re.IGNORECASE)
            if size_match:
                if "Total" in text or "总" in text or "Size" in text or "大小" in text or "File size" in text.lower():
                    size = size_match.group(1).strip()
                elif not re.search(r"\.torrent", text, re.IGNORECASE):
                    size = size_match.group(1).strip()
        if not date:
            date_match = re.search(r"(\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}:\d{2})?)", text)
            if date_match:
                date = date_match.group(1)
            else:
                ts_elem = row_div.select_one("[data-timestamp]")
                if ts_elem:
                    ts = ts_elem.get("data-timestamp", "")
                    if ts:
                        try:
                            date = datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
                        except Exception:
                            pass

    if not size:
        body = soup.select_one("div.panel-body")
        if body:
            body_text = body.get_text()
            size_match = re.search(r"(?:Total\s+size|文件大小|总大小)[:\s]*((?:\d+\.?\d*)\s*(?:Bytes?|KB|MB|GB|TB|PiB))", body_text, re.IGNORECASE)
            if size_match:
                size = size_match.group(1).strip()
            else:
                all_sizes = re.findall(r"((?:\d+\.?\d*)\s*(?:Bytes?|KB|MB|GB|TB|PiB))", body_text, re.IGNORECASE)
                if all_sizes:
                    for s in reversed(all_sizes):
                        if re.search(r"(TB|GB|PiB)", s, re.IGNORECASE):
                            size = s.strip()
                            break
                    if not size:
                        size = all_sizes[-1].strip()

    if not date:
        for elem in soup.select("[data-timestamp]"):
            ts = elem.get("data-timestamp", "")
            if ts:
                try:
                    date = datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
                    break
                except Exception:
                    pass

    # ── 解析 Category ──
    if not category:
        for row_div in rows_divs:
            label_div = row_div.find("div", class_="col-md-1")
            if label_div and "category" in label_div.get_text(strip=True).lower():
                value_div = row_div.find("div", class_="col-md-5")
                if value_div:
                    # 提取所有链接文本并用 " - " 连接（如 Literature - Raw）
                    cat_links = value_div.find_all("a")
                    if cat_links:
                        category = " - ".join(a.get_text(strip=True) for a in cat_links)
                    else:
                        category = value_div.get_text(strip=True)
                break

    return name, info_hash, magnet, size, date, category

# ─────────────────────────── 加载已爬取ID集合 ──────────────────
def load_existing_ids(out_file):
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
                    row_id = row.get("id", "")
                    if row_id:
                        try:
                            ids.add(int(row_id))
                        except ValueError:
                            pass
        except Exception:
            pass
    return ids

# ═══════════════════════════════════════════════════════════════
#  多线程核心组件
# ═══════════════════════════════════════════════════════════════

class GlobalRateLimiter:
    """令牌桶式全局速率限制器 —— 精确控制所有线程的总 QPS"""

    def __init__(self, max_qps):
        self.max_qps = max_qps
        self.min_interval = 1.0 / max_qps if max_qps > 0 else 0
        self.lock = threading.Lock()
        self.last_request_time = 0.0

    def acquire(self):
        """阻塞直到允许发送下一个请求（带随机抖动）"""
        while True:
            with self.lock:
                now = time.monotonic()
                wait = self.min_interval - (now - self.last_request_time)
                if wait <= 0:
                    self.last_request_time = now
                    return
            # 在锁外等待，加入 0~20% 随机抖动避免多线程同步
            jitter = wait * random.uniform(0, 0.2)
            time.sleep(wait + jitter)

    def update_qps(self, new_qps):
        """动态调整 QPS"""
        with self.lock:
            self.max_qps = max(0.5, new_qps)
            self.min_interval = 1.0 / self.max_qps


class ScraperState:
    """线程安全的共享爬取状态"""

    def __init__(self, start_id, end_id, existing_ids):
        self.lock = threading.RLock()  # 使用可重入锁，避免 get_stats() → get_failure_rate() 嵌套加锁死锁
        self.next_id = start_id
        self.end_id = end_id
        self.existing_ids = existing_ids
        self.processed = 0
        self.total_items = 0
        self.total_not_found = 0
        self.total_failed = 0
        self.active_threads = 0
        self.start_time = time.time()
        self.stop_event = threading.Event()
        # 用于自适应速率：滑动窗口统计失败率
        self._recent_results = []  # (timestamp, is_success)
        self._window_size = 200     # 滑动窗口大小

    def get_next_id(self):
        with self.lock:
            while self.next_id <= self.end_id:
                current = self.next_id
                self.next_id += 1
                if current not in self.existing_ids:
                    self.processed += 1
                    return current
                else:
                    continue
            return None

    def record_result(self, is_success):
        """记录一次请求结果，用于自适应速率调整"""
        with self.lock:
            if is_success:
                self.total_items += 1
            self._recent_results.append((time.time(), is_success))
            # 清理超过窗口大小的旧记录
            cutoff = time.time() - 60  # 保留最近 60 秒
            self._recent_results = [
                (ts, ok) for ts, ok in self._recent_results if ts > cutoff
            ]

    def record_not_found(self):
        with self.lock:
            self.total_not_found += 1
            self._recent_results.append((time.time(), True))  # 404 = 正常，不算失败
            cutoff = time.time() - 60
            self._recent_results = [
                (ts, ok) for ts, ok in self._recent_results if ts > cutoff
            ]

    def record_failed(self):
        with self.lock:
            self.total_failed += 1
            self._recent_results.append((time.time(), False))
            cutoff = time.time() - 60
            self._recent_results = [
                (ts, ok) for ts, ok in self._recent_results if ts > cutoff
            ]

    def get_failure_rate(self):
        """获取最近 60 秒的失败率 (0.0 ~ 1.0)"""
        with self.lock:
            if not self._recent_results:
                return 0.0
            failures = sum(1 for _, ok in self._recent_results if not ok)
            return failures / len(self._recent_results)

    def get_stats(self):
        with self.lock:
            elapsed = time.time() - self.start_time
            speed = self.processed / elapsed if elapsed > 0 else 0
            return {
                "processed": self.processed,
                "total_items": self.total_items,
                "total_not_found": self.total_not_found,
                "total_failed": self.total_failed,
                "speed": speed,
                "elapsed": elapsed,
                "active_threads": self.active_threads,
                "failure_rate": self.get_failure_rate(),
                "current_qps": 0,  # 由外部填入
            }

    def set_active_threads(self, count):
        with self.lock:
            self.active_threads = count


def create_session(base_url, proxies):
    """为每个线程创建独立的 requests.Session"""
    session = requests.Session()
    ua = random.choice(UA_POOL)
    session.headers.update({
        "User-Agent": ua,
        "Referer": base_url,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,ja;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    })
    if proxies:
        session.proxies.update(proxies)
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=8,
        pool_maxsize=8,
        max_retries=0
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def writer_thread(out_file, fieldnames, result_queue, state):
    """专用写入线程"""
    file_exists = os.path.isfile(out_file)
    with open(out_file, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        while True:
            try:
                row = result_queue.get(timeout=1.0)
                if row is None:
                    result_queue.task_done()
                    break
                writer.writerow(row)
                f.flush()
                result_queue.task_done()
            except queue.Empty:
                if state.active_threads <= 0:
                    break


def progress_display_thread(state, total_range, rate_limiter):
    """进度显示 + 自适应速率调整线程"""
    adaptive_check_interval = 5  # 每 5 秒检查一次
    last_adaptive_check = time.time()

    while not state.stop_event.is_set():
        stats = state.get_stats()
        stats["current_qps"] = rate_limiter.max_qps

        elapsed = stats["elapsed"]
        speed = stats["speed"]
        remaining = total_range - stats["processed"]
        eta_seconds = remaining / speed if speed > 0 else 0

        if eta_seconds < 3600:
            eta_str = f"{int(eta_seconds // 60)}m{int(eta_seconds % 60)}s"
        elif eta_seconds < 86400:
            eta_str = f"{int(eta_seconds // 3600)}h{int((eta_seconds % 3600) // 60)}m"
        else:
            eta_str = f"{int(eta_seconds // 86400)}d{int((eta_seconds % 86400) // 3600)}h"

        fail_pct = stats["failure_rate"] * 100

        progress_line = (
            f"  [{stats['processed']:,}/{total_range:,}] "
            f"有效={stats['total_items']} 不存在={stats['total_not_found']} 失败={stats['total_failed']} | "
            f"线程={stats['active_threads']} QPS={rate_limiter.max_qps:.1f} "
            f"失败率={fail_pct:.0f}% 速度={speed:.1f}id/s ETA={eta_str}"
        )
        # 使用 \r + 填充空白覆盖旧内容，确保终端兼容
        sys.stdout.write(f"\r{progress_line:<100}")
        sys.stdout.flush()

        # ── 自适应速率调整 ──
        now = time.time()
        if now - last_adaptive_check >= adaptive_check_interval:
            last_adaptive_check = now
            fr = stats["failure_rate"]

            if fr > 0.5:
                # 失败率 > 50% → 大幅降速
                new_qps = max(1.0, rate_limiter.max_qps * 0.5)
                if new_qps != rate_limiter.max_qps:
                    rate_limiter.update_qps(new_qps)
                    print(c(f"\n  ⚡ 自适应: 失败率 {fr*100:.0f}% > 50%, 降速至 {new_qps:.1f} QPS", "yellow"), flush=True)
            elif fr > 0.2:
                # 失败率 > 20% → 小幅降速
                new_qps = max(1.0, rate_limiter.max_qps * 0.7)
                if new_qps != rate_limiter.max_qps:
                    rate_limiter.update_qps(new_qps)
                    print(c(f"\n  ⚡ 自适应: 失败率 {fr*100:.0f}% > 20%, 降速至 {new_qps:.1f} QPS", "yellow"), flush=True)
            elif fr < 0.05:
                # 失败率 < 5% 且当前 QPS 较低 → 逐步提速
                original_qps = params_ref[0].get("_original_qps", 10.0)
                if rate_limiter.max_qps < original_qps:
                    new_qps = min(original_qps, rate_limiter.max_qps * 1.3)
                    rate_limiter.update_qps(new_qps)
                    print(c(f"\n  ⚡ 自适应: 失败率 {fr*100:.0f}% < 5%, 提速至 {new_qps:.1f} QPS", "green"), flush=True)

        if state.stop_event.wait(0.5):
            break


# ─────────────────────────── 主爬取逻辑（多线程） ─────────────────
def scrape(proxies, params):
    global params_ref
    params_ref = [params]  # 用于自适应线程访问原始QPS

    BASE = "https://nyaa.si"
    tmp_session = create_session(BASE, proxies)
    out_file = params["out"]
    is_resume = params.get("is_resume", False)
    fieldnames = ["id", "name", "info_hash", "magnet", "size", "date", "category", "detail_url"]
    num_threads = params.get("num_threads", 8)
    max_qps = params.get("max_qps", 10.0)
    params["_original_qps"] = max_qps  # 保存原始QPS用于恢复

    # ── 1. 检测连通性 ──
    print(c("\n正在测试连接...", "blue"))
    try:
        r = tmp_session.get(BASE, timeout=params["timeout"])
        r.raise_for_status()
        print(c(f"✔ 连接成功 (HTTP {r.status_code})", "green"))
    except Exception as e:
        print(c(f"✗ 连接失败: {e}", "red"))
        sys.exit(1)

    # ── 2. 确定ID范围 ──
    start_id = params["start_id"]
    end_id = params["end_id"]
    if end_id == "auto":
        end_id = detect_latest_id(tmp_session, BASE, timeout=params["timeout"])
    else:
        print(c(f"\n使用手动指定的结束ID: {end_id}", "yellow"))
    tmp_session.close()

    if start_id > end_id:
        print(c(f"\n⚠ 起始ID ({start_id}) 已超过结束ID ({end_id})，无需继续爬取", "yellow"))
        print(c(f"  上次已爬取到 {end_id}，数据已保存在 {out_file}", "green"))
        return

    total_range = end_id - start_id + 1
    print(c(f"\n[ 爬取范围 ]", "bold"))
    print(f"  起始ID: {start_id}")
    print(f"  结束ID: {end_id}")
    print(f"  本次待爬: {total_range:,} 个ID")
    print(f"  并行线程: {num_threads}")
    print(f"  全局QPS: {max_qps} (自适应调节)")
    if is_resume:
        print(c(f"  模式: 续爬（新数据将追加到 {out_file}）", "green"))
    print(f"  (注: 不是所有ID都存在，实际有效条目会少于该数字)")

    confirm = input(c("\n确认开始爬取? [Y/n]: ", "yellow")).strip().lower()
    if confirm == "n":
        print(c("已取消。", "yellow"))
        sys.exit(0)

    # ── 3. 断点续爬 ──
    existing_ids = load_existing_ids(out_file)
    if existing_ids:
        skipped_count = len(existing_ids)
        print(c(f"\n✔ 去重保护: 已加载 {skipped_count} 条已有记录的ID，将自动跳过重复项", "green"))
    else:
        skipped_count = 0

    # ── 4. 初始化 ──
    state = ScraperState(start_id, end_id, existing_ids)
    rate_limiter = GlobalRateLimiter(max_qps)
    result_queue = queue.Queue(maxsize=num_threads * 4)

    ids_to_scrape = total_range - len(existing_ids.intersection(range(start_id, end_id + 1)))
    print(c(f"\n✔ 需要实际爬取约 {ids_to_scrape:,} 个ID（已去重）", "cyan"))
    print(c(f"✔ 启动 {num_threads} 个工作线程 + 全局 {max_qps:.0f} QPS 限速...", "cyan"))
    print(c(f"✔ 自适应速率已启用: 失败率高→自动降速, 失败率低→自动提速", "cyan"))
    print()

    # ── 5. 写入线程 ──
    writer = threading.Thread(
        target=writer_thread,
        args=(out_file, fieldnames, result_queue, state),
        name="Writer", daemon=True
    )
    writer.start()

    # ── 6. 进度 + 自适应线程 ──
    progress = threading.Thread(
        target=progress_display_thread,
        args=(state, total_range, rate_limiter),
        name="Progress", daemon=True
    )
    progress.start()

    # ── 7. 工作线程 ──
    state.set_active_threads(num_threads)
    timeout = params["timeout"]
    retry_count = params["retry_count"]

    def thread_target(s):
        """工作线程：从 ID 分配器取任务，通过全局限速器发送请求"""
        # 本线程独立的失败计数（只计真正的失败，不计"不存在"）
        consecutive_real_fail = 0

        while not state.stop_event.is_set():
            target_id = state.get_next_id()
            if target_id is None:
                break

            detail_url = f"{BASE}/view/{target_id}"

            # ✅ 全局速率限制（所有线程共享）
            rate_limiter.acquire()

            # 随机轮换 User-Agent
            s.headers["User-Agent"] = random.choice(UA_POOL)

            try:
                r = request_with_retry(s, detail_url, timeout=timeout,
                                       retry_count=retry_count, allow_redirects=False)
                status = r.status_code

                # ─── 不存在的ID（404 / 重定向）───
                if status in (301, 302, 303, 307, 308, 404):
                    state.record_not_found()
                    # ✅ FIX: 不存在 ≠ 失败，重置连续失败计数
                    consecutive_real_fail = 0
                    continue

                # ─── 非预期的状态码（服务器错误等）───
                if status != 200:
                    state.record_failed()
                    state.record_result(False)
                    consecutive_real_fail += 1
                    if consecutive_real_fail >= 20:
                        print(c(f"\n  ⚠ [{threading.current_thread().name}] 连续 {consecutive_real_fail} 次真正失败，暂停 60s...", "yellow"), flush=True)
                        time.sleep(60)
                        consecutive_real_fail = 0
                    continue

                # ─── 页面内容检查 ───
                soup = BeautifulSoup(r.text, "html.parser")
                kbd = soup.select_one("kbd")
                mag_a = soup.find("a", href=re.compile(r"^magnet:"))
                panel = soup.select_one("div.panel.panel-default")

                if not panel and not kbd and not mag_a:
                    state.record_not_found()
                    # 页面无内容 = 空号，不算失败
                    consecutive_real_fail = 0
                    continue

                # ─── 有效条目 ───
                name, info_hash, magnet, size, date, category = parse_detail_page(soup, detail_url)
                row = {
                    "id": target_id, "name": name, "info_hash": info_hash,
                    "magnet": magnet, "size": size, "date": date,
                    "category": category, "detail_url": detail_url,
                }
                result_queue.put(row)
                state.record_result(True)
                consecutive_real_fail = 0  # 成功，重置

            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                state.record_failed()
                state.record_result(False)
                consecutive_real_fail += 1
                err_type = "连接错误" if isinstance(e, requests.exceptions.ConnectionError) else "超时"
                print(c(f"\n  ✗ [{threading.current_thread().name}] {err_type} (ID {target_id}), 连续失败={consecutive_real_fail}", "red"), flush=True)
                if consecutive_real_fail >= 20:
                    print(c(f"\n  ⚠ [{threading.current_thread().name}] 连续 {consecutive_real_fail} 次失败，暂停 60s...", "yellow"), flush=True)
                    time.sleep(60)
                    consecutive_real_fail = 0

            except Exception as e:
                state.record_failed()
                state.record_result(False)
                consecutive_real_fail += 1
                print(c(f"\n  ✗ [{threading.current_thread().name}] 出错 (ID {target_id}): {e}", "red"), flush=True)
                if consecutive_real_fail >= 20:
                    print(c(f"\n  ⚠ [{threading.current_thread().name}] 连续 {consecutive_real_fail} 次失败，暂停 60s...", "yellow"), flush=True)
                    time.sleep(60)
                    consecutive_real_fail = 0

        s.close()

    # 创建并启动所有工作线程
    threads = []
    try:
        for i in range(num_threads):
            sess = create_session(BASE, proxies)
            t = threading.Thread(target=thread_target, args=(sess,), name=f"Worker-{i+1:02d}")
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        print(c(f"\n\n⚠ 用户中断 (Ctrl+C)，正在优雅停止...", "yellow"), flush=True)
        state.stop_event.set()

    state.set_active_threads(0)
    result_queue.join()
    result_queue.put(None)
    writer.join(timeout=5)
    state.stop_event.set()
    progress.join(timeout=2)

    # ── 8. 最终统计 ──
    final_stats = state.get_stats()
    elapsed_total = final_stats["elapsed"]
    hours = int(elapsed_total // 3600)
    mins = int((elapsed_total % 3600) // 60)
    secs = int(elapsed_total % 60)

    final_total = 0
    if os.path.isfile(out_file):
        try:
            with open(out_file, "r", encoding="utf-8-sig") as f:
                final_total = sum(1 for _ in csv.DictReader(f))
        except Exception:
            final_total = final_stats["total_items"]

    mode_str = "续爬" if is_resume else "新建"
    speed_str = f"{final_stats['speed']:.1f} id/s"
    time_str = f"{hours}h {mins}m {secs}s"

    sys.stdout.write("\n")
    sys.stdout.flush()

    print(c(f"""
╔══════════════════════════════════════════════════════════════╗
║  爬取完成！                                                  ║
║  模式       : {mode_str:<36}║
║  线程数     : {num_threads:<36}║
║  本次新增   : {final_stats['total_items']:<36}║
║  文件总记录 : {final_total:<36}║
║  ID 范围   : {start_id} ~ {end_id:<24}║
║  不存在ID  : {final_stats['total_not_found']:<36}║
║  请求失败  : {final_stats['total_failed']:<36}║
║  去重跳过  : {skipped_count:<36}║
║  平均速度  : {speed_str:<36}║
║  耗时      : {time_str:<36}║
║  输出文件  : {out_file[:36]:<34}║
╚══════════════════════════════════════════════════════════════╝
""", "green"))

    if state.stop_event.is_set():
        print(c(f"  当前进度: 已处理 {final_stats['processed']:,} / {total_range:,}", "cyan"))
        print(c(f"  下次运行本脚本会自动检测该文件并从断点继续", "cyan"))


# ─────────────────────────── 入口 ───────────────────────────────
params_ref = [{}]

def main():
    print_banner()
    proxies = setup_proxy()
    params  = setup_params()
    print(c("\n配置完成，开始爬取...\n", "bold"))
    scrape(proxies, params)

if __name__ == "__main__":
    main()
