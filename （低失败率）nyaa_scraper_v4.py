#!/usr/bin/env python3
"""
nyaa.si 全量元数据爬虫  v4.0 零失败版（多线程+无限重试+失败重做）

═══════════════════════════════════════════════════════════════
  核心设计哲学: 任何 ID 都不应失败，只有"尚未成功"
═══════════════════════════════════════════════════════════════

v4.0 重大改进（零失败架构）:
  1. 【核心】失败ID重试队列 — 任何失败的ID不会被丢弃，全部进入重试队列
  2. 【核心】Session自动重建 — 连接池损坏后自动创建新Session，不再卡死
  3. 【核心】全异常捕获 — 捕获 requests 库所有可能的异常子类
  4. 【核心】全状态码重试 — 5xx/429/503/502/504/403 全部自动重试
  5. 【传输层】urllib3.Retry — socket/连接级别的底层自动重连
  6. 【架构】最终重做轮次 — 主爬取结束后自动对所有失败ID做最终清扫
  7. 【架构】全局熔断器 — 检测到服务端严重故障时全线程暂停等待
  8. 【优化】退避上限 — 最大退避120s，避免天文数字等待
  9. 【优化】失败ID持久化 — 进程崩溃后下次启动可从failed_ids.txt恢复

策略: 自动检测最新ID → /view/{id} 直连遍历 → 全站爬取
爬取: 资源名称 | InfoHash | Magnet | 大小 | 日期 | 分类
支持: 多线程 | 全局限速 | 自适应退避 | 断点续爬 | 代理 | 零失败保证
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0",
]

# ─────────────────────────── 常量定义 ───────────────────────────
# 永久失败状态码（不应重试的）
PERMANENT_STATUS_CODES = {400, 401, 405, 415, 422}

# 临时性状态码（应该重试的）
RETRY_STATUS_CODES = {
    429,  # Too Many Requests
    500,  # Internal Server Error
    502,  # Bad Gateway
    503,  # Service Unavailable
    504,  # Gateway Timeout
    403,  # Forbidden（可能是临时限制）
}

# 不存在的状态码
NOT_FOUND_STATUS_CODES = {301, 302, 303, 307, 308, 404}

# 最大退避时间上限（秒）
MAX_BACKOFF = 120

# Session重建阈值：连续失败多少次后重建Session
SESSION_REBUILD_THRESHOLD = 5

# 熔断器阈值：全局连续失败多少次触发熔断
CIRCUIT_BREAKER_THRESHOLD = 50

# 熔断器恢复等待时间（秒）
CIRCUIT_BREAKER_WAIT = 120

# 失败ID持久化文件
FAILED_IDS_FILE = "nyaa_failed_ids.txt"


# ─────────────────────────── 颜色输出 ───────────────────────────
def c(text, color):
    codes = {
        "red": "\033[91m", "green": "\033[92m", "yellow": "\033[93m",
        "blue": "\033[94m", "cyan": "\033[96m", "bold": "\033[1m",
        "reset": "\033[0m", "magenta": "\033[95m",
    }
    return f"{codes.get(color, '')}{text}{codes['reset']}"


# ─────────────────────────── Banner ─────────────────────────────
def print_banner():
    print(c("""
╔══════════════════════════════════════════════════════════════════╗
║     nyaa.si 全量元数据爬虫  v4.0 零失败版（多线程自适应）        ║
║                                                                  ║
║  核心: 任何ID都不会失败，只有"尚未成功"                           ║
║  保证: 失败ID重试队列 + Session自动重建 + 全异常捕获              ║
║  爬取: 资源名称 | InfoHash | Magnet | 大小 | 日期 | 分类          ║
║  支持: 多线程 | 全局限速 | 自适应退避 | 断点续爬 | 代理            ║
╚══════════════════════════════════════════════════════════════════╝
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
            "http": f"http://{host}:{port}",
            "https": f"http://{host}:{port}",
        }
        print(c(f"✔ 已设置 HTTP 代理: {host}:{port}", "green"))
    elif choice == "3":
        host = input("SOCKS5 代理主机 (如 127.0.0.1): ").strip()
        port = input("SOCKS5 代理端口 (如 1080): ").strip()
        user = input("用户名 (无则回车): ").strip()
        pwd = input("密码   (无则回车): ").strip()
        if user and pwd:
            proxies = {
                "http": f"socks5://{user}:{pwd}@{host}:{port}",
                "https": f"socks5://{user}:{pwd}@{host}:{port}",
            }
        else:
            proxies = {
                "http": f"socks5://{host}:{port}",
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

    threads_input = input(c("并行线程数 [默认 8]: ", "yellow")).strip()
    num_threads = int(threads_input) if threads_input.isdigit() and int(threads_input) > 0 else 8

    qps_input = input(c("全局最大请求速率 QPS [默认 10]: ", "yellow")).strip()
    max_qps = float(qps_input) if qps_input.strip() else 10.0

    timeout_input = input(c("请求超时时间（秒）[默认 60]: ", "yellow")).strip()
    try:
        timeout_val = float(timeout_input)
        if timeout_val < 10:
            timeout_val = 10  # 最低10秒，避免过短超时
    except Exception:
        timeout_val = 60

    retry_input = input(c("单次请求最大重试次数 [默认 50，0=无限重试直到成功]: ", "yellow")).strip()
    if retry_input.strip() == "0":
        retry_count = float("inf")  # 无限重试
    else:
        retry_count = int(retry_input) if retry_input.isdigit() else 50

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


# ═══════════════════════════════════════════════════════════════
#  核心组件：零失败重试引擎
# ═══════════════════════════════════════════════════════════════

class SessionManager:
    """线程安全的 Session 管理器，支持自动重建损坏的 Session"""

    def __init__(self, base_url, proxies, timeout=60):
        self.base_url = base_url
        self.proxies = proxies
        self.timeout = timeout
        self.lock = threading.Lock()
        self._sessions = {}  # thread_id -> session
        self._fail_counts = {}  # thread_id -> consecutive_fail_count

    def get_session(self):
        """获取当前线程的 Session，如果连续失败过多则自动重建"""
        tid = threading.get_ident()
        with self.lock:
            fail_count = self._fail_counts.get(tid, 0)
            if fail_count >= SESSION_REBUILD_THRESHOLD:
                # 重建 Session
                old_session = self._sessions.get(tid)
                if old_session:
                    try:
                        old_session.close()
                    except Exception:
                        pass
                    del self._sessions[tid]
                self._fail_counts[tid] = 0
                print(c(f"  🔄 [Thread-{tid}] Session已重建（连续失败{fail_count}次）", "magenta"), flush=True)

            if tid not in self._sessions:
                self._sessions[tid] = self._create_session()
                self._fail_counts[tid] = 0

            return self._sessions[tid]

    def record_success(self):
        """记录成功，重置失败计数"""
        tid = threading.get_ident()
        with self.lock:
            self._fail_counts[tid] = 0

    def record_failure(self):
        """记录失败，增加失败计数"""
        tid = threading.get_ident()
        with self.lock:
            self._fail_counts[tid] = self._fail_counts.get(tid, 0) + 1

    def _create_session(self):
        """创建新 Session，配置 urllib3 传输层自动重试"""
        session = requests.Session()
        ua = random.choice(UA_POOL)
        session.headers.update({
            "User-Agent": ua,
            "Referer": self.base_url,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,ja;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache",
        })
        if self.proxies:
            session.proxies.update(self.proxies)

        # ★ 核心：urllib3 传输层自动重试
        # 处理 socket 错误、连接重置、DNS 解析失败等底层问题
        retry_strategy = Retry(
            total=5,                    # 每个请求底层最多重试5次
            backoff_factor=2,            # 退避因子：0, 2, 4, 8, 16 秒
            status_forcelist=list(RETRY_STATUS_CODES),  # 这些状态码自动重试
            allowed_methods=["GET", "HEAD"],  # 只对安全方法重试
            raise_on_status=False,       # 不抛异常，返回响应让上层处理
            respect_retry_after_header=True,  # 尊重 Retry-After 头
        )
        adapter = HTTPAdapter(
            pool_connections=16,
            pool_maxsize=16,
            max_retries=retry_strategy,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def close_all(self):
        with self.lock:
            for session in self._sessions.values():
                try:
                    session.close()
                except Exception:
                    pass
            self._sessions.clear()


def request_with_infinite_retry(session_manager, url, timeout=60, max_retries=50, rate_limiter=None):
    """
    零失败请求引擎：
    - 捕获 requests 库 ALL 异常子类
    - 所有临时性 HTTP 错误自动重试
    - Session 损坏自动重建
    - 退避时间有上限，不会无限增长
    - 429/503 特别处理：更长的退避时间

    返回: (response, status_code) 或 (None, None) 如果是永久性失败
    """
    thread_name = threading.current_thread().name

    # ★ 当 max_retries = inf 时，永远不放弃
    attempt = 0
    while True:
        if max_retries != float("inf") and attempt >= max_retries:
            # 重试次数用尽 — 但仍然不放弃，等待后继续
            # 因为 v4.0 的哲学是：任何ID都不应失败
            wait = MAX_BACKOFF + random.uniform(0, 30)
            print(c(f"\n  ⚠ [{thread_name}] ID重试{max_retries}次已达上限，冷却{wait:.0f}s后继续...", "yellow"), flush=True)
            time.sleep(wait)
            attempt = 0  # 重置计数器，继续重试

        attempt += 1
        session = session_manager.get_session()

        # 随机轮换 User-Agent
        session.headers["User-Agent"] = random.choice(UA_POOL)

        try:
            # 全局速率限制（在请求前获取令牌）
            if rate_limiter:
                rate_limiter.acquire()

            r = session.get(url, timeout=timeout, allow_redirects=False)

            # ─── HTTP 状态码处理 ───
            if r.status_code in NOT_FOUND_STATUS_CODES:
                # 不存在的ID — 正常情况，不是失败
                session_manager.record_success()
                return r, r.status_code

            if r.status_code == 200:
                session_manager.record_success()
                return r, r.status_code

            if r.status_code in PERMANENT_STATUS_CODES:
                # 永久性错误 — 记录但尝试继续
                session_manager.record_failure()
                print(c(f"\n  ✗ [{thread_name}] HTTP {r.status_code}（永久性错误），仍将重试", "red"), flush=True)

            if r.status_code in RETRY_STATUS_CODES:
                session_manager.record_failure()
                # 根据错误类型选择不同的退避策略
                if r.status_code == 429:
                    # 限流：特别长的退避 + 检查 Retry-After 头
                    retry_after = r.headers.get("Retry-After", "")
                    if retry_after.isdigit():
                        wait = int(retry_after) + random.uniform(1, 5)
                    else:
                        wait = min(30 * (1.5 ** min(attempt, 8)), MAX_BACKOFF)
                    print(c(f"\n  ⚠ [{thread_name}] HTTP 429 被限流, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
                elif r.status_code == 503:
                    wait = min(15 * (1.5 ** min(attempt, 6)), MAX_BACKOFF)
                    print(c(f"\n  ⚠ [{thread_name}] HTTP 503 服务不可用, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
                elif r.status_code == 502 or r.status_code == 504:
                    wait = min(10 * (1.5 ** min(attempt, 6)), MAX_BACKOFF)
                    print(c(f"\n  ⚠ [{thread_name}] HTTP {r.status_code} 网关错误, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
                else:
                    wait = min(8 * (1.5 ** min(attempt, 5)), MAX_BACKOFF)
                    print(c(f"\n  ⚠ [{thread_name}] HTTP {r.status_code}, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
                time.sleep(wait + random.uniform(0, 3))
                continue

            # 其他未知状态码 — 也重试
            session_manager.record_failure()
            wait = min(5 * (1.3 ** min(attempt, 5)), 60)
            print(c(f"\n  ⚠ [{thread_name}] HTTP {r.status_code}（未知状态码）, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
            time.sleep(wait + random.uniform(0, 2))
            continue

        except requests.exceptions.Timeout as e:
            session_manager.record_failure()
            wait = min(5 * (1.5 ** min(attempt, 6)), MAX_BACKOFF)
            print(c(f"\n  ⚠ [{thread_name}] 超时, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
            time.sleep(wait + random.uniform(0, 3))
            continue

        except requests.exceptions.ConnectionError as e:
            session_manager.record_failure()
            # 连接错误可能是 Session 损坏，SessionManager 会在下次 get_session 时自动重建
            wait = min(8 * (1.5 ** min(attempt, 6)), MAX_BACKOFF)
            # 简化错误信息，避免输出过长的错误消息
            err_msg = str(e)[:80] if len(str(e)) > 80 else str(e)
            print(c(f"\n  ⚠ [{thread_name}] 连接错误, 等待{wait:.0f}s (attempt {attempt}): {err_msg}", "yellow"), flush=True)
            time.sleep(wait + random.uniform(0, 3))
            continue

        except requests.exceptions.ChunkedEncodingError as e:
            session_manager.record_failure()
            # 分块传输编码错误 — 通常是连接中断
            wait = min(3 * (1.3 ** min(attempt, 5)), 60)
            print(c(f"\n  ⚠ [{thread_name}] 分块编码错误, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
            time.sleep(wait + random.uniform(0, 2))
            continue

        except requests.exceptions.ContentDecodingError as e:
            session_manager.record_failure()
            # 内容解码错误 — 可能是响应不完整
            wait = min(5 * (1.3 ** min(attempt, 5)), 60)
            print(c(f"\n  ⚠ [{thread_name}] 内容解码错误, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
            time.sleep(wait + random.uniform(0, 2))
            continue

        except requests.exceptions.SSLError as e:
            session_manager.record_failure()
            # SSL 错误 — 可能需要重建连接
            wait = min(10 * (1.5 ** min(attempt, 4)), 60)
            print(c(f"\n  ⚠ [{thread_name}] SSL错误, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
            time.sleep(wait + random.uniform(0, 2))
            continue

        except requests.exceptions.ProxyError as e:
            session_manager.record_failure()
            # 代理错误 — 代理可能暂时不可用
            wait = min(15 * (1.3 ** min(attempt, 4)), MAX_BACKOFF)
            print(c(f"\n  ⚠ [{thread_name}] 代理错误, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
            time.sleep(wait + random.uniform(0, 5))
            continue

        except requests.exceptions.TooManyRedirects as e:
            session_manager.record_failure()
            wait = min(5 * (1.2 ** min(attempt, 4)), 30)
            print(c(f"\n  ⚠ [{thread_name}] 重定向过多, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
            time.sleep(wait + random.uniform(0, 2))
            continue

        except requests.exceptions.ReadTimeout as e:
            session_manager.record_failure()
            # 读取超时 — 服务器响应太慢
            wait = min(8 * (1.5 ** min(attempt, 5)), MAX_BACKOFF)
            print(c(f"\n  ⚠ [{thread_name}] 读取超时, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
            time.sleep(wait + random.uniform(0, 3))
            continue

        except requests.exceptions.RequestException as e:
            # ★ 兜底：捕获所有其他 requests 异常
            session_manager.record_failure()
            wait = min(10 * (1.3 ** min(attempt, 5)), MAX_BACKOFF)
            err_msg = str(type(e).__name__)
            print(c(f"\n  ⚠ [{thread_name}] {err_msg}, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
            time.sleep(wait + random.uniform(0, 3))
            continue

        except Exception as e:
            # ★★ 终极兜底：捕获所有 Python 异常
            session_manager.record_failure()
            wait = min(15 * (1.2 ** min(attempt, 4)), MAX_BACKOFF)
            err_msg = f"{type(e).__name__}: {str(e)[:60]}"
            print(c(f"\n  ⚠ [{thread_name}] 未知异常({err_msg}), 等待{wait:.0f}s (attempt {attempt})", "red"), flush=True)
            time.sleep(wait + random.uniform(0, 5))
            continue


# ─────────────────────────── 自动检测最新条目ID ──────────────────
def detect_latest_id(session, base_url, timeout=60):
    print(c("\n[ 自动检测最新条目ID ]", "bold"))
    try:
        # 先尝试标准首页方法
        for _ in range(5):  # 最多重试5次
            try:
                r = session.get(base_url, params={"s": "id", "o": "desc"}, timeout=timeout)
                r.raise_for_status()
                soup = BeautifulSoup(r.text, "html.parser")
                rows = soup.select("table.torrent-list tbody tr")
                if rows:
                    max_id = 0
                    for row in rows:
                        links = row.select("td:nth-child(2) a:not(.comments)")
                        for link in links:
                            href = link.get("href", "")
                            m = re.search(r"/view/(\d+)", href)
                            if m:
                                row_id = int(m.group(1))
                                if row_id > max_id:
                                    max_id = row_id
                    if max_id > 0:
                        print(c(f"✔ 检测到最新条目ID: {max_id}", "green"))
                        return max_id
                print(c("⚠ 首页解析失败，重试中...", "yellow"))
                time.sleep(3)
            except Exception as e:
                print(c(f"⚠ 首页请求失败: {e}，重试中...", "yellow"))
                time.sleep(5)
    except Exception as e:
        print(c(f"✗ 标准检测失败: {e}", "red"))

    # 备用方法：二分查找
    print(c("⚠ 使用二分查找法检测最新ID...", "yellow"))
    try:
        lo, hi = 1, 10000000
        latest = 0
        while lo <= hi:
            mid = (lo + hi) // 2
            try:
                r = session.get(f"{base_url}/view/{mid}", timeout=timeout, allow_redirects=False)
                if r.status_code == 200:
                    latest = mid
                    lo = mid + 1
                else:
                    hi = mid - 1
                time.sleep(0.3)  # 礼貌间隔
            except Exception:
                time.sleep(2)
        if latest > 0:
            # 向上精确探测
            for offset in range(1, 500):
                check_id = latest + offset
                try:
                    r = session.get(f"{base_url}/view/{check_id}", timeout=timeout, allow_redirects=False)
                    if r.status_code == 200:
                        latest = check_id
                    else:
                        break
                except Exception:
                    time.sleep(1)
            print(c(f"✔ 二分查找检测到最新条目ID: {latest}", "green"))
            return latest
    except Exception as e:
        print(c(f"✗ 二分查找失败: {e}", "red"))

    print(c("✗ 自动检测失败，请手动输入", "red"))
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

    if not category:
        for row_div in rows_divs:
            label_div = row_div.find("div", class_="col-md-1")
            if label_div and "category" in label_div.get_text(strip=True).lower():
                value_div = row_div.find("div", class_="col-md-5")
                if value_div:
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


# ─────────────────────────── 失败ID持久化 ─────────────────────
def load_failed_ids(filepath):
    """从文件加载失败的ID列表"""
    ids = []
    if os.path.isfile(filepath):
        try:
            with open(filepath, "r") as f:
                for line in f:
                    line = line.strip()
                    if line.isdigit():
                        ids.append(int(line))
        except Exception:
            pass
    return ids


def save_failed_ids(filepath, ids):
    """将失败的ID保存到文件（进程崩溃恢复用）"""
    try:
        with open(filepath, "w") as f:
            for tid in ids:
                f.write(f"{tid}\n")
    except Exception:
        pass


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
            jitter = wait * random.uniform(0, 0.2)
            time.sleep(wait + jitter)

    def update_qps(self, new_qps):
        """动态调整 QPS"""
        with self.lock:
            self.max_qps = max(0.5, new_qps)
            self.min_interval = 1.0 / self.max_qps


class CircuitBreaker:
    """全局熔断器 — 检测到服务端严重故障时暂停所有线程"""

    def __init__(self):
        self.lock = threading.Lock()
        self.consecutive_global_failures = 0
        self.is_open = False  # 熔断器是否打开
        self.last_fail_time = 0

    def record_success(self):
        with self.lock:
            self.consecutive_global_failures = 0
            if self.is_open:
                self.is_open = False

    def record_failure(self):
        with self.lock:
            self.consecutive_global_failures += 1
            self.last_fail_time = time.time()
            if self.consecutive_global_failures >= CIRCUIT_BREAKER_THRESHOLD and not self.is_open:
                self.is_open = True
                return True  # 刚触发熔断
        return False

    def check_and_wait(self, thread_name=""):
        """如果熔断器打开，阻塞等待直到恢复"""
        while True:
            with self.lock:
                if not self.is_open:
                    return False
                elapsed = time.time() - self.last_fail_time
                if elapsed >= CIRCUIT_BREAKER_WAIT:
                    # 尝试半开恢复
                    self.consecutive_global_failures = 0
                    self.is_open = False
                    print(c(f"\n  🔓 [{thread_name}] 熔断器恢复，尝试重新请求", "green"), flush=True)
                    return False
            # 熔断等待中
            remaining = CIRCUIT_BREAKER_WAIT - elapsed
            print(c(f"\n  🔒 [{thread_name}] 全局熔断中，等待{remaining:.0f}s（服务端故障）...", "red"), flush=True)
            time.sleep(min(remaining, 5))


class ScraperState:
    """线程安全的共享爬取状态"""

    def __init__(self, start_id, end_id, existing_ids):
        self.lock = threading.RLock()
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
        self._recent_results = []
        self._window_size = 200

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
        with self.lock:
            if is_success:
                self.total_items += 1
            self._recent_results.append((time.time(), is_success))
            cutoff = time.time() - 60
            self._recent_results = [
                (ts, ok) for ts, ok in self._recent_results if ts > cutoff
            ]

    def record_not_found(self):
        with self.lock:
            self.total_not_found += 1
            self._recent_results.append((time.time(), True))
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
                "current_qps": 0,
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
    adapter = HTTPAdapter(
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


# ─────────────────────────── 临时 Session（用于检测等） ──────────
def create_temp_session(base_url, proxies):
    """创建临时 Session，用于连通性检测等一次性操作"""
    return create_session(base_url, proxies)


def progress_display_thread(state, total_range, rate_limiter):
    """进度显示 + 自适应速率调整线程"""
    adaptive_check_interval = 5
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
        sys.stdout.write(f"\r{progress_line:<110}")
        sys.stdout.flush()

        # ── 自适应速率调整 ──
        now = time.time()
        if now - last_adaptive_check >= adaptive_check_interval:
            last_adaptive_check = now
            fr = stats["failure_rate"]

            if fr > 0.5:
                new_qps = max(1.0, rate_limiter.max_qps * 0.5)
                if new_qps != rate_limiter.max_qps:
                    rate_limiter.update_qps(new_qps)
                    print(c(f"\n  ⚡ 自适应: 失败率 {fr*100:.0f}% > 50%, 降速至 {new_qps:.1f} QPS", "yellow"), flush=True)
            elif fr > 0.2:
                new_qps = max(1.0, rate_limiter.max_qps * 0.7)
                if new_qps != rate_limiter.max_qps:
                    rate_limiter.update_qps(new_qps)
                    print(c(f"\n  ⚡ 自适应: 失败率 {fr*100:.0f}% > 20%, 降速至 {new_qps:.1f} QPS", "yellow"), flush=True)
            elif fr < 0.05:
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
    params_ref = [params]

    BASE = "https://nyaa.si"
    tmp_session = create_temp_session(BASE, proxies)
    out_file = params["out"]
    is_resume = params.get("is_resume", False)
    fieldnames = ["id", "name", "info_hash", "magnet", "size", "date", "category", "detail_url"]
    num_threads = params.get("num_threads", 8)
    max_qps = params.get("max_qps", 10.0)
    params["_original_qps"] = max_qps

    # ── 1. 检测连通性（重试5次） ──
    print(c("\n正在测试连接...", "blue"))
    connected = False
    for i in range(5):
        try:
            r = tmp_session.get(BASE, timeout=params["timeout"])
            r.raise_for_status()
            print(c(f"✔ 连接成功 (HTTP {r.status_code})", "green"))
            connected = True
            break
        except Exception as e:
            print(c(f"  第{i+1}次连接失败: {e}", "yellow"))
            if i < 4:
                time.sleep(5)
    if not connected:
        print(c("✗ 5次连接尝试均失败，请检查网络/代理设置", "red"))
        sys.exit(1)
    tmp_session.close()

    # ── 2. 确定ID范围 ──
    start_id = params["start_id"]
    end_id = params["end_id"]
    if end_id == "auto":
        detect_session = create_temp_session(BASE, proxies)
        end_id = detect_latest_id(detect_session, BASE, timeout=params["timeout"])
        detect_session.close()
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
    print(f"  并行线程: {num_threads}")
    print(f"  全局QPS: {max_qps} (自适应调节)")
    print(f"  请求超时: {params['timeout']}s")
    print(f"  最大重试: {'∞ 无限' if params['retry_count'] == float('inf') else params['retry_count']}")
    print(f"  Session重建阈值: 连续{SESSION_REBUILD_THRESHOLD}次失败")
    print(f"  全局熔断阈值: {CIRCUIT_BREAKER_THRESHOLD}次连续失败")
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

    # ── 4. 加载之前失败的ID ──
    failed_ids_from_file = load_failed_ids(FAILED_IDS_FILE)
    if failed_ids_from_file:
        print(c(f"✔ 恢复: 从 {FAILED_IDS_FILE} 加载了 {len(failed_ids_from_file)} 个之前失败的ID", "yellow"))

    # ── 5. 初始化核心组件 ──
    state = ScraperState(start_id, end_id, existing_ids)
    rate_limiter = GlobalRateLimiter(max_qps)
    circuit_breaker = CircuitBreaker()
    session_manager = SessionManager(BASE, proxies, timeout=params["timeout"])
    result_queue = queue.Queue(maxsize=num_threads * 4)
    failed_ids_queue = queue.Queue()  # 失败ID队列（线程安全）

    # 把之前失败的ID加入重试队列
    for fid in failed_ids_from_file:
        failed_ids_queue.put(fid)

    ids_to_scrape = total_range - len(existing_ids.intersection(range(start_id, end_id + 1)))
    print(c(f"\n✔ 需要实际爬取约 {ids_to_scrape:,} 个ID（已去重）", "cyan"))
    print(c(f"✔ 启动 {num_threads} 个工作线程 + 全局 {max_qps:.0f} QPS 限速...", "cyan"))
    print(c(f"✔ 零失败架构: 失败ID自动进入重试队列", "cyan"))
    print(c(f"✔ 自适应速率: 失败率高→自动降速, 失败率低→自动提速", "cyan"))
    print()

    # ── 6. 写入线程 ──
    writer = threading.Thread(
        target=writer_thread,
        args=(out_file, fieldnames, result_queue, state),
        name="Writer", daemon=True
    )
    writer.start()

    # ── 7. 进度 + 自适应线程 ──
    progress = threading.Thread(
        target=progress_display_thread,
        args=(state, total_range, rate_limiter),
        name="Progress", daemon=True
    )
    progress.start()

    # ── 8. 工作线程 ──
    state.set_active_threads(num_threads)
    timeout = params["timeout"]
    retry_count = params["retry_count"]

    def thread_target():
        """工作线程：从 ID 分配器取任务，零失败请求"""
        while not state.stop_event.is_set():
            target_id = state.get_next_id()
            if target_id is None:
                break

            # 检查熔断器
            circuit_breaker.check_and_wait(threading.current_thread().name)

            detail_url = f"{BASE}/view/{target_id}"

            try:
                r, status = request_with_infinite_retry(
                    session_manager, detail_url,
                    timeout=timeout,
                    max_retries=retry_count,
                    rate_limiter=rate_limiter,
                )

                if r is None:
                    # 理论上不应该到达这里（无限重试保证），但以防万一
                    failed_ids_queue.put(target_id)
                    state.record_failed()
                    continue

                # ─── 不存在的ID ───
                if status in NOT_FOUND_STATUS_CODES:
                    state.record_not_found()
                    circuit_breaker.record_success()
                    continue

                # ─── 非预期状态码（理论上不应到达，但兜底处理） ───
                if status != 200:
                    state.record_failed()
                    circuit_breaker.record_failure()
                    failed_ids_queue.put(target_id)
                    continue

                # ─── 页面内容检查 ───
                soup = BeautifulSoup(r.text, "html.parser")
                kbd = soup.select_one("kbd")
                mag_a = soup.find("a", href=re.compile(r"^magnet:"))
                panel = soup.select_one("div.panel.panel-default")

                if not panel and not kbd and not mag_a:
                    state.record_not_found()
                    circuit_breaker.record_success()
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
                circuit_breaker.record_success()

            except Exception as e:
                # ★ 终极兜底 — 任何未预期的异常都捕获，ID不丢失
                state.record_failed()
                circuit_breaker.record_failure()
                failed_ids_queue.put(target_id)
                print(c(f"\n  ✗ [{threading.current_thread().name}] 未知异常 (ID {target_id}): {type(e).__name__}: {str(e)[:60]}", "red"), flush=True)

    # 创建并启动所有工作线程
    threads = []
    try:
        for i in range(num_threads):
            t = threading.Thread(target=thread_target, name=f"Worker-{i+1:02d}")
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        print(c(f"\n\n⚠ 用户中断 (Ctrl+C)，正在优雅停止...", "yellow"), flush=True)
        state.stop_event.set()

    # ── 9. 主爬取结束，收集所有失败ID ──
    all_failed_ids = []
    while not failed_ids_queue.empty():
        try:
            all_failed_ids.append(failed_ids_queue.get_nowait())
        except queue.Empty:
            break

    # 去重
    all_failed_ids = sorted(set(all_failed_ids))

    # ── 10. 最终重做轮次 — 扫清所有失败ID ──
    if all_failed_ids:
        print(c(f"\n\n{'='*60}", "bold"))
        print(c(f"  🔄 最终重做轮次: {len(all_failed_ids)} 个失败ID将被重新爬取", "bold"))
        print(c(f"  哲学: 任何ID都不应失败，只有'尚未成功'", "bold"))
        print(c(f"{'='*60}\n", "bold"))

        # 保存失败ID到文件（以防进程崩溃）
        save_failed_ids(FAILED_IDS_FILE, all_failed_ids)

        retry_round = 1
        max_retry_rounds = 5  # 最多5轮最终重做

        while all_failed_ids and retry_round <= max_retry_rounds:
            print(c(f"\n  📋 重做第 {retry_round} 轮: 剩余 {len(all_failed_ids)} 个失败ID", "cyan"))
            sys.stdout.flush()

            still_failed = []
            for idx, fid in enumerate(all_failed_ids):
                if state.stop_event.is_set():
                    break

                # 每处理100个ID保存一次进度
                if idx % 100 == 0 and idx > 0:
                    save_failed_ids(FAILED_IDS_FILE, all_failed_ids[idx:])

                # 检查熔断器
                circuit_breaker.check_and_wait(f"Retry-{retry_round}")

                detail_url = f"{BASE}/view/{fid}"
                success = False

                try:
                    r, status = request_with_infinite_retry(
                        session_manager, detail_url,
                        timeout=max(timeout, 120),  # 重做时使用更长超时
                        max_retries=float("inf"),  # 无限重试
                        rate_limiter=rate_limiter,
                    )

                    if r is not None:
                        if status in NOT_FOUND_STATUS_CODES:
                            state.record_not_found()
                            circuit_breaker.record_success()
                            success = True
                        elif status == 200:
                            soup = BeautifulSoup(r.text, "html.parser")
                            kbd = soup.select_one("kbd")
                            mag_a = soup.find("a", href=re.compile(r"^magnet:"))
                            panel = soup.select_one("div.panel.panel-default")

                            if panel or kbd or mag_a:
                                name, info_hash, magnet, size, date, category = parse_detail_page(soup, detail_url)
                                row = {
                                    "id": fid, "name": name, "info_hash": info_hash,
                                    "magnet": magnet, "size": size, "date": date,
                                    "category": category, "detail_url": detail_url,
                                }
                                result_queue.put(row)
                                state.record_result(True)
                                circuit_breaker.record_success()
                                success = True
                            else:
                                state.record_not_found()
                                success = True

                except Exception:
                    pass

                if not success:
                    circuit_breaker.record_failure()
                    still_failed.append(fid)

                # 进度显示
                if (idx + 1) % 10 == 0 or idx == len(all_failed_ids) - 1:
                    print(c(f"\r  重做进度: {idx+1}/{len(all_failed_ids)} "
                            f"(本轮成功={len(all_failed_ids)-len(still_failed)}, 仍失败={len(still_failed)})",
                            "cyan"), end="", flush=True)

            # 更新失败列表
            resolved = len(all_failed_ids) - len(still_failed)
            print()
            if resolved > 0:
                print(c(f"  ✔ 第 {retry_round} 轮重做成功恢复 {resolved} 个ID", "green"))

            if not still_failed:
                print(c(f"  🎉 所有失败ID已全部恢复！", "green"))
                all_failed_ids = []
                # 清理失败ID文件
                try:
                    os.remove(FAILED_IDS_FILE)
                except Exception:
                    pass
                break
            else:
                all_failed_ids = sorted(set(still_failed))
                save_failed_ids(FAILED_IDS_FILE, all_failed_ids)
                print(c(f"  ⚠ 仍有 {len(all_failed_ids)} 个ID失败，进入下一轮", "yellow"))
                retry_round += 1
                if retry_round <= max_retry_rounds:
                    # 轮次间等待，给服务器喘息时间
                    wait_time = 30
                    print(c(f"  ⏳ 等待 {wait_time}s 后开始第 {retry_round} 轮...", "yellow"))
                    time.sleep(wait_time)

        if all_failed_ids:
            save_failed_ids(FAILED_IDS_FILE, all_failed_ids)
            print(c(f"\n  ⚠ 最终仍有 {len(all_failed_ids)} 个ID未能成功（已保存到 {FAILED_IDS_FILE}）", "yellow"))
            print(c(f"  💡 下次运行脚本时会自动加载这些ID并重新尝试", "yellow"))
            print(c(f"  💡 也可以手动编辑 {FAILED_IDS_FILE} 移除不需要的ID", "yellow"))

    # ── 11. 清理 ──
    state.set_active_threads(0)
    result_queue.join()
    result_queue.put(None)
    writer.join(timeout=5)
    state.stop_event.set()
    progress.join(timeout=2)
    session_manager.close_all()

    # ── 12. 最终统计 ──
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

    remaining_failed = len(all_failed_ids) if 'all_failed_ids' in dir() and all_failed_ids else 0

    print(c(f"""
╔══════════════════════════════════════════════════════════════════╗
║  爬取完成！                                                    ║
║  模式       : {mode_str:<40}║
║  线程数     : {num_threads:<40}║
║  本次新增   : {final_stats['total_items']:<40}║
║  文件总记录 : {final_total:<40}║
║  ID 范围   : {start_id} ~ {end_id:<28}║
║  不存在ID  : {final_stats['total_not_found']:<40}║
║  最终失败  : {remaining_failed:<40}║
║  去重跳过  : {skipped_count:<40}║
║  平均速度  : {speed_str:<40}║
║  耗时      : {time_str:<40}║
║  输出文件  : {out_file[:40]:<38}║
╚══════════════════════════════════════════════════════════════════╝
""", "green"))

    if remaining_failed > 0:
        print(c(f"  ⚠ 仍有 {remaining_failed} 个ID未成功，下次运行时自动重试", "yellow"))
        print(c(f"  📁 失败ID列表: {FAILED_IDS_FILE}", "yellow"))

    if state.stop_event.is_set():
        print(c(f"  当前进度: 已处理 {final_stats['processed']:,} / {total_range:,}", "cyan"))
        print(c(f"  下次运行本脚本会自动检测该文件并从断点继续", "cyan"))


# ─────────────────────────── 入口 ───────────────────────────────
params_ref = [{}]


def main():
    print_banner()
    proxies = setup_proxy()
    params = setup_params()
    print(c("\n配置完成，开始爬取...\n", "bold"))
    scrape(proxies, params)


if __name__ == "__main__":
    main()
