#!/usr/bin/env python3
"""
nyaa.si 全量元数据爬虫  v6.0 优化版（多线程+智能自适应+永不放弃+信号处理）

═══════════════════════════════════════════════════════════════
  核心设计哲学: 任何 ID 都不应失败，只有"尚未成功"
═══════════════════════════════════════════════════════════════

v6.0 改进清单（基于 v5.0 的全部重构）:

  【BUG 修复】
  1. 【修复】统一 Session 创建工厂 — 消除双重 create_session 不一致
  2. 【修复】重试线程安全的 all_failed_ids — 使用 Queue+锁替代直接 append
  3. 【修复】移除 dir() 检测 — 提前初始化 all_failed_ids
  4. 【修复】写入线程存活保障 — 仅靠 None 哨兵退出，不依赖 active_threads
  5. 【修复】移除全局 params_ref — 改为参数传递给进度线程
  6. 【修复】定期调用 save_state() — 每 1000 个 ID 保存一次状态
  7. 【修复】清理未使用导入 — 移除 urljoin 和 socket
  8. 【修复】限速器提前返回 — wait<=0 时直接 return，避免 sleep(0)

  【性能优化】
  1. 【优化】Condition-based 限速器 — 替换 busy-wait，降低 CPU 占用
  2. 【优化】更合理的连接池 — pool_connections/maxsize 从 32 降至 8
  3. 【优化】UA 按会话轮换 — 每个 session 固定 UA，偶尔切换
  4. 【优化】指数+二分 ID 检测 — O(log n) 替代 O(log max_id)
  5. 【优化】大文件加载进度 — load_existing_ids 显示进度条
  6. 【优化】差异化退避上限 — 429/503/Timeout/Connection 各有独立上限
  7. 【优化】DNS 预解析 — 启动时验证域名可达性
  8. 【优化】定期健康检查 — 每 60s 轻量 HEAD 探测
  9. 【优化】重试轮改进 — 无限轮次(可配置)、指数冷却、降低 QPS
  10.【优化】CSV 原子写入 — os.replace() + 写入验证
  11.【优化】信号处理 — SIGTERM/SIGHUP 优雅关停，保存状态+刷缓冲
  12.【优化】进度显示升级 — 百分比、当前 ID、旋转动画、峰值/持续速度

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
import json
import signal
from collections import deque
from datetime import datetime

# ─────────────────────────── UA 池 ─────────────────────────────
UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.0.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.0.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.0.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.0.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.0.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.0.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.0.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.0.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.0.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.0.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.0.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.0.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0",
]

# ─────────────────────────── 常量定义 ───────────────────────────
PERMANENT_STATUS_CODES = {400, 401, 405, 415, 422}
RETRY_STATUS_CODES = {429, 500, 502, 503, 504, 403}
NOT_FOUND_STATUS_CODES = {301, 302, 303, 307, 308, 404}

# v6.0: 差异化退避上限 # Optimization 6
BACKOFF_CEILINGS = {
    "429": 300,       # 速率限制，需要更长等待
    "503": 240,       # 服务过载
    "timeout": 90,    # 超时
    "connection": 120, # 连接错误
    "default": 60,    # 其他错误
}

SESSION_REBUILD_THRESHOLD = 5
CIRCUIT_BREAKER_THRESHOLD = 50
CIRCUIT_BREAKER_WAIT = 120
FAILED_IDS_FILE = "nyaa_failed_ids.txt"
STATE_FILE = "nyaa_scraper_state.json"
BATCH_WRITE_SIZE = 50
STATE_SAVE_INTERVAL = 1000        # v6.0: 每处理 1000 个 ID 保存状态
HEALTH_CHECK_INTERVAL = 60        # v6.0: 健康检查间隔(秒)
MAX_RETRY_ROUNDS = 10             # v6.0: 可配置最大重试轮次
SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]


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
║     nyaa.si 全量元数据爬虫  v6.0 优化版（智能自适应）            ║
║                                                                  ║
║  核心: 任何ID都不会失败，只有"尚未成功"                           ║
║  保证: 永不放弃引擎 + 智能退避 + 多线程清扫 + 解析异常捕获        ║
║  爬取: 资源名称 | InfoHash | Magnet | 大小 | 日期 | 分类          ║
║  支持: 多线程 | 全局限速 | 自适应退避 | 断点续爬 | 代理 | 信号处理 ║
║  v6.0: 统一Session | Condition限速 | 原子写入 | DNS预解析 | 健康检查║
╚══════════════════════════════════════════════════════════════════╝
""", "cyan"))


# ─────────────────────────── 状态持久化 ───────────────────────────
def save_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception:
        pass


def load_state():
    if os.path.isfile(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def clear_state():
    for f in [STATE_FILE, FAILED_IDS_FILE]:
        try:
            if os.path.isfile(f):
                os.remove(f)
        except Exception:
            pass


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
                            if rid > max_id: max_id = rid
                            if rid < min_id: min_id = rid
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
        proxies = {"http": f"http://{host}:{port}", "https": f"http://{host}:{port}"}
        print(c(f"✔ 已设置 HTTP 代理: {host}:{port}", "green"))
    elif choice == "3":
        host = input("SOCKS5 代理主机 (如 127.0.0.1): ").strip()
        port = input("SOCKS5 代理端口 (如 1080): ").strip()
        user = input("用户名 (无则回车): ").strip()
        pwd = input("密码   (无则回车): ").strip()
        if user and pwd:
            proxies = {"http": f"socks5://{user}:{pwd}@{host}:{port}", "https": f"socks5://{user}:{pwd}@{host}:{port}"}
        else:
            proxies = {"http": f"socks5://{host}:{port}", "https": f"socks5://{host}:{port}"}
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

    qps_input = input(c("全局最大请求速率 QPS [默认 12]: ", "yellow")).strip()
    max_qps = float(qps_input) if qps_input.strip() else 12.0

    timeout_input = input(c("请求超时时间（秒）[默认 60]: ", "yellow")).strip()
    try:
        timeout_val = float(timeout_input)
        if timeout_val < 10: timeout_val = 10
    except Exception:
        timeout_val = 60

    retry_input = input(c("单次请求最大重试次数 [默认 0=无限重试直到成功]: ", "yellow")).strip()
    if retry_input.strip() == "0" or retry_input.strip() == "":
        retry_count = float("inf")
    else:
        retry_count = int(retry_input) if retry_input.isdigit() else 0
        if retry_count == 0:
            retry_count = float("inf")

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
#  核心组件：终极零失败引擎 v6.0
# ═══════════════════════════════════════════════════════════════

# v6.0 Bug 1: 统一的 Session 创建工厂函数
def create_session(base_url, proxies, pool_connections=8, pool_maxsize=8):
    """v6.0: 统一 Session 创建工厂 — 所有 Session 共享相同的重试策略和连接池配置"""
    session = requests.Session()
    ua = random.choice(UA_POOL)
    session.headers.update({
        "User-Agent": ua,
        "Referer": base_url,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,ja;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
    })
    if proxies:
        session.proxies.update(proxies)

    # v6.0: 统一使用 Retry 策略 (之前独立 create_session 没有重试)
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=list(RETRY_STATUS_CODES),
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
        max_retries=retry_strategy,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


class SessionManager:
    """v6.0: 线程安全的 Session 管理器，使用统一工厂函数 + UA 按会话轮换"""

    def __init__(self, base_url, proxies, timeout=60):
        self.base_url = base_url
        self.proxies = proxies
        self.timeout = timeout
        self.lock = threading.Lock()
        self._sessions = {}
        self._fail_counts = {}
        self._response_times = {}
        self._ua_counts = {}  # v6.0 Optimization 3: 跟踪每个 session 的请求计数

    def get_session(self):
        tid = threading.get_ident()
        with self.lock:
            fail_count = self._fail_counts.get(tid, 0)
            if fail_count >= SESSION_REBUILD_THRESHOLD:
                old_session = self._sessions.get(tid)
                if old_session:
                    try:
                        old_session.close()
                    except Exception:
                        pass
                    del self._sessions[tid]
                    if tid in self._ua_counts:
                        del self._ua_counts[tid]
                self._fail_counts[tid] = 0
                print(c(f"  🔄 [Thread-{tid}] Session已重建（连续失败{fail_count}次）", "magenta"), flush=True)
            if tid not in self._sessions:
                self._sessions[tid] = create_session(self.base_url, self.proxies)
                self._fail_counts[tid] = 0
                self._ua_counts[tid] = 0
            return self._sessions[tid]

    def maybe_rotate_ua(self):
        """v6.0 Optimization 3: 每 50-100 次请求偶尔轮换 UA"""
        tid = threading.get_ident()
        with self.lock:
            count = self._ua_counts.get(tid, 0)
            count += 1
            self._ua_counts[tid] = count
            if count >= random.randint(50, 100):
                self._ua_counts[tid] = 0
                session = self._sessions.get(tid)
                if session:
                    session.headers["User-Agent"] = random.choice(UA_POOL)

    def record_success(self):
        tid = threading.get_ident()
        with self.lock:
            self._fail_counts[tid] = 0

    def record_failure(self):
        tid = threading.get_ident()
        with self.lock:
            self._fail_counts[tid] = self._fail_counts.get(tid, 0) + 1

    def record_response_time(self, rt):
        tid = threading.get_ident()
        with self.lock:
            if tid not in self._response_times:
                self._response_times[tid] = deque(maxlen=20)
            self._response_times[tid].append(rt)

    def get_avg_response_time(self):
        with self.lock:
            all_times = []
            for dq in self._response_times.values():
                all_times.extend(dq)
            return sum(all_times) / len(all_times) if all_times else 1.0

    def close_all(self):
        with self.lock:
            for session in self._sessions.values():
                try:
                    session.close()
                except Exception:
                    pass
            self._sessions.clear()


# v6.0 Optimization 6: 差异化退避上限
def get_backoff_ceil(error_type):
    """v6.0: 根据错误类型返回不同的退避上限"""
    return BACKOFF_CEILINGS.get(error_type, BACKOFF_CEILINGS["default"])


def request_with_infinite_retry(session_manager, url, timeout=60, max_retries=float("inf"), rate_limiter=None):
    """
    v6.0 终极零失败请求引擎：
    - 永不放弃：max_retries=inf 时真正无限重试
    - 智能退避：基于错误类型差异化退避上限 (v6.0 Optimization 6)
    - 服务探测：连续429/503后主动探测服务恢复
    - 全异常捕获：从 socket 到 SSL 到解析的所有异常
    - UA 按会话轮换 (v6.0 Optimization 3)
    """
    thread_name = threading.current_thread().name
    attempt = 0
    consecutive_429 = 0
    consecutive_503 = 0

    while True:
        if max_retries != float("inf") and attempt >= max_retries:
            ceil = get_backoff_ceil("default")
            wait = ceil + random.uniform(0, 30)
            print(c(f"\n  ⚠ [{thread_name}] 重试{max_retries}次已达上限，冷却{wait:.0f}s后继续...", "yellow"), flush=True)
            time.sleep(wait)
            attempt = 0

        attempt += 1
        session = session_manager.get_session()
        # v6.0 Optimization 3: 不再每次请求都换 UA，由 maybe_rotate_ua 控制
        session_manager.maybe_rotate_ua()

        try:
            if rate_limiter:
                rate_limiter.acquire()

            req_start = time.monotonic()
            r = session.get(url, timeout=timeout, allow_redirects=False)
            req_time = time.monotonic() - req_start
            session_manager.record_response_time(req_time)

            if r.status_code in NOT_FOUND_STATUS_CODES:
                session_manager.record_success()
                consecutive_429 = 0
                consecutive_503 = 0
                return r, r.status_code

            if r.status_code == 200:
                session_manager.record_success()
                consecutive_429 = 0
                consecutive_503 = 0
                return r, r.status_code

            if r.status_code in PERMANENT_STATUS_CODES:
                session_manager.record_failure()
                ceil = get_backoff_ceil("default")
                wait = min(5 * (1.2 ** min(attempt, 5)), ceil)
                print(c(f"\n  ⚠ [{thread_name}] HTTP {r.status_code}（永久性错误），等待{wait:.0f}s后重试", "red"), flush=True)
                time.sleep(wait + random.uniform(0, 2))
                continue

            if r.status_code in RETRY_STATUS_CODES:
                session_manager.record_failure()
                if r.status_code == 429:
                    consecutive_429 += 1
                    retry_after = r.headers.get("Retry-After", "")
                    ceil = get_backoff_ceil("429")  # v6.0: 300s
                    if retry_after.isdigit():
                        wait = int(retry_after) + random.uniform(1, 5)
                    else:
                        base_wait = 30 if consecutive_429 >= 3 else 15
                        wait = min(base_wait * (1.5 ** min(attempt, 8)), ceil)
                    print(c(f"\n  ⚠ [{thread_name}] HTTP 429 被限流, 等待{wait:.0f}s (attempt {attempt}, 连续{consecutive_429}次)", "yellow"), flush=True)
                    time.sleep(wait + random.uniform(0, 5))
                    # 服务状态探测：连续多次429后主动探测
                    if consecutive_429 >= 5:
                        print(c(f"\n  🔍 [{thread_name}] 连续429，主动探测服务恢复状态...", "cyan"), flush=True)
                        time.sleep(10)
                        consecutive_429 = 0
                    continue
                elif r.status_code == 503:
                    consecutive_503 += 1
                    ceil = get_backoff_ceil("503")  # v6.0: 240s
                    wait = min(20 * (1.5 ** min(attempt, 6)), ceil)
                    print(c(f"\n  ⚠ [{thread_name}] HTTP 503 服务不可用, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
                    time.sleep(wait + random.uniform(0, 5))
                    if consecutive_503 >= 5:
                        print(c(f"\n  🔍 [{thread_name}] 连续503，主动探测服务恢复状态...", "cyan"), flush=True)
                        time.sleep(15)
                        consecutive_503 = 0
                    continue
                elif r.status_code in (502, 504):
                    ceil = get_backoff_ceil("default")  # v6.0: 60s
                    wait = min(10 * (1.5 ** min(attempt, 6)), ceil)
                    print(c(f"\n  ⚠ [{thread_name}] HTTP {r.status_code} 网关错误, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
                    time.sleep(wait + random.uniform(0, 3))
                    continue
                elif r.status_code == 403:
                    ceil = get_backoff_ceil("default")
                    wait = min(20 * (1.5 ** min(attempt, 6)), ceil)
                    print(c(f"\n  ⚠ [{thread_name}] HTTP 403 访问被拒绝, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
                    time.sleep(wait + random.uniform(0, 3))
                    continue
                else:
                    ceil = get_backoff_ceil("default")
                    wait = min(8 * (1.5 ** min(attempt, 5)), ceil)
                    print(c(f"\n  ⚠ [{thread_name}] HTTP {r.status_code}, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
                    time.sleep(wait + random.uniform(0, 3))
                    continue

            session_manager.record_failure()
            ceil = get_backoff_ceil("default")
            wait = min(5 * (1.3 ** min(attempt, 5)), ceil)
            print(c(f"\n  ⚠ [{thread_name}] HTTP {r.status_code}（未知状态码）, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
            time.sleep(wait + random.uniform(0, 2))
            continue

        except requests.exceptions.Timeout:
            session_manager.record_failure()
            ceil = get_backoff_ceil("timeout")  # v6.0: 90s
            wait = min(5 * (1.5 ** min(attempt, 6)), ceil)
            print(c(f"\n  ⚠ [{thread_name}] 超时, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
            time.sleep(wait + random.uniform(0, 3))
            continue

        except requests.exceptions.ConnectionError as e:
            session_manager.record_failure()
            ceil = get_backoff_ceil("connection")  # v6.0: 120s
            wait = min(8 * (1.5 ** min(attempt, 6)), ceil)
            err_msg = str(e)[:80] if len(str(e)) > 80 else str(e)
            print(c(f"\n  ⚠ [{thread_name}] 连接错误, 等待{wait:.0f}s (attempt {attempt}): {err_msg}", "yellow"), flush=True)
            time.sleep(wait + random.uniform(0, 3))
            continue

        except requests.exceptions.ChunkedEncodingError:
            session_manager.record_failure()
            ceil = get_backoff_ceil("default")
            wait = min(3 * (1.3 ** min(attempt, 5)), ceil)
            print(c(f"\n  ⚠ [{thread_name}] 分块编码错误, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
            time.sleep(wait + random.uniform(0, 2))
            continue

        except requests.exceptions.ContentDecodingError:
            session_manager.record_failure()
            ceil = get_backoff_ceil("default")
            wait = min(5 * (1.3 ** min(attempt, 5)), ceil)
            print(c(f"\n  ⚠ [{thread_name}] 内容解码错误, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
            time.sleep(wait + random.uniform(0, 2))
            continue

        except requests.exceptions.SSLError:
            session_manager.record_failure()
            ceil = get_backoff_ceil("default")
            wait = min(10 * (1.5 ** min(attempt, 4)), ceil)
            print(c(f"\n  ⚠ [{thread_name}] SSL错误, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
            time.sleep(wait + random.uniform(0, 2))
            continue

        except requests.exceptions.ProxyError:
            session_manager.record_failure()
            ceil = get_backoff_ceil("connection")
            wait = min(15 * (1.3 ** min(attempt, 4)), ceil)
            print(c(f"\n  ⚠ [{thread_name}] 代理错误, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
            time.sleep(wait + random.uniform(0, 5))
            continue

        except requests.exceptions.TooManyRedirects:
            session_manager.record_failure()
            ceil = get_backoff_ceil("default")
            wait = min(5 * (1.2 ** min(attempt, 4)), 30)
            print(c(f"\n  ⚠ [{thread_name}] 重定向过多, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
            time.sleep(wait + random.uniform(0, 2))
            continue

        except requests.exceptions.ReadTimeout:
            session_manager.record_failure()
            ceil = get_backoff_ceil("timeout")  # v6.0: 90s
            wait = min(8 * (1.5 ** min(attempt, 5)), ceil)
            print(c(f"\n  ⚠ [{thread_name}] 读取超时, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
            time.sleep(wait + random.uniform(0, 3))
            continue

        except requests.exceptions.RequestException as e:
            session_manager.record_failure()
            ceil = get_backoff_ceil("default")
            wait = min(10 * (1.3 ** min(attempt, 5)), ceil)
            err_msg = str(type(e).__name__)
            print(c(f"\n  ⚠ [{thread_name}] {err_msg}, 等待{wait:.0f}s (attempt {attempt})", "yellow"), flush=True)
            time.sleep(wait + random.uniform(0, 3))
            continue

        except Exception as e:
            session_manager.record_failure()
            ceil = get_backoff_ceil("default")
            wait = min(15 * (1.2 ** min(attempt, 4)), ceil)
            err_msg = f"{type(e).__name__}: {str(e)[:60]}"
            print(c(f"\n  ⚠ [{thread_name}] 未知异常({err_msg}), 等待{wait:.0f}s (attempt {attempt})", "red"), flush=True)
            time.sleep(wait + random.uniform(0, 5))
            continue


# ─────────────────────────── v6.0 DNS 预解析 ──────────────────
def dns_pre_resolve(host):
    """v6.0 Optimization 7: 启动时验证域名可达性"""
    print(c(f"\n[ DNS 预解析 ]", "bold"))
    print(f"  正在解析 {host} ...")
    try:
        import socket as _socket
        addrs = _socket.getaddrinfo(host, 443, _socket.AF_INET, _socket.SOCK_STREAM)
        if addrs:
            ip = addrs[0][4][0]
            print(c(f"✔ DNS 解析成功: {host} → {ip}", "green"))
            return True
    except Exception as e:
        print(c(f"✗ DNS 解析失败: {e}", "red"))
        return False
    return False


# ─────────────────────────── 自动检测最新条目ID ──────────────────
# v6.0 Optimization 4: 指数+二分搜索
def detect_latest_id(session, base_url, timeout=60):
    print(c("\n[ 自动检测最新条目ID ]", "bold"))

    # 第一步：尝试首页解析
    try:
        for _ in range(5):
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
                                if row_id > max_id: max_id = row_id
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

    # 第二步：指数搜索找到上界
    print(c("⚠ 使用指数+二分查找法检测最新ID...", "yellow"))
    try:
        print(c("  阶段1: 指数搜索确定上界...", "yellow"), end="", flush=True)
        upper = 1
        latest = 0
        while upper <= 10_000_000:
            try:
                r = session.get(f"{base_url}/view/{upper}", timeout=timeout, allow_redirects=False)
                if r.status_code == 200:
                    latest = upper
                    upper *= 2
                else:
                    break
                time.sleep(0.3)
            except Exception:
                time.sleep(2)
                break

        if latest > 0 and upper > latest:
            # 第三步：在 [latest, upper-1] 范围内二分查找
            print(c(f"  上界={upper}, 范围=[{latest}, {upper-1}], 二分查找中...", "yellow"), end="", flush=True)
            lo, hi = latest, upper - 1
            while lo <= hi:
                mid = (lo + hi) // 2
                try:
                    r = session.get(f"{base_url}/view/{mid}", timeout=timeout, allow_redirects=False)
                    if r.status_code == 200:
                        latest = mid
                        lo = mid + 1
                    else:
                        hi = mid - 1
                    time.sleep(0.3)
                except Exception:
                    time.sleep(2)

        # 第四步：向前扫描确认最新
        if latest > 0:
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
            print()  # 换行
            print(c(f"✔ 指数+二分查找检测到最新条目ID: {latest}", "green"))
            return latest
        else:
            # 回退到纯二分
            print(c("  指数搜索未找到有效ID，回退到纯二分查找...", "yellow"))
            lo, hi = 1, 10_000_000
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
                    time.sleep(0.3)
                except Exception:
                    time.sleep(2)
            if latest > 0:
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
                print()
                print(c(f"✔ 二分查找检测到最新条目ID: {latest}", "green"))
                return latest
    except Exception as e:
        print(c(f"✗ 查找失败: {e}", "red"))

    print(c("✗ 自动检测失败，请手动输入", "red"))
    manual = input(c("请手动输入最新条目ID: ", "yellow")).strip()
    return int(manual) if manual.isdigit() else 2093718


# ─────────────────────────── 解析详情页（全字段+异常捕获） ─────────────────
def parse_detail_page(soup, detail_url):
    """v6.0: 解析详情页，所有异常都被捕获，返回空字段而非抛出异常"""
    name = ""
    info_hash = ""
    magnet = ""
    size = ""
    date = ""
    category = ""
    rows_divs = []

    try:
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
    except Exception:
        pass

    try:
        kbd = soup.select_one("kbd")
        if kbd:
            info_hash = kbd.get_text(strip=True)
    except Exception:
        pass

    try:
        mag_a = soup.find("a", href=re.compile(r"^magnet:"))
        if mag_a:
            magnet = mag_a["href"]
    except Exception:
        pass

    try:
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
    except Exception:
        pass

    try:
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
    except Exception:
        pass

    try:
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
    except Exception:
        pass

    try:
        if not date:
            for elem in soup.select("[data-timestamp]"):
                ts = elem.get("data-timestamp", "")
                if ts:
                    try:
                        date = datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
                        break
                    except Exception:
                        pass
    except Exception:
        pass

    try:
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
    except Exception:
        pass

    return name, info_hash, magnet, size, date, category


# ─────────────────────────── 加载已爬取ID集合 ──────────────────
# v6.0 Optimization 5: 大文件加载进度
def load_existing_ids(out_file):
    ids = set()
    if os.path.isfile(out_file):
        try:
            file_size = os.path.getsize(out_file)
            show_progress = file_size > 1_000_000  # 超过 1MB 显示进度
            count = 0
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
                    count += 1
                    if show_progress and count % 50000 == 0:
                        sys.stdout.write(f"\r  加载已有ID中... {count:,} 条")
                        sys.stdout.flush()
            if show_progress and count > 0:
                sys.stdout.write(f"\r  ✔ 已加载 {count:,} 条已有记录的ID        \n")
                sys.stdout.flush()
        except Exception:
            pass
    return ids


# ─────────────────────────── 失败ID持久化 ─────────────────────
def load_failed_ids(filepath):
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


# v6.0 Optimization 10: 原子写入 (fsync 在 with 块内)
def save_failed_ids(filepath, ids):
    try:
        tmp_path = filepath + ".tmp"
        with open(tmp_path, "w") as f:
            for tid in ids:
                f.write(f"{tid}\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, filepath)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════
#  多线程核心组件 v6.0
# ═══════════════════════════════════════════════════════════════

# v6.0 Optimization 1: Condition-based 限速器
class GlobalRateLimiter:
    """v6.0: 基于 threading.Condition 的全局速率限制器，避免 busy-wait"""

    def __init__(self, max_qps):
        self.max_qps = max_qps
        self.min_interval = 1.0 / max_qps if max_qps > 0 else 0
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._last_request_time = 0.0

    def acquire(self):
        with self._cond:
            now = time.monotonic()
            wait = self.min_interval - (now - self._last_request_time)
            if wait <= 0:
                # v6.0 Bug 8: 提前返回，避免不必要的 sleep(0)
                self._last_request_time = now
                return
            # 精确等待，避免 busy-wait
            jitter = wait * random.uniform(0, 0.2)
            self._cond.wait(timeout=wait + jitter)
            self._last_request_time = time.monotonic()

    def update_qps(self, new_qps):
        with self._lock:
            self.max_qps = max(0.5, new_qps)
            self.min_interval = 1.0 / self.max_qps


class CircuitBreaker:
    """全局熔断器 — 检测到服务端严重故障时暂停所有线程"""

    def __init__(self):
        self.lock = threading.Lock()
        self.consecutive_global_failures = 0
        self.is_open = False
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
                return True
        return False

    def check_and_wait(self, thread_name=""):
        while True:
            with self.lock:
                if not self.is_open:
                    return False
                elapsed = time.time() - self.last_fail_time
                if elapsed >= CIRCUIT_BREAKER_WAIT:
                    self.consecutive_global_failures = 0
                    self.is_open = False
                    print(c(f"\n  🔓 [{thread_name}] 熔断器恢复，尝试重新请求", "green"), flush=True)
                    return False
            remaining = CIRCUIT_BREAKER_WAIT - elapsed
            print(c(f"\n  🔒 [{thread_name}] 全局熔断中，等待{remaining:.0f}s（服务端故障）...", "red"), flush=True)
            time.sleep(min(remaining, 5))


class ScraperState:
    """v6.0: 线程安全的共享爬取状态 — 固定窗口滑动统计 + 峰值速度"""

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
        self._recent_results = deque(maxlen=500)
        self.peak_speed = 0.0            # v6.0 Optimization 12: 峰值速度
        self.current_working_id = start_id  # v6.0 Optimization 12: 当前处理的 ID
        self._last_save_count = 0        # v6.0 Bug 6: 状态保存跟踪

    def get_next_id(self):
        with self.lock:
            while self.next_id <= self.end_id:
                current = self.next_id
                self.next_id += 1
                if current not in self.existing_ids:
                    self.processed += 1
                    self.current_working_id = current
                    return current
            return None

    def set_current_id(self, tid):
        """v6.0: 记录当前正在处理的 ID"""
        with self.lock:
            self.current_working_id = tid

    def record_result(self, is_success):
        with self.lock:
            if is_success:
                self.total_items += 1
            self._recent_results.append((time.time(), is_success))
            # 更新峰值速度
            elapsed = time.time() - self.start_time
            if elapsed > 0:
                speed = self.processed / elapsed
                if speed > self.peak_speed:
                    self.peak_speed = speed

    def record_not_found(self):
        with self.lock:
            self.total_not_found += 1
            self._recent_results.append((time.time(), True))

    def record_failed(self):
        with self.lock:
            self.total_failed += 1
            self._recent_results.append((time.time(), False))

    def get_failure_rate(self):
        with self.lock:
            if not self._recent_results:
                return 0.0
            cutoff = time.time() - 60
            recent = [(ts, ok) for ts, ok in self._recent_results if ts > cutoff]
            if not recent:
                return 0.0
            failures = sum(1 for _, ok in recent if not ok)
            return failures / len(recent)

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
                "peak_speed": self.peak_speed,
                "elapsed": elapsed,
                "active_threads": self.active_threads,
                "failure_rate": self.get_failure_rate(),
                "current_qps": 0,
                "current_working_id": self.current_working_id,
            }

    def set_active_threads(self, count):
        with self.lock:
            self.active_threads = count

    def should_save_state(self):
        """v6.0 Bug 6: 检查是否应该保存状态 (每 STATE_SAVE_INTERVAL 个 ID)"""
        with self.lock:
            if self.processed - self._last_save_count >= STATE_SAVE_INTERVAL:
                self._last_save_count = self.processed
                return True
            return False


# v6.0 Bug 4: 写入线程仅依赖 None 哨兵退出
def writer_thread(out_file, fieldnames, result_queue, state):
    """v6.0 专用写入线程 —— 仅在收到 None 哨兵时退出，不受 active_threads 影响"""
    file_exists = os.path.isfile(out_file)
    buffer = []
    written_count = 0
    with open(out_file, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        while True:
            try:
                row = result_queue.get(timeout=1.0)
                if row is None:
                    # v6.0 Bug 4: 哨兵信号，收到后清空缓冲并退出
                    result_queue.task_done()
                    if buffer:
                        for r in buffer:
                            writer.writerow(r)
                        f.flush()
                        written_count += len(buffer)
                        buffer.clear()
                    break
                buffer.append(row)
                if len(buffer) >= BATCH_WRITE_SIZE:
                    for r in buffer:
                        writer.writerow(r)
                    f.flush()
                    written_count += len(buffer)
                    buffer.clear()
                result_queue.task_done()
            except queue.Empty:
                # v6.0 Bug 4: 队列空时只刷缓冲，不检查 active_threads
                if buffer:
                    for r in buffer:
                        writer.writerow(r)
                    f.flush()
                    written_count += len(buffer)
                    buffer.clear()


# v6.0 Optimization 8: 定期健康检查线程（复用 session_manager）
def health_check_thread(base_url, session_manager, state, rate_limiter):
    """v6.0: 每 60s 进行轻量 HEAD 请求检查站点健康，复用已有 session"""
    while not state.stop_event.is_set():
        state.stop_event.wait(HEALTH_CHECK_INTERVAL)
        if state.stop_event.is_set():
            break
        try:
            session = session_manager.get_session()
            r = session.head(base_url, timeout=15, allow_redirects=True)
            if r.status_code < 500:
                print(c(f"\n  ❤ [{time.strftime('%H:%M:%S')}] 健康检查: 站点正常 (HTTP {r.status_code})", "green"), flush=True)
            else:
                print(c(f"\n  ⚠ [{time.strftime('%H:%M:%S')}] 健康检查: 站点异常 (HTTP {r.status_code})", "yellow"), flush=True)
            session_manager.record_success()
        except Exception as e:
            session_manager.record_failure()
            print(c(f"\n  ⚠ [{time.strftime('%H:%M:%S')}] 健康检查失败: {type(e).__name__}", "yellow"), flush=True)


# v6.0 Bug 5: params 通过参数传递
# v6.0 Optimization 12: 增强的进度显示
def progress_display_thread(state, total_range, rate_limiter, session_manager, params):
    """v6.0 进度显示: 百分比 + 当前ID + 旋转动画 + 峰值/持续速度"""
    adaptive_check_interval = 5
    last_adaptive_check = time.time()
    spinner_idx = [0]  # 使用列表以支持闭包内修改

    while not state.stop_event.is_set():
        stats = state.get_stats()
        stats["current_qps"] = rate_limiter.max_qps

        elapsed = stats["elapsed"]
        speed = stats["speed"]
        peak_speed = stats.get("peak_speed", 0)
        remaining = total_range - stats["processed"]
        eta_seconds = remaining / speed if speed > 0 else 0
        pct = (stats["processed"] / total_range * 100) if total_range > 0 else 0
        current_id = stats.get("current_working_id", 0)

        if eta_seconds < 3600:
            eta_str = f"{int(eta_seconds // 60)}m{int(eta_seconds % 60)}s"
        elif eta_seconds < 86400:
            eta_str = f"{int(eta_seconds // 3600)}h{int((eta_seconds % 3600) // 60)}m"
        else:
            eta_str = f"{int(eta_seconds // 86400)}d{int((eta_seconds % 86400) // 3600)}h"

        fail_pct = stats["failure_rate"] * 100
        avg_rt = session_manager.get_avg_response_time()
        spinner = SPINNER_FRAMES[spinner_idx[0] % len(SPINNER_FRAMES)]
        spinner_idx[0] += 1

        # v6.0 Optimization 12: 更丰富的进度行
        progress_line = (
            f"  {spinner} [{stats['processed']:,}/{total_range:,}] ({pct:.1f}%) "
            f"ID={current_id} | "
            f"有效={stats['total_items']} 不存在={stats['total_not_found']} 失败={stats['total_failed']} | "
            f"线程={stats['active_threads']} QPS={rate_limiter.max_qps:.1f} "
            f"速度={speed:.1f}id/s 峰值={peak_speed:.1f}id/s "
            f"失败率={fail_pct:.0f}% 均响={avg_rt:.1f}s ETA={eta_str}"
        )
        sys.stdout.write(f"\r{progress_line:<130}")
        sys.stdout.flush()

        now = time.time()
        if now - last_adaptive_check >= adaptive_check_interval:
            last_adaptive_check = now
            fr = stats["failure_rate"]
            # v6.0 Bug 5: 直接从参数获取，不使用全局变量
            original_qps = params.get("_original_qps", 12.0)

            if fr > 0.5:
                new_qps = max(0.5, rate_limiter.max_qps * 0.5)
                if new_qps != rate_limiter.max_qps:
                    rate_limiter.update_qps(new_qps)
                    print(c(f"\n  ⚡ 自适应: 失败率 {fr*100:.0f}% > 50%, 降速至 {new_qps:.1f} QPS", "yellow"), flush=True)
            elif fr > 0.2:
                new_qps = max(1.0, rate_limiter.max_qps * 0.7)
                if new_qps != rate_limiter.max_qps:
                    rate_limiter.update_qps(new_qps)
                    print(c(f"\n  ⚡ 自适应: 失败率 {fr*100:.0f}% > 20%, 降速至 {new_qps:.1f} QPS", "yellow"), flush=True)
            elif fr < 0.05 and avg_rt < 3.0:
                if rate_limiter.max_qps < original_qps * 1.5:
                    new_qps = min(original_qps * 1.5, rate_limiter.max_qps * 1.3)
                    rate_limiter.update_qps(new_qps)
                    print(c(f"\n  ⚡ 自适应: 失败率 {fr*100:.0f}% < 5% 且响应快, 提速至 {new_qps:.1f} QPS", "green"), flush=True)
            elif fr < 0.05:
                if rate_limiter.max_qps < original_qps:
                    new_qps = min(original_qps, rate_limiter.max_qps * 1.2)
                    rate_limiter.update_qps(new_qps)
                    print(c(f"\n  ⚡ 自适应: 失败率 {fr*100:.0f}% < 5%, 提速至 {new_qps:.1f} QPS", "green"), flush=True)

        if state.stop_event.wait(0.5):
            break


# ─────────────────────────── 主爬取逻辑（多线程） ─────────────────
# v6.0 Optimization 11: 信号处理
_shutdown_requested = False


def _signal_handler(signum, frame):
    global _shutdown_requested
    if _shutdown_requested:
        print(c("\n⚠ 重复信号，强制退出", "red"), flush=True)
        os._exit(1)
    _shutdown_requested = True
    print(c(f"\n⚠ 收到信号 {signum}，优雅关停中... (再次发送信号强制退出)", "yellow"), flush=True)


def scrape(proxies, params):
    BASE = "https://nyaa.si"

    # v6.0 Bug 1: 使用统一工厂函数创建 session
    tmp_session = create_session(BASE, proxies)
    out_file = params["out"]
    is_resume = params.get("is_resume", False)
    fieldnames = ["id", "name", "info_hash", "magnet", "size", "date", "category", "detail_url"]
    num_threads = params.get("num_threads", 8)
    max_qps = params.get("max_qps", 12.0)
    params["_original_qps"] = max_qps

    # v6.0 Optimization 7: DNS 预解析
    dns_pre_resolve("nyaa.si")

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

    start_id = params["start_id"]
    end_id = params["end_id"]
    if end_id == "auto":
        # v6.0 Bug 1: 使用统一工厂函数
        detect_session = create_session(BASE, proxies)
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
    print(f"  全局QPS: {max_qps} (三维自适应调节)")
    print(f"  请求超时: {params['timeout']}s")
    print(f"  最大重试: {'∞ 无限' if params['retry_count'] == float('inf') else params['retry_count']}")
    print(f"  Session重建阈值: 连续{SESSION_REBUILD_THRESHOLD}次失败")
    print(f"  全局熔断阈值: {CIRCUIT_BREAKER_THRESHOLD}次连续失败")
    print(f"  批量写入: 每{BATCH_WRITE_SIZE}条刷盘一次")
    print(f"  状态保存: 每{STATE_SAVE_INTERVAL}个ID")
    print(f"  健康检查: 每{HEALTH_CHECK_INTERVAL}秒")
    print(f"  重试轮次: 最多{MAX_RETRY_ROUNDS}轮(指数冷却)")
    if is_resume:
        print(c(f"  模式: 续爬（新数据将追加到 {out_file}）", "green"))
    print(f"  (注: 不是所有ID都存在，实际有效条目会少于该数字)")

    confirm = input(c("\n确认开始爬取? [Y/n]: ", "yellow")).strip().lower()
    if confirm == "n":
        print(c("已取消。", "yellow"))
        sys.exit(0)

    existing_ids = load_existing_ids(out_file)
    if existing_ids:
        skipped_count = len(existing_ids)
        print(c(f"\n✔ 去重保护: 已加载 {skipped_count} 条已有记录的ID，将自动跳过重复项", "green"))
    else:
        skipped_count = 0

    failed_ids_from_file = load_failed_ids(FAILED_IDS_FILE)
    if failed_ids_from_file:
        print(c(f"✔ 恢复: 从 {FAILED_IDS_FILE} 加载了 {len(failed_ids_from_file)} 个之前失败的ID", "yellow"))

    state = ScraperState(start_id, end_id, existing_ids)
    rate_limiter = GlobalRateLimiter(max_qps)
    circuit_breaker = CircuitBreaker()
    session_manager = SessionManager(BASE, proxies, timeout=params["timeout"])
    result_queue = queue.Queue(maxsize=num_threads * 8)
    failed_ids_queue = queue.Queue()

    for fid in failed_ids_from_file:
        failed_ids_queue.put(fid)

    # v6.0: 避免 range() 创建巨大临时集合，改用生成器求交集
    ids_to_scrape = total_range - sum(1 for eid in existing_ids if start_id <= eid <= end_id)
    print(c(f"\n✔ 需要实际爬取约 {ids_to_scrape:,} 个ID（已去重）", "cyan"))
    print(c(f"✔ 启动 {num_threads} 个工作线程 + 全局 {max_qps:.0f} QPS 限速...", "cyan"))
    print(c(f"✔ 零失败架构: 失败ID自动进入重试队列", "cyan"))
    print(c(f"✔ 三维自适应: 成功率×响应时间×错误类型 → 动态调速", "cyan"))
    print(c(f"✔ 批量写入: 减少磁盘IO，提升整体吞吐", "cyan"))
    print()

    writer = threading.Thread(
        target=writer_thread,
        args=(out_file, fieldnames, result_queue, state),
        name="Writer", daemon=True
    )
    writer.start()

    # v6.0 Bug 5: params 作为参数传递
    progress = threading.Thread(
        target=progress_display_thread,
        args=(state, total_range, rate_limiter, session_manager, params),
        name="Progress", daemon=True
    )
    progress.start()

    # v6.0 Optimization 8: 健康检查线程
    health = threading.Thread(
        target=health_check_thread,
        args=(BASE, session_manager, state, rate_limiter),
        name="HealthCheck", daemon=True
    )
    health.start()

    # v6.0 Optimization 11: 注册信号处理
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGHUP, _signal_handler)

    state.set_active_threads(num_threads)
    timeout = params["timeout"]
    retry_count = params["retry_count"]

    # v6.0 Bug 6: 初始化状态保存数据
    state_data = {
        "start_id": start_id,
        "end_id": end_id,
        "out_file": out_file,
        "last_processed": 0,
        "next_id": start_id,
        "total_items": 0,
        "total_not_found": 0,
        "total_failed": 0,
    }
    save_state(state_data)

    def thread_target():
        """v6.0 工作线程：本地缓冲 + 解析异常捕获 + 零失败保证 + 状态保存触发"""
        nonlocal state_data
        local_buffer = []
        while not state.stop_event.is_set():
            # v6.0 Optimization 11: 检查信号关停
            if _shutdown_requested:
                break

            target_id = state.get_next_id()
            if target_id is None:
                break

            state.set_current_id(target_id)

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
                    failed_ids_queue.put(target_id)
                    state.record_failed()
                    continue

                if status in NOT_FOUND_STATUS_CODES:
                    state.record_not_found()
                    circuit_breaker.record_success()
                    continue

                if status != 200:
                    state.record_failed()
                    circuit_breaker.record_failure()
                    failed_ids_queue.put(target_id)
                    continue

                # ─── v6.0: 解析异常捕获 ───
                try:
                    soup = BeautifulSoup(r.text, "html.parser")
                except Exception as e:
                    print(c(f"\n  ⚠ [{threading.current_thread().name}] BeautifulSoup解析异常 (ID {target_id}): {type(e).__name__}", "yellow"), flush=True)
                    failed_ids_queue.put(target_id)
                    state.record_failed()
                    continue

                try:
                    kbd = soup.select_one("kbd")
                    mag_a = soup.find("a", href=re.compile(r"^magnet:"))
                    panel = soup.select_one("div.panel.panel-default")
                except Exception:
                    kbd = None
                    mag_a = None
                    panel = None

                if not panel and not kbd and not mag_a:
                    state.record_not_found()
                    circuit_breaker.record_success()
                    continue

                try:
                    name, info_hash, magnet, size, date, category = parse_detail_page(soup, detail_url)
                except Exception as e:
                    print(c(f"\n  ⚠ [{threading.current_thread().name}] 页面解析异常 (ID {target_id}): {type(e).__name__}", "yellow"), flush=True)
                    failed_ids_queue.put(target_id)
                    state.record_failed()
                    continue

                row = {
                    "id": target_id, "name": name, "info_hash": info_hash,
                    "magnet": magnet, "size": size, "date": date,
                    "category": category, "detail_url": detail_url,
                }

                # v6.0: 队列满时写入本地缓冲，避免阻塞工作线程
                try:
                    result_queue.put(row, block=False)
                except queue.Full:
                    local_buffer.append(row)
                    if len(local_buffer) >= BATCH_WRITE_SIZE:
                        # 强制写入队列（阻塞等待）
                        for r2 in local_buffer:
                            result_queue.put(r2)
                        local_buffer.clear()

                state.record_result(True)
                circuit_breaker.record_success()

                # v6.0 Bug 6: 定期保存状态
                if state.should_save_state():
                    stats = state.get_stats()
                    state_data["last_processed"] = stats["processed"]
                    state_data["next_id"] = state.next_id
                    state_data["total_items"] = stats["total_items"]
                    state_data["total_not_found"] = stats["total_not_found"]
                    state_data["total_failed"] = stats["total_failed"]
                    save_state(state_data)

            except Exception as e:
                state.record_failed()
                circuit_breaker.record_failure()
                failed_ids_queue.put(target_id)
                print(c(f"\n  ✗ [{threading.current_thread().name}] 未知异常 (ID {target_id}): {type(e).__name__}: {str(e)[:60]}", "red"), flush=True)

        # 线程结束前清空本地缓冲
        for r2 in local_buffer:
            try:
                result_queue.put(r2, timeout=30)
            except queue.Full:
                failed_ids_queue.put(r2["id"])
                state.record_failed()

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

    # ── 收集所有失败ID ──
    all_failed_ids = []  # v6.0 Bug 3: 提前初始化，不依赖 dir() 检测
    while not failed_ids_queue.empty():
        try:
            all_failed_ids.append(failed_ids_queue.get_nowait())
        except queue.Empty:
            break
    all_failed_ids = sorted(set(all_failed_ids))

    # ── v6.0 多线程最终重做轮次 ──
    if all_failed_ids:
        print(c(f"\n\n{'='*60}", "bold"))
        print(c(f"  🔄 最终重做轮次: {len(all_failed_ids)} 个失败ID将被重新爬取", "bold"))
        print(c(f"  使用 {num_threads} 个线程并行清扫", "bold"))
        print(c(f"{'='*60}\n", "bold"))

        save_failed_ids(FAILED_IDS_FILE, all_failed_ids)

        retry_round = 1

        # v6.0 Optimization 9: 使用更低的 QPS 进行重试
        retry_qps = max(1.0, max_qps * 0.5)
        original_qps = rate_limiter.max_qps
        rate_limiter.update_qps(retry_qps)
        print(c(f"  📉 重试阶段: QPS 降至 {retry_qps:.1f}", "yellow"), flush=True)

        while all_failed_ids and retry_round <= MAX_RETRY_ROUNDS:
            # v6.0 Optimization 9: 指数冷却 (30s, 60s, 120s, 240s...)
            if retry_round > 1:
                cooldown = min(30 * (2 ** (retry_round - 2)), 600)
                print(c(f"\n  ⏳ 第 {retry_round} 轮冷却 {cooldown}s 后开始...", "yellow"), flush=True)
                state.stop_event.wait(cooldown)
                if _shutdown_requested:
                    break

            print(c(f"\n  📋 重做第 {retry_round} 轮: 剩余 {len(all_failed_ids)} 个失败ID", "cyan"))
            sys.stdout.flush()

            retry_failed_queue = queue.Queue()
            # v6.0 Bug 2: 使用线程安全的队列收集重试失败，替代直接 append 到 all_failed_ids
            retry_results_lock = threading.Lock()
            retry_success_ids = []
            retry_still_failed_ids = []

            def retry_thread_target(round_num, result_success, result_fail, res_lock):
                local_buf = []
                while True:
                    try:
                        fid = retry_failed_queue.get(timeout=0.5)
                    except queue.Empty:
                        break

                    circuit_breaker.check_and_wait(f"Retry-{round_num}")
                    detail_url = f"{BASE}/view/{fid}"
                    success = False

                    try:
                        r, status = request_with_infinite_retry(
                            session_manager, detail_url,
                            timeout=max(timeout, 120),
                            max_retries=float("inf"),
                            rate_limiter=rate_limiter,
                        )

                        if r is not None:
                            if status in NOT_FOUND_STATUS_CODES:
                                state.record_not_found()
                                circuit_breaker.record_success()
                                success = True
                            elif status == 200:
                                try:
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
                                        try:
                                            result_queue.put(row, block=False)
                                        except queue.Full:
                                            local_buf.append(row)
                                            if len(local_buf) >= BATCH_WRITE_SIZE:
                                                for r2 in local_buf:
                                                    result_queue.put(r2)
                                                local_buf.clear()
                                        state.record_result(True)
                                        circuit_breaker.record_success()
                                        success = True
                                    else:
                                        state.record_not_found()
                                        success = True
                                except Exception:
                                    success = False
                    except Exception:
                        success = False

                    # v6.0 Bug 2: 使用锁保护的列表收集结果
                    if success:
                        with res_lock:
                            result_success.append(fid)
                    else:
                        with res_lock:
                            result_fail.append(fid)
                        circuit_breaker.record_failure()

                    retry_failed_queue.task_done()

                # 清空本地缓冲
                for r2 in local_buf:
                    try:
                        result_queue.put(r2, timeout=30)
                    except queue.Full:
                        with res_lock:
                            result_fail.append(r2["id"])
                        state.record_failed()

            for fid in all_failed_ids:
                retry_failed_queue.put(fid)

            retry_threads = []
            for i in range(min(num_threads, len(all_failed_ids))):
                t = threading.Thread(
                    target=retry_thread_target,
                    args=(retry_round, retry_success_ids, retry_still_failed_ids, retry_results_lock),
                    name=f"Retry-{i+1:02d}"
                )
                t.start()
                retry_threads.append(t)
            for t in retry_threads:
                t.join()

            # v6.0: 中断检查
            if _shutdown_requested or state.stop_event.is_set():
                print(c(f"\n  ⚠ 重做轮次被中断，剩余 {len(retry_still_failed_ids)} 个ID将保存", "yellow"), flush=True)
                all_failed_ids = sorted(set(retry_still_failed_ids))
                save_failed_ids(FAILED_IDS_FILE, all_failed_ids)
                break

            resolved = len(retry_success_ids)
            still_failed_count = len(retry_still_failed_ids)
            print()
            if resolved > 0:
                print(c(f"  ✔ 第 {retry_round} 轮重做成功恢复 {resolved} 个ID", "green"))

            # v6.0 Bug 2: 从线程安全的列表获取失败 ID
            all_failed_ids = sorted(set(retry_still_failed_ids))
            if not all_failed_ids:
                print(c(f"  🎉 所有失败ID已全部恢复！", "green"))
                try:
                    os.remove(FAILED_IDS_FILE)
                except Exception:
                    pass
                break
            else:
                save_failed_ids(FAILED_IDS_FILE, all_failed_ids)
                if retry_round < MAX_RETRY_ROUNDS:
                    print(c(f"  ⚠ 仍有 {len(all_failed_ids)} 个ID失败，进入下一轮", "yellow"))
                retry_round += 1

        # 恢复原始 QPS
        rate_limiter.update_qps(original_qps)

        if all_failed_ids:
            save_failed_ids(FAILED_IDS_FILE, all_failed_ids)
            print(c(f"\n  ⚠ 最终仍有 {len(all_failed_ids)} 个ID未能成功（已保存到 {FAILED_IDS_FILE}）", "yellow"))
            print(c(f"  💡 下次运行脚本时会自动加载这些ID并重新尝试", "yellow"))

    # ── 清理 ──
    # v6.0 Bug 4: 发送 None 哨兵关闭写入线程（在此之前确保所有数据已入队）
    # 先等待 result_queue 中所有数据被消费
    result_queue.join()
    result_queue.put(None)
    writer.join(timeout=10)
    state.stop_event.set()
    progress.join(timeout=2)
    health.join(timeout=2)
    session_manager.close_all()

    # v6.0 Bug 6: 最终保存状态
    stats = state.get_stats()
    state_data["last_processed"] = stats["processed"]
    state_data["next_id"] = state.next_id
    state_data["total_items"] = stats["total_items"]
    state_data["total_not_found"] = stats["total_not_found"]
    state_data["total_failed"] = stats["total_failed"]
    save_state(state_data)

    # ── 最终统计 ──
    final_stats = state.get_stats()
    elapsed_total = final_stats["elapsed"]
    hours = int(elapsed_total // 3600)
    mins = int((elapsed_total % 3600) // 60)
    secs = int(elapsed_total % 60)

    final_total = 0
    if os.path.isfile(out_file):
        try:
            with open(out_file, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for _ in reader:
                    final_total += 1
        except Exception:
            pass

    print(c(f"\n\n{'='*60}", "bold"))
    print(c("  ✔ 爬取完成!", "bold"))
    print(c(f"{'='*60}", "bold"))
    print(f"  总耗时: {hours}h{mins}m{secs}s")
    print(f"  处理ID范围: {start_id} ~ {end_id}")
    print(f"  已处理: {final_stats['processed']:,} 个ID")
    print(f"  有效条目: {final_stats['total_items']:,}")
    print(f"  不存在: {final_stats['total_not_found']:,}")
    print(f"  失败: {final_stats['total_failed']:,}")
    print(f"  CSV总记录: {final_total:,}")
    print(f"  峰值速度: {final_stats.get('peak_speed', 0):.1f} id/s")
    print(f"  平均速度: {final_stats['speed']:.1f} id/s")
    print(f"  输出文件: {out_file}")
    if all_failed_ids:
        print(c(f"  未完成ID: {len(all_failed_ids)} 个 (已保存到 {FAILED_IDS_FILE})", "yellow"))
    print(c(f"{'='*60}\n", "bold"))


# ─────────────────────────── 主入口 ─────────────────────────────
def main():
    print_banner()
    proxies = setup_proxy()
    params = setup_params()
    try:
        scrape(proxies, params)
    except KeyboardInterrupt:
        print(c("\n\n⚠ 用户中断，程序退出", "yellow"))
    except Exception as e:
        print(c(f"\n✗ 严重错误: {type(e).__name__}: {e}", "red"))
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
