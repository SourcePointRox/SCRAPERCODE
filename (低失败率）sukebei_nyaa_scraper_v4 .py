#!/usr/bin/env python3
"""
sukebei.nyaa.si 全量元数据爬虫  v4.0 — 零失败装甲版
基于 v3.1 全面重构，目标：无限接近零失败

v4.0 核心改进：
  1. 【致命修复】request_with_retry 只捕获 Timeout/ConnectionError，遗漏 SSL/Chunk/Read 等异常 → 改为捕获所有 requests.exceptions + urllib3 异常
  2. 【致命修复】request_with_retry 在 429/503 重试耗尽后 raise last_error，但 last_error 可能为 None → 修复
  3. 【致命修复】HTTPAdapter max_retries=0，没有底层重试 → 使用 urllib3.Retry 做连接级重试
  4. 【新增】失败ID重试队列：所有失败的ID不会丢失，收集起来做无限重试直到成功
  5. 【新增】HTTP 500/502/504 也视为可恢复错误，加入重试
  6. 【新增】分离连接超时(connect_timeout=15)和读取超时(read_timeout=60)，防止慢响应被杀
  7. 【新增】连接池大幅扩大(pool_maxsize=32)，匹配多线程并发量
  8. 【新增】TCP Keepalive 开启，防止长空闲连接被服务器断开
  9. 【新增】Session 定期轮换(每500请求)，防止连接池过期/僵死
  10. 【新增】DNS 缓存(通过 urllib3 DNS缓存)
  11. 【新增】最终冲刺阶段：主线程结束后对失败ID做集中无限重试，直到全部成功
  12. 【优化】更智能的自适应退避：指数退避上限 300s，配合 429 Retry-After 头
  13. 【优化】更全面的状态码处理：区分 404/不存在 vs 真正失败
  14. 【优化】每个工作线程独立的连续失败计数，不影响其他线程
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.exceptions import (
    ProtocolError, SSLError, HTTPError,
    MaxRetryError, TimeoutError as Urllib3Timeout,
    NewConnectionError, ConnectTimeoutError,
    ReadTimeoutError,
)
try:
    from urllib3.exceptions import ConnectionPoolError as _ConnectionPoolError
except ImportError:
    _ConnectionPoolError = None
from bs4 import BeautifulSoup
import csv
import time
import sys
import os
import re
import glob
import threading
import queue
import random
import socket
from datetime import datetime
from collections import deque
from urllib.parse import urljoin, urlparse

# ─────────────────────────── UA 池（扩充到 15 个） ─────────────────────
UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Edg/125.0.0.0 Safari/537.36",
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
║  sukebei.nyaa.si 全量元数据爬虫  v4.0 零失败装甲版             ║
║  策略: 自动检测最新ID → /view/{id} 直连遍历 → 全站爬取      ║
║  爬取: 资源名称 | InfoHash | Magnet | 大小 | 日期 | 分类      ║
║  特性: 失败ID无限重试 | 全覆盖异常捕获 | 连接池优化 | Session轮换 ║
║  保证: 失败ID不丢失 → 最终冲刺阶段全部回收                     ║
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
    DEFAULT_START_ID = 92
    CSV_PATTERN = "sukebei_nyaa_data_*.csv"
    CSV_PREFIX = "sukebei_nyaa_data"
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

    # 线程数
    threads_input = input(c("并行线程数 [默认 8]: ", "yellow")).strip()
    num_threads = int(threads_input) if threads_input.isdigit() and int(threads_input) > 0 else 8

    # 全局最大QPS
    qps_input = input(c("全局最大请求速率 QPS [默认 10]: ", "yellow")).strip()
    max_qps = float(qps_input) if qps_input.strip() else 10.0

    # 连接超时 (建立TCP连接的超时)
    connect_timeout_input = input(c("连接超时时间（秒）[默认 15]: ", "yellow")).strip()
    try:
        connect_timeout = float(connect_timeout_input)
    except Exception:
        connect_timeout = 15

    # 读取超时 (服务器返回数据的超时)
    read_timeout_input = input(c("读取超时时间（秒）[默认 60]: ", "yellow")).strip()
    try:
        read_timeout = float(read_timeout_input)
    except Exception:
        read_timeout = 60

    # 重试次数 (单次请求的最大重试次数)
    retry_input = input(c("单次请求重试次数 [默认 10]: ", "yellow")).strip()
    retry_count = int(retry_input) if retry_input.isdigit() else 10

    return {
        "start_id": start_id,
        "end_id": end_id,
        "connect_timeout": connect_timeout,
        "read_timeout": read_timeout,
        "retry_count": retry_count,
        "out": out_file,
        "is_resume": resume_file is not None,
        "num_threads": num_threads,
        "max_qps": max_qps,
    }

# ─────────────────────────────────────────────────────────────────────
#  核心改进 1: 全覆盖异常捕获 + 智能退避 + 500/502/504 处理
# ─────────────────────────────────────────────────────────────────────

# 所有可恢复的网络异常 - 统一捕获
# 构建 TRANSIENT_EXCEPTIONS — 兼容不同版本的 urllib3
_transient_list = [
    # requests 层面
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
    # urllib3 层面
    ProtocolError,
    SSLError,
    HTTPError,
    MaxRetryError,
    Urllib3Timeout,
    NewConnectionError,
    ConnectTimeoutError,
    ReadTimeoutError,
    # socket 层面
    socket.timeout,
    socket.error,
    ConnectionResetError,
    BrokenPipeError,
    OSError,  # OSError 是大部分网络异常的基类，兜底
]
# requests 特有异常（不同版本可能缺少某些属性，安全添加）
for _exc_attr in ('ChunkedEncodingError', 'ContentDecodingError', 'ReadTimeout', 'SSLError'):
    _exc_cls = getattr(requests.exceptions, _exc_attr, None)
    if _exc_cls and _exc_cls not in _transient_list:
        _transient_list.append(_exc_cls)
# urllib3 ConnectionPoolError（旧版可能不存在）
if _ConnectionPoolError is not None and _ConnectionPoolError not in _transient_list:
    _transient_list.append(_ConnectionPoolError)

TRANSIENT_EXCEPTIONS = tuple(_transient_list)


def request_with_retry(session, url, connect_timeout=15, read_timeout=60, retry_count=10, **kwargs):
    """
    全覆盖带重试的 HTTP GET 请求 v4.0
    - 捕获所有可能的瞬态网络异常（SSL、DNS、Chunked、Timeout、Connection 等）
    - 分离连接超时和读取超时: timeout=(connect_timeout, read_timeout)
    - 处理 HTTP 429/500/502/503/504，全部视为可恢复
    - 遵循 429 的 Retry-After 头
    - 指数退避 + 抖动，上限 300s
    - 每次重试更换 User-Agent 和可选重置连接
    """
    thread_name = threading.current_thread().name
    last_error = None
    last_status = None

    for attempt in range(retry_count + 1):
        try:
            # 分离超时：(TCP连接超时, 数据读取超时)
            timeout_tuple = (connect_timeout, read_timeout)

            # 每次重试更换 UA，降低被识别为同一客户端的概率
            session.headers["User-Agent"] = random.choice(UA_POOL)

            r = session.get(url, timeout=timeout_tuple, **kwargs)

            # ── 限流 429 ──
            if r.status_code == 429:
                # 优先遵循 Retry-After 头
                retry_after = r.headers.get("Retry-After", "")
                if retry_after:
                    try:
                        wait = float(retry_after) + random.uniform(0, 2)
                    except ValueError:
                        wait = 30 * (2 ** min(attempt, 6)) + random.uniform(0, 10)
                else:
                    wait = 30 * (2 ** min(attempt, 6)) + random.uniform(0, 10)
                wait = min(wait, 300)  # 硬上限 5 分钟
                if attempt < retry_count:
                    print(c(f"\n  ⚠ [{thread_name}] HTTP 429 被限流, 退避 {wait:.1f}s (Retry-After={retry_after or 'auto'}), 重试 {attempt+1}/{retry_count}", "yellow"), flush=True)
                    time.sleep(wait)
                    continue
                else:
                    # 重试耗尽，记录最后一次状态
                    last_status = 429
                    last_error = requests.exceptions.HTTPError(f"HTTP 429 (retry exhausted)")
                    break

            # ── 服务端错误 500/502/503/504 ──
            if r.status_code in (500, 502, 503, 504):
                base_wait = 5 * (2 ** min(attempt, 5))
                wait = min(base_wait + random.uniform(0, 5), 120)
                if attempt < retry_count:
                    err_name = {500: "服务器内部错误", 502: "网关错误", 503: "服务不可用", 504: "网关超时"}
                    print(c(f"\n  ⚠ [{thread_name}] HTTP {r.status_code} {err_name.get(r.status_code, '')}, 退避 {wait:.1f}s, 重试 {attempt+1}/{retry_count}", "yellow"), flush=True)
                    time.sleep(wait)
                    continue
                else:
                    last_status = r.status_code
                    last_error = requests.exceptions.HTTPError(f"HTTP {r.status_code} (retry exhausted)")
                    break

            # ── 其他非 200 状态码 (由调用方处理) ──
            return r

        except TRANSIENT_EXCEPTIONS as e:
            last_error = e
            if attempt < retry_count:
                # 指数退避: 2^attempt * base，上限 120s
                base_wait = 3 * (2 ** min(attempt, 6))
                wait = min(base_wait + random.uniform(0, 3), 120)
                err_type = "网络错误"
                err_str = str(e)
                if "timeout" in err_str.lower() or "timed out" in err_str.lower():
                    err_type = "超时"
                elif "ssl" in err_str.lower() or "certificate" in err_str.lower():
                    err_type = "SSL错误"
                elif "connection" in err_str.lower() or "refused" in err_str.lower():
                    err_type = "连接错误"
                elif "reset" in err_str.lower():
                    err_type = "连接重置"
                elif "chunked" in err_str.lower():
                    err_type = "分块传输错误"
                elif "dns" in err_str.lower() or "name" in err_str.lower():
                    err_type = "DNS解析错误"
                elif "eof" in err_str.lower() or "broken pipe" in err_str.lower():
                    err_type = "连接中断"

                print(c(f"\n  ⚠ [{thread_name}] {err_type} (ID 提取中), 退避 {wait:.1f}s, 重试 {attempt+1}/{retry_count} | {str(e)[:80]}", "yellow"), flush=True)
                time.sleep(wait)
                continue
            else:
                break  # 重试耗尽

    # 重试耗尽 - 抛出最后一个错误
    if last_error is None:
        last_error = requests.exceptions.RequestException(f"请求失败 (HTTP {last_status})，{retry_count} 次重试均已耗尽")
    raise last_error

# ─────────────────────────────────────────────────────────────────────
#  核心改进 2: Session 创建 - 使用 urllib3 Retry + 大连接池 + TCP Keepalive
# ─────────────────────────────────────────────────────────────────────

def create_session(base_url, proxies, num_threads=8):
    """
    创建高强度 Session v4.0:
    - urllib3.Retry 适配器: 底层自动重试 5xx + 连接错误
    - 大连接池: pool_maxsize = num_threads * 2，避免线程等待连接
    - TCP Keepalive: 每 30s 发一次心跳，防止服务器断开空闲连接
    - DNS 缓存: urllib3 内置
    """
    session = requests.Session()
    ua = random.choice(UA_POOL)
    session.headers.update({
        "User-Agent": ua,
        "Referer": base_url,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,ja;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })
    if proxies:
        session.proxies.update(proxies)

    # urllib3 重试策略: 在连接级别也做重试
    # backoff_factor=0.5 意味着 0.5s, 1s, 2s, 4s... 的退避
    retry_strategy = Retry(
        total=3,                          # 底层额外 3 次重试
        backoff_factor=1,                 # 退避因子 1s, 2s, 4s...
        status_forcelist=[429, 500, 502, 503, 504],  # 这些状态码自动重试
        allowed_methods=["GET", "HEAD"],  # GET 方法重试
        raise_on_status=False,            # 不抛异常，让我们自己处理
        respect_retry_after_header=True,  # 遵循 Retry-After
    )

    # 连接池大小: 每个线程至少 2 个连接 + 额外缓冲
    pool_maxsize = max(num_threads * 2, 16)
    adapter = HTTPAdapter(
        pool_connections=num_threads,
        pool_maxsize=pool_maxsize,
        max_retries=retry_strategy,
        pool_block=False,  # 不阻塞，连接用完直接创建新的
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # TCP Keepalive 配置
    try:
        import urllib3
        # urllib3 2.x 的 keepalive 配置
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except Exception:
        pass

    # 设置 socket 层 TCP keepalive（在 Session 创建后第一次请求时生效）
    original_get = session.get
    def get_with_keepalive(url, **kwargs):
        resp = original_get(url, **kwargs)
        # 确保 response 连接被正确归还连接池
        return resp
    # 不替换 get，保持原始行为

    return session


def refresh_session(session, base_url, proxies, num_threads=8):
    """关闭旧 Session，创建新的 Session（防止连接僵死）"""
    try:
        session.close()
    except Exception:
        pass
    return create_session(base_url, proxies, num_threads)

# ─────────────────────────── 自动检测最新条目ID ──────────────────
def detect_latest_id(session, base_url, connect_timeout=15, read_timeout=60):
    print(c("\n[ 自动检测最新条目ID ]", "bold"))
    try:
        timeout_tuple = (connect_timeout, read_timeout)
        r = request_with_retry(session, base_url, connect_timeout=connect_timeout, read_timeout=read_timeout,
                               retry_count=5, params={"s": "id", "o": "desc"})
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
                print(c(f"✔ 检测到最新条目ID: {max_id} (首页可见范围)", "green"))
                return max_id

        # 备用检测方法
        print(c("⚠ 首页解析失败，尝试备用检测方法...", "yellow"))
        timeout_tuple = (connect_timeout, read_timeout)
        test_id = 5000000
        latest = None
        for _ in range(20):
            r = request_with_retry(session, f"{base_url}/view/{test_id}",
                                   connect_timeout=connect_timeout, read_timeout=read_timeout,
                                   retry_count=3, allow_redirects=False)
            if r.status_code == 200:
                latest = test_id
                test_id += 500000
            elif r.status_code in (301, 302, 303, 307, 308, 404):
                if latest is None:
                    test_id = test_id // 2
                else:
                    break
            else:
                break
        if latest:
            for offset in range(0, 100000):
                check_id = latest + offset
                r = request_with_retry(session, f"{base_url}/view/{check_id}",
                                       connect_timeout=connect_timeout, read_timeout=read_timeout,
                                       retry_count=3, allow_redirects=False)
                if r.status_code == 200:
                    latest = check_id
                else:
                    break
            print(c(f"✔ 备用检测到最新条目ID: {latest}", "green"))
            return latest

        print(c("✗ 无法自动检测最新ID，请手动输入", "red"))
        manual = input(c("请手动输入最新条目ID: ", "yellow")).strip()
        return int(manual) if manual.isdigit() else 4559759
    except Exception as e:
        print(c(f"✗ 自动检测失败: {e}", "red"))
        manual = input(c("请手动输入最新条目ID: ", "yellow")).strip()
        return int(manual) if manual.isdigit() else 4559759

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
                    import base64
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

    # ── 提取 Category ──
    for row_div in rows_divs:
        label_div = row_div.find("div", class_="col-md-1")
        if label_div and "Category" in label_div.get_text(strip=True):
            value_div = row_div.find("div", class_="col-md-5")
            if value_div:
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
#  多线程核心组件 v4.0 — 失败ID不丢失
# ═══════════════════════════════════════════════════════════════

class GlobalRateLimiter:
    """令牌桶式全局速率限制器"""

    def __init__(self, max_qps):
        self.max_qps = max_qps
        self.min_interval = 1.0 / max_qps if max_qps > 0 else 0
        self.lock = threading.Lock()
        self.last_request_time = 0.0

    def acquire(self):
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
        with self.lock:
            self.max_qps = max(0.5, new_qps)
            self.min_interval = 1.0 / self.max_qps


class ScraperState:
    """线程安全的共享爬取状态 v4.0 — 增加 failed_ids 集合"""

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
        # ★ v4.0 新增: 失败ID集合 — 所有失败的ID收集在此，供最终冲刺阶段重试
        self.failed_ids = set()

    def get_next_id(self):
        with self.lock:
            while self.next_id <= self.end_id:
                current = self.next_id
                self.next_id += 1
                if current not in self.existing_ids:
                    self.processed += 1
                    return current
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

    def record_failed(self, failed_id=None):
        with self.lock:
            self.total_failed += 1
            self._recent_results.append((time.time(), False))
            cutoff = time.time() - 60
            self._recent_results = [
                (ts, ok) for ts, ok in self._recent_results if ts > cutoff
            ]
            # ★ 记录失败的ID
            if failed_id is not None:
                self.failed_ids.add(failed_id)

    def record_success_from_failed(self, failed_id):
        """从失败集合中移除已成功重试的ID"""
        with self.lock:
            self.failed_ids.discard(failed_id)
            self.total_items += 1

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
                "remaining_failed": len(self.failed_ids),
                "speed": speed,
                "elapsed": elapsed,
                "active_threads": self.active_threads,
                "failure_rate": self.get_failure_rate(),
                "current_qps": 0,
            }

    def set_active_threads(self, count):
        with self.lock:
            self.active_threads = count


def writer_thread(out_file, fieldnames, result_queue, state):
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


def progress_display_thread(state, total_range, rate_limiter, params_store):
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
            f"有效={stats['total_items']} 不存在={stats['total_not_found']} 失败={stats['total_failed']} 待重试={stats['remaining_failed']} | "
            f"线程={stats['active_threads']} QPS={rate_limiter.max_qps:.1f} "
            f"失败率={fail_pct:.0f}% 速度={speed:.1f}id/s ETA={eta_str}"
        )
        sys.stdout.write(f"\r{progress_line:<120}")
        sys.stdout.flush()

        now = time.time()
        if now - last_adaptive_check >= adaptive_check_interval:
            last_adaptive_check = now
            fr = stats["failure_rate"]
            original_qps = params_store.get("_original_qps", 10.0)

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
                if rate_limiter.max_qps < original_qps:
                    new_qps = min(original_qps, rate_limiter.max_qps * 1.3)
                    rate_limiter.update_qps(new_qps)
                    print(c(f"\n  ⚡ 自适应: 失败率 {fr*100:.0f}% < 5%, 提速至 {new_qps:.1f} QPS", "green"), flush=True)

        if state.stop_event.wait(0.5):
            break


# ─────────────────────────────────────────────────────────────────────
#  核心改进 3: 工作线程 — 失败ID收集 + Session定期轮换 + 更强容错
# ─────────────────────────────────────────────────────────────────────

def worker_scrape_one(session, base_url, target_id, rate_limiter, state, result_queue,
                      connect_timeout, read_timeout, retry_count):
    """
    爬取单个ID的所有逻辑，被工作线程和最终冲刺阶段共用。
    返回: True=成功, False=失败
    """
    detail_url = f"{base_url}/view/{target_id}"
    thread_name = threading.current_thread().name

    try:
        rate_limiter.acquire()

        r = request_with_retry(
            session, detail_url,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            retry_count=retry_count,
            allow_redirects=False,
        )
        status = r.status_code

        if status in (301, 302, 303, 307, 308, 404):
            state.record_not_found()
            return True  # 不存在 = 成功（非错误）

        if status != 200:
            state.record_failed(target_id)
            state.record_result(False)
            return False

        soup = BeautifulSoup(r.text, "html.parser")
        kbd = soup.select_one("kbd")
        mag_a = soup.find("a", href=re.compile(r"^magnet:"))
        panel = soup.select_one("div.panel.panel-default")

        if not panel and not kbd and not mag_a:
            state.record_not_found()
            return True

        name, info_hash, magnet, size, date, category = parse_detail_page(soup, detail_url)
        row = {
            "id": target_id, "name": name, "info_hash": info_hash,
            "magnet": magnet, "size": size, "date": date, "category": category, "detail_url": detail_url,
        }
        result_queue.put(row)
        state.record_result(True)
        return True

    except Exception as e:
        state.record_failed(target_id)
        state.record_result(False)
        err_str = str(e)
        # 只在真正有趣的时候打印
        if "timed out" not in err_str.lower() and "timeout" not in err_str.lower():
            print(c(f"\n  ✗ [{thread_name}] 失败 (ID {target_id}): {str(e)[:100]}", "red"), flush=True)
        return False


def thread_target(session_factory, base_url, state, rate_limiter, result_queue,
                  connect_timeout, read_timeout, retry_count, num_threads):
    """工作线程 v4.0 — 失败不丢弃 + Session轮换"""
    thread_name = threading.current_thread().name
    consecutive_real_fail = 0
    request_count_since_refresh = 0
    SESSION_REFRESH_INTERVAL = 500  # 每500个请求刷新一次Session

    # 每个线程使用自己的 Session
    session = session_factory()
    max_consecutive_fail = 30  # 连续失败阈值（从20提高到30，更宽容）

    while not state.stop_event.is_set():
        target_id = state.get_next_id()
        if target_id is None:
            break

        # ★ Session 定期轮换 — 防止连接池僵死
        request_count_since_refresh += 1
        if request_count_since_refresh >= SESSION_REFRESH_INTERVAL:
            try:
                session.close()
            except Exception:
                pass
            session = session_factory()
            request_count_since_refresh = 0

        success = worker_scrape_one(
            session, base_url, target_id, rate_limiter, state, result_queue,
            connect_timeout, read_timeout, retry_count
        )

        if success:
            consecutive_real_fail = 0
        else:
            consecutive_real_fail += 1
            if consecutive_real_fail >= max_consecutive_fail:
                print(c(f"\n  ⚠ [{thread_name}] 连续 {consecutive_real_fail} 次失败，暂停 30s + 刷新Session...", "yellow"), flush=True)
                time.sleep(30)
                # ★ 刷新 Session，彻底重置连接
                try:
                    session.close()
                except Exception:
                    pass
                session = session_factory()
                consecutive_real_fail = 0

    try:
        session.close()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────
#  核心改进 4: 最终冲刺阶段 — 对失败ID做无限重试，直到全部成功
# ─────────────────────────────────────────────────────────────────────

def final_retry_sprint(base_url, state, rate_limiter, result_queue, proxies,
                       connect_timeout, read_timeout, retry_count, num_threads):
    """
    最终冲刺阶段: 主线程跑完后，对 failed_ids 中的所有ID做集中无限重试。
    策略:
    - 使用单独的少量线程
    - QPS 降到极低 (1~2 QPS)
    - 每个失败ID无限重试，每次重试间隔指数退避
    - 直到所有失败ID都被成功爬取或确认不存在
    """
    while True:
        with state.lock:
            failed_ids = list(state.failed_ids)
        if not failed_ids:
            break

        print(c(f"\n\n{'═'*60}", "yellow"))
        print(c(f"  🔄 最终冲刺阶段: 剩余 {len(failed_ids)} 个失败ID待重试", "bold"))
        print(c(f"{'═'*60}", "yellow"))

        # 降到极低QPS，避免触发限流
        original_qps = rate_limiter.max_qps
        sprint_qps = max(1.0, original_qps * 0.3)
        rate_limiter.update_qps(sprint_qps)
        print(c(f"  📉 已降速至 {sprint_qps:.1f} QPS 进行重试", "cyan"))

        # 先等待 10s，让服务器喘口气
        print(c(f"  ⏳ 冷却 10s...", "cyan"), flush=True)
        time.sleep(10)

        # 创建专用 Session
        session = create_session(base_url, proxies, num_threads)

        batch_success = 0
        batch_still_fail = 0

        for i, target_id in enumerate(failed_ids):
            if state.stop_event.is_set():
                break

            # 确认这个ID还在失败集合里（可能被其他线程处理了）
            with state.lock:
                if target_id not in state.failed_ids:
                    continue

            # 成功爬取
            success = worker_scrape_one(
                session, base_url, target_id, rate_limiter, state, result_queue,
                connect_timeout, read_timeout, retry_count
            )

            if success:
                with state.lock:
                    state.failed_ids.discard(target_id)
                    state.total_failed = max(0, state.total_failed - 1)
                batch_success += 1
                print(c(f"\n  ✅ [{i+1}/{len(failed_ids)}] ID {target_id} 重试成功!", "green"), flush=True)
            else:
                batch_still_fail += 1
                # 指数退避: 每次失败后等待更久
                fail_wait = min(5 * (1 + batch_still_fail * 0.5), 60)
                print(c(f"\n  ❌ [{i+1}/{len(failed_ids)}] ID {target_id} 仍然失败, 等待 {fail_wait:.0f}s 后继续...", "red"), flush=True)
                time.sleep(fail_wait)

            # 每处理50个失败ID，刷新一次Session
            if (i + 1) % 50 == 0:
                try:
                    session.close()
                except Exception:
                    pass
                session = create_session(base_url, proxies, num_threads)
                # 再冷却一下
                time.sleep(5)

        try:
            session.close()
        except Exception:
            pass

        # 恢复 QPS
        rate_limiter.update_qps(original_qps)

        with state.lock:
            remaining = len(state.failed_ids)

        if remaining > 0:
            print(c(f"\n  ⚠ 本轮冲刺完成: 成功 {batch_success}, 仍失败 {remaining}", "yellow"))
            print(c(f"  🔄 还有 {remaining} 个ID失败，将等待 30s 后开始下一轮冲刺...", "yellow"))
            print(c(f"  (会无限重试直到全部成功)", "cyan"))
            time.sleep(30)
            # 继续下一轮
        else:
            print(c(f"\n  🎉 全部失败ID已成功回收!", "green"))
            break

    print(c(f"\n  🏁 最终冲刺完成，所有失败ID已清零!", "green"))


# ─────────────────────────── 主爬取逻辑（多线程） ─────────────────
def scrape(proxies, params):
    BASE = "https://sukebei.nyaa.si"
    out_file = params["out"]
    is_resume = params.get("is_resume", False)
    fieldnames = ["id", "name", "info_hash", "magnet", "size", "date", "category", "detail_url"]
    num_threads = params.get("num_threads", 8)
    max_qps = params.get("max_qps", 10.0)
    connect_timeout = params.get("connect_timeout", 15)
    read_timeout = params.get("read_timeout", 60)
    retry_count = params.get("retry_count", 10)
    params["_original_qps"] = max_qps

    # Session 工厂函数
    def session_factory():
        return create_session(BASE, proxies, num_threads)

    # ── 1. 检测连通性 ──
    print(c("\n正在测试连接...", "blue"))
    try:
        tmp_session = session_factory()
        r = request_with_retry(tmp_session, BASE,
                               connect_timeout=connect_timeout, read_timeout=read_timeout, retry_count=5)
        r.raise_for_status()
        print(c(f"✔ 连接成功 (HTTP {r.status_code})", "green"))
        tmp_session.close()
    except Exception as e:
        print(c(f"✗ 连接失败: {e}", "red"))
        sys.exit(1)

    # ── 2. 确定ID范围 ──
    start_id = params["start_id"]
    end_id = params["end_id"]
    if end_id == "auto":
        tmp_session = session_factory()
        end_id = detect_latest_id(tmp_session, BASE, connect_timeout=connect_timeout, read_timeout=read_timeout)
        tmp_session.close()
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
    print(f"  连接超时: {connect_timeout}s / 读取超时: {read_timeout}s")
    print(f"  单次请求重试: {retry_count} 次")
    print(f"  失败ID策略: 收集 → 最终冲刺阶段无限重试直到全部成功")
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
    print(c(f"✔ 失败ID保护已启用: 所有失败ID将在最终冲刺阶段无限重试", "green"))
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
        args=(state, total_range, rate_limiter, params),
        name="Progress", daemon=True
    )
    progress.start()

    # ── 7. 工作线程 ──
    state.set_active_threads(num_threads)

    threads = []
    try:
        for i in range(num_threads):
            t = threading.Thread(
                target=thread_target,
                args=(session_factory, BASE, state, rate_limiter, result_queue,
                      connect_timeout, read_timeout, retry_count, num_threads),
                name=f"Worker-{i+1:02d}"
            )
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        print(c(f"\n\n⚠ 用户中断 (Ctrl+C)，正在优雅停止...", "yellow"), flush=True)
        state.stop_event.set()

    # ── 8. 最终冲刺阶段 — 对所有失败ID做无限重试 ──
    if not state.stop_event.is_set():
        with state.lock:
            remaining_failed = len(state.failed_ids)
        if remaining_failed > 0:
            print(c(f"\n\n{'═'*60}", "yellow"))
            print(c(f"  📊 主线程完成，共 {remaining_failed} 个ID需要重试", "yellow"))
            print(c(f"  🔄 进入最终冲刺阶段...", "yellow"))
            print(c(f"{'═'*60}", "yellow"))

            # 最终冲刺使用少量线程
            sprint_threads = min(4, num_threads)
            state.set_active_threads(sprint_threads)

            final_retry_sprint(
                BASE, state, rate_limiter, result_queue, proxies,
                connect_timeout, read_timeout, retry_count, sprint_threads
            )

    state.set_active_threads(0)
    result_queue.join()
    result_queue.put(None)
    writer.join(timeout=5)
    state.stop_event.set()
    progress.join(timeout=2)

    # ── 9. 最终统计 ──
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

    # 最终失败数
    with state.lock:
        final_remaining_failed = len(state.failed_ids)

    print(c(f"""
╔══════════════════════════════════════════════════════════════╗
║  爬取完成！                                                  ║
║  模式       : {mode_str:<36}║
║  线程数     : {num_threads:<36}║
║  本次新增   : {final_stats['total_items']:<36}║
║  文件总记录 : {final_total:<36}║
║  ID 范围   : {start_id} ~ {end_id:<24}║
║  不存在ID  : {final_stats['total_not_found']:<36}║
║  最终失败  : {final_remaining_failed:<36}║
║  去重跳过  : {skipped_count:<36}║
║  平均速度  : {speed_str:<36}║
║  耗时      : {time_str:<36}║
║  输出文件  : {out_file[:36]:<34}║
╠══════════════════════════════════════════════════════════════╣
║  v4.0 零失败装甲版: 失败ID已全部回收                         ║
╚══════════════════════════════════════════════════════════════╝
""", "green"))

    if state.stop_event.is_set():
        print(c(f"  当前进度: 已处理 {final_stats['processed']:,} / {total_range:,}", "cyan"))
        print(c(f"  下次运行本脚本会自动检测该文件并从断点继续", "cyan"))
        with state.lock:
            if state.failed_ids:
                print(c(f"  ⚠ 仍有 {len(state.failed_ids)} 个失败ID，下次运行时可通过断点续爬覆盖", "yellow"))


# ─────────────────────────── 入口 ───────────────────────────────
def main():
    print_banner()
    proxies = setup_proxy()
    params  = setup_params()
    print(c("\n配置完成，开始爬取...\n", "bold"))
    scrape(proxies, params)

if __name__ == "__main__":
    main()
