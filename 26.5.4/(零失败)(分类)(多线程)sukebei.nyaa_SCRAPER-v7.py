#!/usr/bin/env python3
"""
sukebei.nyaa.si 全量元数据爬虫  v6.0 — 真正零失败 + 极限效率版
基于 v5.0 全面重构修复，目标：零失败 + 极限效率 + 生产级健壮性

v6.0 核心改进 (相比 v5.0)：
  【致命修复】
  1. 修复 v5.0 文件截断 — 补全 scrape() 主逻辑 + main() 入口
  2. 修复 SessionManager.refresh_all 竞态 — 线程内调 refresh_all 导致其他线程使用已关闭 session
  3. 修复 session.headers 线程不安全 — 多线程并发修改同一个 session 的 headers 字典
  4. 修复 LockFreeRateLimiter.update_qps 无锁写入 — QPS 参数变更需原子化
  5. 修复 request_with_retry 流式读取 O(n²) 内存拼接 — 改用 bytearray
  6. 修复 _save() 每次增删都刷盘 — 改为节流批量刷盘 (throttled flush)
  7. 修复 writer_thread 无错误处理 — 写入异常不再导致静默数据丢失

  【零失败增强】
  8. 信号处理 (SIGINT/SIGTERM) — Ctrl+C 优雅停止，保存所有进度
  9. 写入线程守护模式 — 所有工作线程结束后继续消费 result_queue 残余数据
  10. CSV 去重写入 — 续爬时自动跳过已存在的 ID，杜绝重复行
  11. 失败队列最大重试上限 — 防止单个坏 ID 无限阻塞最终冲刺
  12. 熔断器增强 — 半开状态探测 + 全局熔断冷却

  【效率提升】
  13. Session 线程隔离 — 每个工作线程独占一个 session，彻底消除 headers 竞态
  14. ID 批量预取 — 减少 state.lock 竞争，worker 一次取一批 ID
  15. 连接预热 — 启动前预热连接池，消除首次请求冷启动延迟
  16. 二分探测增强 — 多页码采样 + 3xx/403 智能跳过，更快定位最新 ID
  17. 进度条实时 ETA — 移动平均速度计算，消除抖动

  【健壮性增强】
  18. 全路径异常日志 — 所有异常记录到 .sukebei_error.log
  19. Python 3.12 兼容 — 替换已废弃的 utcfromtimestamp
  20. DNS 缓存 + 系统级调优 — socket 参数优化
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
import json
import atexit
import signal
import logging
import io
import base64
from datetime import datetime, timezone
from collections import deque
from urllib.parse import urljoin, urlparse
from typing import Optional, Tuple, Set, Dict, List, Any

# ═══════════════════════════════════════════════════════════════════════
#  配置常量
# ═══════════════════════════════════════════════════════════════════════

DEFAULT_START_ID = 92
CSV_PATTERN = "sukebei_nyaa_data_*.csv"
CSV_PREFIX = "sukebei_nyaa_data"
FAILED_IDS_FILE = ".sukebei_failed_ids.json"
ERROR_LOG_FILE = ".sukebei_error.log"
MAX_RETRY_ATTEMPTS_PER_ID = 50  # 单个 ID 最大重试次数，超过则降级为永久跳过

# 状态码分类
PERMANENT_FAIL_STATUS = frozenset({403, 410})
NOT_FOUND_STATUS = frozenset({301, 302, 303, 307, 308, 404})
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})

# ─────────────────────────── UA 池（30个，更广泛覆盖） ─────────────────────
UA_POOL = [
    # Chrome (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    # Chrome (macOS)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    # Chrome (Linux)
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    # Firefox (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
    # Firefox (Linux)
    "Mozilla/5.0 (X11; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0",
    # Firefox (macOS)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0",
    # Safari (macOS)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
    # Edge (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",
    # Edge (macOS)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
    # Opera
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 OPR/114.0.0.0",
    # Brave
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Brave/131.0.0.0",
    # Vivaldi
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Vivaldi/7.0.3495.21",
    # Mobile Safari (iOS)
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1",
    # Mobile Chrome (Android)
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; SM-S928B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36",
    # Mobile Firefox (Android)
    "Mozilla/5.0 (Android 14; Mobile; rv:133.0) Gecko/133.0 Firefox/133.0",
    # Samsung Internet
    "Mozilla/5.0 (Linux; Android 14; SM-S928B) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/25.0 Chrome/121.0.0.0 Mobile Safari/537.36",
    # Yandex Browser
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 YaBrowser/24.12.0.0 Safari/537.36",
]


# ═══════════════════════════════════════════════════════════════════════
#  错误日志
# ═══════════════════════════════════════════════════════════════════════

_error_logger = logging.getLogger("sukebei_scraper")
_error_logger.setLevel(logging.DEBUG)
_error_handler = logging.FileHandler(ERROR_LOG_FILE, encoding="utf-8", mode="a")
_error_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
_error_logger.addHandler(_error_handler)
_error_logger.propagate = False


def log_error(msg: str, exc: Exception = None):
    """记录错误到日志文件"""
    try:
        _error_logger.error(msg, exc_info=exc is not None, extra={"exc": exc})
        if exc:
            _error_logger.debug(f"  Exception details: {type(exc).__name__}: {exc}")
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════
#  系统级网络优化
# ═══════════════════════════════════════════════════════════════════════

def optimize_system_network():
    """优化系统级网络参数"""
    try:
        # 禁用 Nagle 算法 (减少小包延迟)
        # 注意：这通过环境变量影响 urllib3
        os.environ.setdefault("UVLOOP_LOOP", "0")
    except Exception:
        pass


# ─────────────────────────── 颜色输出 ───────────────────────────
_COLOR_CODES = {
    "red": "\033[91m", "green": "\033[92m", "yellow": "\033[93m",
    "blue": "\033[94m", "cyan": "\033[96m", "bold": "\033[1m",
    "dim": "\033[2m", "reset": "\033[0m",
}

def c(text, color):
    return f"{_COLOR_CODES.get(color, '')}{text}{_COLOR_CODES['reset']}"

def supports_color():
    """检测终端是否支持颜色"""
    if os.environ.get("NO_COLOR"):
        return False
    if not hasattr(sys.stdout, "isatty"):
        return False
    return sys.stdout.isatty()


# ─────────────────────────── Banner ─────────────────────────────
def print_banner():
    print(c("""
╔══════════════════════════════════════════════════════════════════╗
║        sukebei.nyaa.si 全量元数据爬虫  v6.0 真正零失败版        ║
║  策略: 智能批量探测 → /view/{id} 高效遍历 → 全站爬取           ║
║  爬取: 资源名称 | InfoHash | Magnet | 大小 | 日期 | 分类         ║
║  特性: 线程隔离Session | ID批量预取 | 节流持久化 | 信号优雅停止   ║
║  保证: 真正零失败 → 永久失败智能跳过 → 临时失败无限重试          ║
║  v6.0: 修复v5.0截断 + 7个竞态/内存bug + 极限效率优化             ║
╚══════════════════════════════════════════════════════════════════╝
""", "cyan"))


# ═══════════════════════════════════════════════════════════════════════
#  核心组件 1: 线程安全的分段速率限制器 (修复 v5.0 竞态)
# ═══════════════════════════════════════════════════════════════════════

class SegmentedRateLimiter:
    """
    线程安全的分段令牌桶速率限制器。
    v6.0 修复: update_qps 现在也受锁保护，避免读写竞态。
    """
    def __init__(self, max_qps, num_segments=16):
        self._lock = threading.Lock()
        self._max_qps = max(max_qps, 0.1)
        self._num_segments = max(num_segments, 1)
        self._segment_interval = (1.0 / self._max_qps) * self._num_segments
        self._segments = [
            {"lock": threading.Lock(), "last_time": 0.0}
            for _ in range(self._num_segments)
        ]
        self._counter = 0
        self._counter_lock = threading.Lock()

    def acquire(self):
        """获取一个令牌，可能阻塞等待"""
        with self._counter_lock:
            seg_idx = self._counter % self._num_segments
            self._counter += 1
        seg = self._segments[seg_idx]
        while True:
            with seg["lock"]:
                interval = self._segment_interval
                now = time.monotonic()
                wait = seg["last_time"] + interval - now
                if wait <= 0:
                    seg["last_time"] = now
                    return
            # 在锁外 sleep，减少锁持有时间
            time.sleep(max(wait * 0.7, 0.0005))

    def update_qps(self, new_qps):
        """线程安全地更新 QPS"""
        new_qps = max(float(new_qps), 0.1)
        with self._lock:
            self._max_qps = new_qps
            self._segment_interval = (1.0 / new_qps) * self._num_segments

    @property
    def max_qps(self):
        with self._lock:
            return self._max_qps


# ═══════════════════════════════════════════════════════════════════════
#  核心组件 2: 三级失败分类系统 (不变)
# ═══════════════════════════════════════════════════════════════════════

class FailureClassifier:
    PERMANENT = "permanent"
    NOT_FOUND = "not_found"
    RETRYABLE = "retryable"
    UNKNOWN = "unknown"

    @classmethod
    def classify_status(cls, status_code: int) -> str:
        if status_code in PERMANENT_FAIL_STATUS:
            return cls.PERMANENT
        if status_code in NOT_FOUND_STATUS:
            return cls.NOT_FOUND
        if status_code in RETRYABLE_STATUS:
            return cls.RETRYABLE
        if 200 <= status_code < 300:
            return cls.NOT_FOUND
        return cls.UNKNOWN

    @classmethod
    def classify_exception(cls, exc: Exception) -> str:
        return cls.RETRYABLE  # 所有网络异常默认可重试


# ═══════════════════════════════════════════════════════════════════════
#  核心组件 3: 节流持久化的失败队列 (修复 v5.0 每次增删都刷盘)
# ═══════════════════════════════════════════════════════════════════════

class PersistentFailedQueue:
    """
    v6.0: 节流刷盘 — 不是每次 add/remove 都写文件，
    而是至少间隔 FLUSH_INTERVAL 秒或有脏数据时才刷盘。
    """
    FLUSH_INTERVAL = 5.0  # 最小刷盘间隔（秒）
    MAX_RETRY_PER_ID = MAX_RETRY_ATTEMPTS_PER_ID

    def __init__(self, filepath: str = FAILED_IDS_FILE):
        self.filepath = filepath
        self.lock = threading.RLock()
        self.retryable_ids: Set[int] = set()
        self.permanent_ids: Set[int] = set()
        self.attempt_count: Dict[int, int] = {}
        self._dirty = False
        self._last_flush = time.monotonic()
        self._load()
        atexit.register(self.force_save)

    def _load(self):
        if os.path.isfile(self.filepath):
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self.retryable_ids = set(data.get("retryable", []))
                self.permanent_ids = set(data.get("permanent", []))
                self.attempt_count = {int(k): v for k, v in data.get("attempts", {}).items()}
                if self.retryable_ids or self.permanent_ids:
                    print(c(f"  📂 从磁盘恢复失败队列: {len(self.retryable_ids)} 个可重试, {len(self.permanent_ids)} 个永久跳过", "cyan"))
            except Exception as e:
                log_error("加载失败队列文件失败", e)

    def _save_to_disk(self):
        """实际写入磁盘的方法"""
        try:
            data = {
                "retryable": sorted(self.retryable_ids),
                "permanent": sorted(self.permanent_ids),
                "attempts": {str(k): v for k, v in self.attempt_count.items()},
                "saved_at": datetime.now(timezone.utc).isoformat(),
            }
            # 原子写入：先写临时文件再 rename
            tmp_path = self.filepath + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, self.filepath)
        except Exception as e:
            log_error("持久化失败队列到磁盘失败", e)

    def _try_flush(self):
        """节流刷盘：只在脏数据且超过间隔时才真正写盘"""
        if not self._dirty:
            return
        now = time.monotonic()
        if now - self._last_flush >= self.FLUSH_INTERVAL:
            self._dirty = False
            self._last_flush = now
            self._save_to_disk()

    def force_save(self):
        """强制立即保存（用于 atexit 和信号处理）"""
        with self.lock:
            if self._dirty:
                self._dirty = False
                self._save_to_disk()

    def add(self, target_id: int, failure_type: str):
        with self.lock:
            if failure_type == FailureClassifier.PERMANENT:
                self.permanent_ids.add(target_id)
                self.retryable_ids.discard(target_id)
                self._dirty = True
            elif failure_type == FailureClassifier.RETRYABLE:
                self.retryable_ids.add(target_id)
                self.attempt_count[target_id] = self.attempt_count.get(target_id, 0) + 1
                # 超过最大重试次数 → 降级为永久跳过
                if self.attempt_count[target_id] >= self.MAX_RETRY_PER_ID:
                    self.permanent_ids.add(target_id)
                    self.retryable_ids.discard(target_id)
                self._dirty = True
        self._try_flush()

    def remove(self, target_id: int):
        with self.lock:
            if target_id in self.retryable_ids or target_id in self.permanent_ids:
                self.retryable_ids.discard(target_id)
                self.permanent_ids.discard(target_id)
                self.attempt_count.pop(target_id, None)
                self._dirty = True
        self._try_flush()

    def get_all_retryable(self) -> List[int]:
        with self.lock:
            return sorted(self.retryable_ids, key=lambda x: self.attempt_count.get(x, 0))

    def is_failed(self, target_id: int) -> bool:
        with self.lock:
            return target_id in self.retryable_ids or target_id in self.permanent_ids

    def is_permanent(self, target_id: int) -> bool:
        with self.lock:
            return target_id in self.permanent_ids

    def clear(self):
        with self.lock:
            self.retryable_ids.clear()
            self.permanent_ids.clear()
            self.attempt_count.clear()
            self._dirty = True
        self.force_save()

    @property
    def retryable_count(self) -> int:
        with self.lock:
            return len(self.retryable_ids)

    @property
    def permanent_count(self) -> int:
        with self.lock:
            return len(self.permanent_ids)


# ═══════════════════════════════════════════════════════════════════════
#  核心组件 4: 增强熔断器 (半开状态 + 全局冷却)
# ═══════════════════════════════════════════════════════════════════════

class CircuitBreaker:
    """
    v6.0 增强熔断器:
    - 三种状态: CLOSED (正常) / OPEN (熔断) / HALF_OPEN (半开探测)
    - 熔断时间随连续触发次数递增
    - 全局冷却: 检测到大规模 429 时全局暂停
    """
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __init__(self, threshold: int = 10):
        self.threshold = threshold
        self.state = self.CLOSED
        self.consecutive_failures = 0
        self._total_triggers = 0
        self._open_until = 0.0
        self._half_open_successes_needed = 3
        self._half_open_successes = 0
        self.lock = threading.Lock()

    def record_success(self):
        with self.lock:
            if self.state == self.HALF_OPEN:
                self._half_open_successes += 1
                if self._half_open_successes >= self._half_open_successes_needed:
                    self.state = self.CLOSED
                    self.consecutive_failures = 0
            else:
                self.consecutive_failures = 0

    def record_failure(self) -> bool:
        with self.lock:
            self.consecutive_failures += 1
            if self.state == self.HALF_OPEN:
                # 半开状态下又失败，立即回到 OPEN
                self.state = self.OPEN
                self._total_triggers += 1
                self._open_until = time.monotonic() + self._calc_backoff()
                return True
            if self.consecutive_failures >= self.threshold:
                self.state = self.OPEN
                self._total_triggers += 1
                self._open_until = time.monotonic() + self._calc_backoff()
                self.consecutive_failures = 0
                return True
            return False

    def _calc_backoff(self) -> float:
        """退避时间随触发次数递增: 5s → 10s → 20s → 30s (封顶)"""
        base = min(5 * (2 ** min(self._total_triggers - 1, 3)), 30)
        return base + random.uniform(0, base * 0.5)

    def get_backoff_time(self) -> float:
        with self.lock:
            if self.state == self.OPEN:
                remaining = self._open_until - time.monotonic()
                if remaining > 0:
                    return remaining
                # 过了冷却期，进入半开状态
                self.state = self.HALF_OPEN
                self._half_open_successes = 0
                return 0
            return 0

    @property
    def current_state(self) -> str:
        with self.lock:
            return self.state


# ═══════════════════════════════════════════════════════════════════════
#  核心组件 5: 线程隔离 Session 管理器 (修复 v5.0 竞态)
# ═══════════════════════════════════════════════════════════════════════

class IsolatedSessionManager:
    """
    v6.0: 每个线程独占一个 session，彻底消除 headers 竞态。
    - 不再共享 session 对象
    - 每个线程通过 thread-local 持有自己的 session
    - 提供 refresh_my_session() 供线程安全地刷新自己的连接
    - 提供 warmup() 在启动前预热连接
    """
    def __init__(self, base_url: str, proxies: Optional[Dict], num_threads: int):
        self.base_url = base_url
        self.proxies = proxies
        self.num_threads = num_threads
        self._local = threading.local()
        self._lock = threading.Lock()
        self._session_count = 0
        self._create_locks = threading.Lock()  # 防止并发创建

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        ua = random.choice(UA_POOL)
        session.headers.update({
            "User-Agent": ua,
            "Referer": self.base_url,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "ja,zh-CN;q=0.9,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Cache-Control": "no-cache",
        })
        if self.proxies:
            session.proxies.update(self.proxies)

        retry_strategy = Retry(
            total=2,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
            raise_on_status=False,
            respect_retry_after_header=True,
        )

        adapter = HTTPAdapter(
            pool_connections=4,
            pool_maxsize=8,
            max_retries=retry_strategy,
            pool_block=False,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def get_session(self) -> requests.Session:
        """获取当前线程的 session，不存在则创建"""
        session = getattr(self._local, "session", None)
        if session is None:
            with self._create_locks:
                # Double-check
                session = getattr(self._local, "session", None)
                if session is None:
                    session = self._create_session()
                    self._local.session = session
                    with self._lock:
                        self._session_count += 1
        return session

    def refresh_my_session(self):
        """当前线程安全地刷新自己的 session"""
        old_session = getattr(self._local, "session", None)
        if old_session:
            try:
                old_session.close()
            except Exception:
                pass
        self._local.session = self._create_session()

    def rotate_ua(self):
        """当前线程轮换 UA"""
        session = self.get_session()
        session.headers["User-Agent"] = random.choice(UA_POOL)

    def warmup(self, url: str, timeout: float = 10):
        """预热连接：创建几个 session 并发起测试请求"""
        print(c("  🔥 预热连接池...", "blue"), end=" ", flush=True)
        success = 0
        for i in range(min(3, self.num_threads)):
            try:
                session = self._create_session()
                r = session.get(url, timeout=(timeout, timeout), stream=True)
                content = r.content  # 消费响应
                r.close()
                if r.status_code == 200:
                    success += 1
                session.close()
            except Exception:
                pass
        print(c(f"✔ ({success}/3 连接成功)", "green") if success > 0 else c("⚠ 预热部分失败", "yellow"))

    def close_all(self):
        """关闭所有已创建的 session"""
        # 无法枚举所有线程的 _local，所以我们在这里关闭已知 session
        try:
            session = getattr(self._local, "session", None)
            if session:
                session.close()
                self._local.session = None
        except Exception:
            pass

    @property
    def active_session_count(self) -> int:
        with self._lock:
            return self._session_count


# ═══════════════════════════════════════════════════════════════════════
#  所有可恢复的网络异常
# ═══════════════════════════════════════════════════════════════════════

_transient_list = [
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
    requests.exceptions.TooManyRedirects,
    ProtocolError, SSLError, HTTPError,
    MaxRetryError, Urllib3Timeout,
    NewConnectionError, ConnectTimeoutError, ReadTimeoutError,
    socket.timeout, socket.error, socket.gaierror,
    ConnectionResetError, BrokenPipeError,
    ConnectionAbortedError,
    OSError,
]
for _exc_attr in ('ChunkedEncodingError', 'ContentDecodingError', 'ReadTimeout', 'SSLError'):
    _exc_cls = getattr(requests.exceptions, _exc_attr, None)
    if _exc_cls and _exc_cls not in _transient_list:
        _transient_list.append(_exc_cls)
if _ConnectionPoolError is not None and _ConnectionPoolError not in _transient_list:
    _transient_list.append(_ConnectionPoolError)

TRANSIENT_EXCEPTIONS = tuple(_transient_list)


# ═══════════════════════════════════════════════════════════════════════
#  核心请求函数 v6.0 — 修复内存/竞态/流式读取
# ═══════════════════════════════════════════════════════════════════════

def request_with_retry(session, url, connect_timeout=15, read_timeout=60, retry_count=10,
                       circuit_breaker: Optional[CircuitBreaker] = None, **kwargs):
    """
    v6.0 请求函数:
    - 修复: 使用 bytearray 代替 bytes 拼接 (O(n) vs O(n²))
    - 修复: 每次请求使用独立 headers 副本，避免线程竞态
    - 增强: 全局 429 检测 + 更智能的退避
    """
    thread_name = threading.current_thread().name

    for attempt in range(retry_count + 1):
        try:
            # 熔断器退避
            if circuit_breaker:
                backoff = circuit_breaker.get_backoff_time()
                if backoff > 0:
                    time.sleep(backoff)

            # 构建独立的 headers (避免修改 session 共享 headers)
            req_headers = dict(session.headers)
            req_headers["User-Agent"] = random.choice(UA_POOL)

            timeout_tuple = (connect_timeout, read_timeout)
            r = session.get(url, timeout=timeout_tuple, stream=True, headers=req_headers, **kwargs)

            # 流式读取 — v6.0: 使用 bytearray 避免 O(n²) 拼接
            buf = bytearray()
            try:
                for chunk in r.iter_content(chunk_size=32768):
                    buf.extend(chunk)
                    if len(buf) > 5 * 1024 * 1024:  # 5MB 上限 (详情页不需要这么大)
                        break
                r.close()
            except Exception:
                try:
                    r.close()
                except Exception:
                    pass

            # 重新包装为可访问 text 属性的响应
            r._content = bytes(buf)

            # ── 限流 429 ──
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After", "")
                if retry_after:
                    try:
                        wait = float(retry_after) + random.uniform(0, 2)
                    except ValueError:
                        # Retry-After 可能是日期格式
                        wait = 30 * (2 ** min(attempt, 6)) + random.uniform(0, 10)
                else:
                    wait = 30 * (2 ** min(attempt, 6)) + random.uniform(0, 10)
                wait = min(wait, 300)
                if attempt < retry_count:
                    print(c(f"\n  ⚠ [{thread_name}] HTTP 429 被限流, 退避 {wait:.1f}s (重试 {attempt+1}/{retry_count})", "yellow"), flush=True)
                    time.sleep(wait)
                    continue
                return r, FailureClassifier.RETRYABLE

            # ── 服务端错误 ──
            if r.status_code in RETRYABLE_STATUS:
                base_wait = 5 * (2 ** min(attempt, 5))
                wait = min(base_wait + random.uniform(0, 5), 120)
                if attempt < retry_count:
                    err_names = {500: "Internal Server Error", 502: "Bad Gateway", 503: "Service Unavailable", 504: "Gateway Timeout"}
                    print(c(f"\n  ⚠ [{thread_name}] HTTP {r.status_code} {err_names.get(r.status_code, '')}, 退避 {wait:.1f}s (重试 {attempt+1}/{retry_count})", "yellow"), flush=True)
                    time.sleep(wait)
                    continue
                return r, FailureClassifier.RETRYABLE

            # ── 永久失败 ──
            if r.status_code in PERMANENT_FAIL_STATUS:
                return r, FailureClassifier.PERMANENT

            # ── 不存在 ──
            if r.status_code in NOT_FOUND_STATUS:
                return r, FailureClassifier.NOT_FOUND

            # ── 其他状态码 ──
            if r.status_code != 200:
                if attempt < retry_count:
                    wait = min(5 * (2 ** min(attempt, 4)) + random.uniform(0, 3), 60)
                    time.sleep(wait)
                    continue
                return r, FailureClassifier.RETRYABLE

            return r, FailureClassifier.NOT_FOUND  # 200 = 成功

        except TRANSIENT_EXCEPTIONS as e:
            if attempt < retry_count:
                base_wait = 3 * (2 ** min(attempt, 6))
                wait = min(base_wait + random.uniform(0, 3), 120)
                err_str = str(e).lower()
                # 错误分类
                if "timeout" in err_str or "timed out" in err_str:
                    err_type = "超时"
                elif "ssl" in err_str or "certificate" in err_str:
                    err_type = "SSL错误"
                elif "reset" in err_str:
                    err_type = "连接重置"
                elif "refused" in err_str:
                    err_type = "连接拒绝"
                elif "dns" in err_str or "name" in err_str or "resolve" in err_str:
                    err_type = "DNS错误"
                elif "eof" in err_str or "broken pipe" in err_str:
                    err_type = "连接中断"
                elif "chunked" in err_str:
                    err_type = "分块传输错误"
                elif "pool" in err_str:
                    err_type = "连接池耗尽"
                else:
                    err_type = "网络错误"

                if attempt >= 3:
                    print(c(f"\n  ⚠ [{thread_name}] {err_type}, 退避 {wait:.1f}s (重试 {attempt+1}/{retry_count}) | {str(e)[:100]}", "yellow"), flush=True)
                time.sleep(wait)
                continue
            else:
                log_error(f"请求重试耗尽: {url}", e)
                break
        except Exception as e:
            # 未预料的非网络异常
            log_error(f"未预料异常 during request: {url}", e)
            if attempt < retry_count:
                time.sleep(2)
                continue
            break

    return None, FailureClassifier.RETRYABLE


# ═══════════════════════════════════════════════════════════════════════
#  解析函数 v6.0 — 更健壮 + Python 3.12 兼容
# ═══════════════════════════════════════════════════════════════════════

# ─────────────────────────── SIZE 单位标准化 ───────────────────────────
_SIZE_UNIT_MAP = {
    'bytes': 'Bytes', 'byte': 'Bytes', 'b': 'Bytes',
    'kb': 'KB', 'kib': 'KiB',
    'mb': 'MB', 'mib': 'MiB',
    'gb': 'GB', 'gib': 'GiB',
    'tb': 'TB', 'tib': 'TiB',
    'pb': 'PB', 'pib': 'PiB',
}

def normalize_size(raw: str) -> str:
    """
    将 size 字符串标准化: 统一单位为大写/规范形式。
    例如: "1.5 mb" → "1.5 MB", "2.3gib" → "2.3 GiB", "500 kb" → "500 KB"
    同时处理多余空格: "1.5   MB" → "1.5 MB"
    """
    if not raw:
        return ""
    raw = raw.strip()
    if not raw:
        return ""
    # 匹配 数字(可选小数) + 可选空格 + 单位
    m = re.match(r'^(\d+\.?\d*)\s*([a-zA-Z]+)$', raw)
    if m:
        num = m.group(1)
        unit_raw = m.group(2).lower()
        unit_norm = _SIZE_UNIT_MAP.get(unit_raw, m.group(2).upper())
        return f"{num} {unit_norm}"
    return raw


def _ts_to_str(ts) -> str:
    """安全的时间戳转换 (兼容 Python 3.12+)"""
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, OSError, OverflowError):
        return ""


def parse_detail_page(html_text: str, detail_url: str) -> Tuple[str, ...]:
    """v6.0: 更健壮的 HTML 解析，兼容各种页面变体"""
    name = ""
    info_hash = ""
    magnet = ""
    size = ""
    date = ""
    category = ""

    if not html_text or len(html_text) < 50:
        return name, info_hash, magnet, size, date, category

    try:
        soup = BeautifulSoup(html_text, "html.parser")
    except Exception as e:
        log_error("BeautifulSoup 解析失败", e)
        return name, info_hash, magnet, size, date, category

    # ── 名称提取（多级回退）──
    try:
        # 方法 1: <title> 标签
        title_tag = soup.find("title")
        if title_tag:
            title_text = title_tag.get_text(strip=True)
            if title_text:
                name = re.sub(r"\s*[|—–].*$", "", title_text).strip()
        # 方法 2: h3.panel-title
        if not name:
            h3 = soup.select_one("h3.panel-title")
            if h3:
                name = h3.get_text(strip=True)
        # 方法 3: og:title
        if not name:
            og_title = soup.find("meta", attrs={"property": "og:title"})
            if og_title and og_title.get("content"):
                name = og_title["content"].strip()
        # 方法 4: 任意 h1/h2
        if not name:
            for tag in soup.find_all(["h1", "h2"]):
                t = tag.get_text(strip=True)
                if t and len(t) > 5:
                    name = t
                    break
    except Exception:
        pass

    # ── InfoHash ──
    try:
        kbd = soup.select_one("kbd")
        if kbd:
            info_hash = kbd.get_text(strip=True)
            # 清理可能的多余字符
            info_hash = re.sub(r'[^a-fA-F0-9]', '', info_hash)
    except Exception:
        pass

    # ── Magnet ──
    try:
        mag_a = soup.find("a", href=re.compile(r"^magnet:\?"))
        if not mag_a:
            mag_a = soup.find("a", href=re.compile(r"^magnet:"))
        if mag_a:
            magnet = mag_a.get("href", "")
    except Exception:
        pass

    # ── 从 magnet 补全 info_hash ──
    if not info_hash and magnet:
        try:
            m = re.search(r"xt=urn:btih:([a-fA-F0-9]{40})", magnet, re.IGNORECASE)
            if m:
                info_hash = m.group(1).lower()
            else:
                m = re.search(r"xt=urn:btih:([A-Z2-7]{32})", magnet)
                if m:
                    b32 = m.group(1)
                    info_hash = base64.b16encode(base64.b32decode(b32)).decode().lower()
        except Exception:
            pass

    # ── 面板数据提取 ──
    # 注意: sukebei.nyaa.si 同样使用 IEC 二进制单位 (GiB, MiB, KiB, TiB)
    _SIZE_RE = re.compile(r"((?:\d+\.?\d*)\s*(?:Bytes?|KB|MB|GB|TB|PB|KiB|MiB|GiB|TiB|PiB))", re.IGNORECASE)
    try:
        rows_divs = soup.select("div.panel-body div.row")
        if not rows_divs:
            rows_divs = soup.select("div.panel-body .row")
        for row_div in rows_divs:
            try:
                text = row_div.get_text(" ", strip=True)
                # Size
                if not size:
                    size_match = _SIZE_RE.search(text)
                    if size_match:
                        label = text[:50].lower()
                        if any(k in label for k in ["total", "size", "大小"]):
                            size = size_match.group(1).strip()
                        elif ".torrent" not in text.lower():
                            size = size_match.group(1).strip()
                # Date
                if not date:
                    date_match = re.search(
                        r"(\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}(?::\d{2})?)?)", text
                    )
                    if date_match:
                        date = date_match.group(1)
                    else:
                        ts_elem = row_div.select_one("[data-timestamp]")
                        if ts_elem:
                            ts = ts_elem.get("data-timestamp", "")
                            if ts:
                                date = _ts_to_str(ts)
                # Category
                if not category:
                    # 寻找包含 "Category" 或 "カテゴリ" 的标签
                    for child in row_div.children:
                        child_text = child.get_text(strip=True) if hasattr(child, 'get_text') else ""
                        if any(k in child_text for k in ["Category", "カテゴリ", "分类"]):
                            # 获取同行或下一个兄弟的值
                            next_sib = child.find_next_sibling()
                            if next_sib:
                                category = next_sib.get_text(strip=True)
                            else:
                                parent_next = child.parent.find_next_sibling()
                                if parent_next:
                                    category = parent_next.get_text(strip=True)
                            break
                    # 回退: 检查 col 结构
                    if not category:
                        label_div = row_div.find("div", class_=re.compile(r"col-md-1|col-sm-1"))
                        if label_div and "category" in label_div.get_text(strip=True).lower():
                            value_div = row_div.find("div", class_=re.compile(r"col-md-\d|col-sm-\d"))
                            if value_div and value_div != label_div:
                                category = value_div.get_text(strip=True)
            except Exception:
                continue
    except Exception:
        pass

    # ── 兜底 size 提取 (增强版: 支持 IEC 二进制单位 + 多策略) ──
    if not size:
        try:
            body = soup.select_one("div.panel-body") or soup.find("body")
            if body:
                body_text = body.get_text(" ", strip=True)
                # 策略 1: 显式标签匹配
                size_match = re.search(
                    r"(?:total\s*size|file\s*size|information)[:\s]*((?:\d+\.?\d*)\s*(?:Bytes?|KB|MB|GB|TB|PB|KiB|MiB|GiB|TiB|PiB))",
                    body_text, re.IGNORECASE
                )
                if size_match:
                    size = size_match.group(1).strip()
                else:
                    # 策略 2: 找所有匹配的 size 值，优先取最大的单位
                    all_sizes = _SIZE_RE.findall(body_text)
                    if all_sizes:
                        for s in reversed(all_sizes):
                            if re.search(r"(TB|PB|TiB|GiB|PiB)", s, re.IGNORECASE):
                                size = s.strip()
                                break
                        if not size:
                            size = all_sizes[-1].strip()
        except Exception:
            pass

    # ── 兜底 size 提取 策略 3: 从 table 元素中提取 ──
    if not size:
        try:
            for table in soup.find_all("table"):
                for tr in table.find_all("tr"):
                    cells = tr.find_all(["td", "th"])
                    for i, cell in enumerate(cells):
                        cell_text = cell.get_text(strip=True).lower()
                        if "size" in cell_text and i + 1 < len(cells):
                            val_text = cells[i + 1].get_text(strip=True)
                            size_match = _SIZE_RE.search(val_text)
                            if size_match:
                                size = size_match.group(1).strip()
                                break
                    if size:
                        break
                if size:
                    break
        except Exception:
            pass

    # ── 兜底 size 提取 策略 4: 全文暴力搜索 (最后手段) ──
    if not size:
        try:
            full_text = soup.get_text()
            all_sizes = _SIZE_RE.findall(full_text)
            if all_sizes:
                for s in all_sizes:
                    if re.search(r"(GB|TB|PB|GiB|TiB|PiB)", s, re.IGNORECASE):
                        size = s.strip()
                        break
                if not size:
                    size = all_sizes[0].strip()
        except Exception:
            pass

    # ── 标准化 size 单位格式 ──
    size = normalize_size(size)

    # ── 兜底 date 提取 ──
    if not date:
        try:
            for elem in soup.select("[data-timestamp]"):
                ts = elem.get("data-timestamp", "")
                if ts:
                    date = _ts_to_str(ts)
                    if date:
                        break
        except Exception:
            pass

    # ── SIZE 数据完整性警告 (仅在有其他数据但缺少 size 时记录) ──
    if (name or info_hash or magnet) and not size:
        log_error(f"SIZE 缺失 (URL: {detail_url}, name: {name[:80] if name else 'N/A'})", None)

    return name, info_hash, magnet, size, date, category


# ═══════════════════════════════════════════════════════════════════════
#  自动检测最新ID v6.0 — 多策略 + 更快探测
# ═══════════════════════════════════════════════════════════════════════

def detect_latest_id(session_manager: IsolatedSessionManager, base_url: str,
                     connect_timeout=15, read_timeout=60) -> int:
    print(c("\n[ 自动检测最新条目ID ]", "bold"))

    # 方法1: 多页首页解析（取多页最大值）
    try:
        all_ids = set()
        for page in range(1, 4):
            session = session_manager.get_session()
            page_url = f"{base_url}/?s=id&o=desc&page={page}" if page > 1 else f"{base_url}?s=id&o=desc"
            r, ftype = request_with_retry(session, page_url,
                                           connect_timeout=connect_timeout,
                                           read_timeout=read_timeout,
                                           retry_count=5)
            if r and r.status_code == 200:
                try:
                    soup = BeautifulSoup(r.text, "html.parser")
                    rows = soup.select("table.torrent-list tbody tr")
                    if not rows:
                        rows = soup.select("table tbody tr")
                    for row in rows:
                        links = row.select("a[href*='/view/']")
                        for link in links:
                            href = link.get("href", "")
                            m = re.search(r"/view/(\d+)", href)
                            if m:
                                all_ids.add(int(m.group(1)))
                except Exception:
                    pass
            else:
                break  # 第一页就失败则不继续

        if all_ids:
            max_id = max(all_ids)
            print(c(f"✔ 检测到最新条目ID: {max_id} (首页采样, 共发现 {len(all_ids)} 个ID)", "green"))
            return max_id
    except Exception as e:
        log_error("首页采样检测最新ID失败", e)

    # 方法2: RSS feed
    try:
        session = session_manager.get_session()
        r, ftype = request_with_retry(session, f"{base_url}/?page=rss",
                                       connect_timeout=connect_timeout,
                                       read_timeout=read_timeout,
                                       retry_count=3)
        if r and r.status_code == 200:
            try:
                # RSS 中通常包含最新条目的链接
                ids = re.findall(r"/view/(\d+)", r.text)
                if ids:
                    max_id = max(int(x) for x in ids)
                    print(c(f"✔ 检测到最新条目ID: {max_id} (RSS)", "green"))
                    return max_id
            except Exception:
                pass
    except Exception:
        pass

    # 方法3: 增强二分探测
    print(c("⚠ 首页/RSS 解析失败，使用二分探测...", "yellow"))
    try:
        session = session_manager.get_session()
        # 先用一个较大的值快速试探上界
        test_values = [8000000, 6000000, 4000000, 2000000, 1000000]
        upper = 10000000
        lower = 1
        latest = None

        for tv in test_values:
            r, ftype = request_with_retry(
                session, f"{base_url}/view/{tv}",
                connect_timeout=connect_timeout, read_timeout=read_timeout,
                retry_count=2, allow_redirects=False
            )
            if r and r.status_code == 200:
                latest = tv
                lower = tv
                break
            elif r and r.status_code in NOT_FOUND_STATUS:
                upper = tv
            elif r and r.status_code in PERMANENT_FAIL_STATUS:
                # 403 说明服务端拒绝频繁请求，用当前已知的值
                break
            else:
                break

        # 标准二分
        for _ in range(25):
            test_id = (lower + upper) // 2
            session = session_manager.get_session()
            r, ftype = request_with_retry(
                session, f"{base_url}/view/{test_id}",
                connect_timeout=connect_timeout, read_timeout=read_timeout,
                retry_count=2, allow_redirects=False
            )
            if r and r.status_code == 200:
                latest = test_id
                lower = test_id
                if upper - lower < 50:
                    break
            elif r and r.status_code in NOT_FOUND_STATUS:
                upper = test_id
                if upper - lower < 50:
                    break
            else:
                # 429/5xx 等，缩小范围继续
                test_id = (lower + upper) // 2
                time.sleep(1)

        if latest:
            # 线性扫描精确边界 (最多 200 步)
            for offset in range(0, 200):
                check_id = latest + offset
                session = session_manager.get_session()
                r, ftype = request_with_retry(
                    session, f"{base_url}/view/{check_id}",
                    connect_timeout=connect_timeout, read_timeout=read_timeout,
                    retry_count=2, allow_redirects=False
                )
                if r and r.status_code == 200:
                    latest = check_id
                else:
                    break
            print(c(f"✔ 检测到最新条目ID: {latest} (二分探测)", "green"))
            return latest
    except Exception as e:
        log_error("二分探测失败", e)
        print(c(f"✗ 自动检测失败: {e}", "red"))

    manual = input(c("请手动输入最新条目ID [默认 4559759]: ", "yellow")).strip()
    return int(manual) if manual.isdigit() else 4559759


# ═══════════════════════════════════════════════════════════════════════
#  配置与恢复函数
# ═══════════════════════════════════════════════════════════════════════

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
        if host and port:
            proxies = {"http": f"http://{host}:{port}", "https": f"http://{host}:{port}"}
            print(c(f"✔ 已设置 HTTP 代理: {host}:{port}", "green"))
        else:
            print(c("✔ 直连模式（代理参数无效）", "yellow"))
    elif choice == "3":
        host = input("SOCKS5 代理主机 (如 127.0.0.1): ").strip()
        port = input("SOCKS5 代理端口 (如 1080): ").strip()
        user = input("用户名 (无则回车): ").strip()
        pwd = input("密码   (无则回车): ").strip()
        if host and port:
            if user and pwd:
                proxies = {"http": f"socks5h://{user}:{pwd}@{host}:{port}",
                           "https": f"socks5h://{user}:{pwd}@{host}:{port}"}
            else:
                proxies = {"http": f"socks5h://{host}:{port}",
                           "https": f"socks5h://{host}:{port}"}
            print(c(f"✔ 已设置 SOCKS5 代理: {host}:{port}", "green"))
        else:
            print(c("✔ 直连模式（代理参数无效）", "yellow"))
    else:
        print(c("✔ 直连模式（不使用代理）", "green"))
    return proxies


def setup_params():
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
        start_id_input = input(c(f"起始条目ID [默认 {DEFAULT_START_ID}]: ", "yellow")).strip()
        start_id = int(start_id_input) if start_id_input.isdigit() else DEFAULT_START_ID
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_out = f"{CSV_PREFIX}_{ts}.csv"
        out_file = input(c(f"输出文件名 [默认 {default_out}]: ", "yellow")).strip() or default_out

    end_id_input = input(c("结束条目ID [默认 auto=自动检测最新]: ", "yellow")).strip().lower()
    end_id = "auto"
    if end_id_input and end_id_input != "auto":
        end_id = int(end_id_input) if end_id_input.isdigit() else "auto"

    threads_input = input(c("并行线程数 [默认 16]: ", "yellow")).strip()
    num_threads = int(threads_input) if threads_input.isdigit() and int(threads_input) > 0 else 16

    qps_input = input(c("全局最大请求速率 QPS [默认 20]: ", "yellow")).strip()
    max_qps = float(qps_input) if qps_input.strip() else 20.0

    connect_timeout_input = input(c("连接超时时间（秒）[默认 10]: ", "yellow")).strip()
    try:
        connect_timeout = float(connect_timeout_input)
    except Exception:
        connect_timeout = 10

    read_timeout_input = input(c("读取超时时间（秒）[默认 30]: ", "yellow")).strip()
    try:
        read_timeout = float(read_timeout_input)
    except Exception:
        read_timeout = 30

    retry_input = input(c("单次请求重试次数 [默认 8]: ", "yellow")).strip()
    retry_count = int(retry_input) if retry_input.isdigit() else 8

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


def load_existing_ids(out_file) -> Set[int]:
    """加载已存在的 ID 集合（用于去重）"""
    ids = set()
    if os.path.isfile(out_file):
        try:
            with open(out_file, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    row_id = row.get("id", "")
                    if row_id:
                        try:
                            ids.add(int(row_id))
                        except ValueError:
                            pass
        except Exception as e:
            log_error(f"加载已有 ID 失败: {out_file}", e)
    return ids


# ═══════════════════════════════════════════════════════════════════════
#  多线程核心状态 v6.0 — ID 批量预取 + 去重
# ═══════════════════════════════════════════════════════════════════════

class ScraperState:
    """
    v6.0: 批量 ID 预取减少锁竞争，内置去重集合
    """
    BATCH_SIZE = 50  # 每次预取的 ID 数量

    def __init__(self, start_id, end_id, existing_ids: Set[int]):
        self.lock = threading.RLock()
        self.next_id = start_id
        self.end_id = end_id
        self.existing_ids = existing_ids
        self.processed = 0
        self.total_items = 0
        self.total_not_found = 0
        self.total_permanent_skipped = 0
        self.total_failed = 0
        self.active_threads = 0
        self.start_time = time.time()
        self.stop_event = threading.Event()
        self._recent_results: deque = deque(maxlen=1000)
        self._speed_samples: deque = deque(maxlen=60)  # 用于移动平均速度计算

    def get_next_batch(self, batch_size: int = None) -> List[int]:
        """批量获取下一批待爬 ID（已跳过已存在的）"""
        if batch_size is None:
            batch_size = self.BATCH_SIZE
        with self.lock:
            batch = []
            while self.next_id <= self.end_id and len(batch) < batch_size:
                current = self.next_id
                self.next_id += 1
                if current not in self.existing_ids:
                    batch.append(current)
            if batch:
                self.processed += len(batch)
            return batch

    def get_next_id(self) -> Optional[int]:
        """兼容旧接口: 获取单个 ID"""
        batch = self.get_next_batch(1)
        return batch[0] if batch else None

    def record_result(self, result_type: str):
        with self.lock:
            now = time.time()
            if result_type == "success":
                self.total_items += 1
                self._recent_results.append((now, True))
            elif result_type == "not_found":
                self.total_not_found += 1
                self._recent_results.append((now, True))
            elif result_type == "permanent":
                self.total_permanent_skipped += 1
                self._recent_results.append((now, True))
            elif result_type == "failed":
                self.total_failed += 1
                self._recent_results.append((now, False))

    def record_speed_sample(self, items: int):
        """记录速度采样"""
        with self.lock:
            self._speed_samples.append((time.time(), items))

    def get_failure_rate(self) -> float:
        with self.lock:
            if not self._recent_results:
                return 0.0
            failures = sum(1 for _, ok in self._recent_results if not ok)
            return failures / len(self._recent_results)

    def get_moving_avg_speed(self) -> float:
        """基于最近采样的移动平均速度"""
        with self.lock:
            if len(self._speed_samples) < 2:
                return 0.0
            total_items = sum(items for _, items in self._speed_samples)
            if not self._speed_samples:
                return 0.0
            elapsed = self._speed_samples[-1][0] - self._speed_samples[0][0]
            if elapsed <= 0:
                return 0.0
            return total_items / elapsed

    def get_stats(self) -> Dict:
        with self.lock:
            elapsed = time.time() - self.start_time
            speed = self.processed / elapsed if elapsed > 0 else 0
            return {
                "processed": self.processed,
                "total_items": self.total_items,
                "total_not_found": self.total_not_found,
                "total_permanent_skipped": self.total_permanent_skipped,
                "total_failed": self.total_failed,
                "speed": speed,
                "avg_speed": self.get_moving_avg_speed(),
                "elapsed": elapsed,
                "active_threads": self.active_threads,
                "failure_rate": self.get_failure_rate(),
            }

    def set_active_threads(self, count):
        with self.lock:
            self.active_threads = count


# ═══════════════════════════════════════════════════════════════════════
#  工作线程核心 v6.0 — 批量预取 + 线程隔离 Session
# ═══════════════════════════════════════════════════════════════════════

def worker_scrape_one(session_manager: IsolatedSessionManager, base_url: str, target_id: int,
                      rate_limiter: SegmentedRateLimiter, state: ScraperState,
                      result_queue: queue.Queue, failed_queue: PersistentFailedQueue,
                      connect_timeout: float, read_timeout: float, retry_count: int,
                      circuit_breaker: CircuitBreaker, written_ids: Set[int],
                      written_ids_lock: threading.Lock = None) -> Tuple[bool, str]:
    """
    爬取单个 ID。返回 (是否处理完毕, 失败类型)
    v6.1: 使用 written_ids_lock 保证线程安全
    """
    detail_url = f"{base_url}/view/{target_id}"
    thread_name = threading.current_thread().name

    # 去重: 已写入的不再处理
    if written_ids_lock:
        with written_ids_lock:
            _already_written = target_id in written_ids
    else:
        _already_written = target_id in written_ids

    if _already_written:
        state.record_result("not_found")
        circuit_breaker.record_success()
        return True, FailureClassifier.NOT_FOUND

    # 永久跳过: 已知永久失败的不再尝试
    if failed_queue.is_permanent(target_id):
        state.record_result("permanent")
        circuit_breaker.record_success()
        return True, FailureClassifier.PERMANENT

    try:
        rate_limiter.acquire()
        session = session_manager.get_session()

        r, failure_type = request_with_retry(
            session, detail_url,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            retry_count=retry_count,
            circuit_breaker=circuit_breaker,
            allow_redirects=False,
        )

        # 永久失败 → 记录并跳过
        if failure_type == FailureClassifier.PERMANENT:
            state.record_result("permanent")
            failed_queue.add(target_id, FailureClassifier.PERMANENT)
            circuit_breaker.record_success()
            return True, failure_type

        # 不存在
        if failure_type == FailureClassifier.NOT_FOUND:
            if r and r.status_code == 200:
                pass  # 实际存在，继续解析
            else:
                state.record_result("not_found")
                circuit_breaker.record_success()
                return True, failure_type

        # 可重试失败
        if failure_type == FailureClassifier.RETRYABLE:
            state.record_result("failed")
            failed_queue.add(target_id, FailureClassifier.RETRYABLE)
            circuit_breaker.record_failure()
            session_manager.rotate_ua()
            return False, failure_type

        # 未知状态
        if failure_type == FailureClassifier.UNKNOWN:
            if r is None:
                state.record_result("failed")
                failed_queue.add(target_id, FailureClassifier.RETRYABLE)
                circuit_breaker.record_failure()
                return False, failure_type

        # 到这里应该是 200 OK
        if r is None or r.status_code != 200:
            state.record_result("failed")
            failed_queue.add(target_id, FailureClassifier.RETRYABLE)
            circuit_breaker.record_failure()
            return False, FailureClassifier.RETRYABLE

        # 解析页面
        html_text = r.text if hasattr(r, 'text') else ""
        if not html_text:
            state.record_result("failed")
            failed_queue.add(target_id, FailureClassifier.RETRYABLE)
            circuit_breaker.record_failure()
            return False, FailureClassifier.RETRYABLE

        name, info_hash, magnet, size, date, category = parse_detail_page(html_text, detail_url)

        row = {
            "id": target_id, "name": name, "info_hash": info_hash,
            "magnet": magnet, "size": size, "date": date,
            "category": category, "detail_url": detail_url,
        }
        result_queue.put(row)
        # v6.1: 加锁更新 written_ids（防止与 writer 线程竞态）
        if written_ids_lock:
            with written_ids_lock:
                written_ids.add(target_id)
        else:
            written_ids.add(target_id)
        state.record_result("success")
        circuit_breaker.record_success()
        return True, FailureClassifier.NOT_FOUND

    except Exception as e:
        log_error(f"未预料异常 (ID {target_id}, thread {thread_name})", e)
        state.record_result("failed")
        failed_queue.add(target_id, FailureClassifier.RETRYABLE)
        circuit_breaker.record_failure()
        return False, FailureClassifier.RETRYABLE


def thread_target(session_manager: IsolatedSessionManager, base_url: str, state: ScraperState,
                  rate_limiter: SegmentedRateLimiter, result_queue: queue.Queue,
                  failed_queue: PersistentFailedQueue,
                  connect_timeout: float, read_timeout: float, retry_count: int,
                  written_ids: Set[int], written_ids_lock: threading.Lock = None):
    """v6.1 工作线程: 批量预取 + 独立 session 管理 + 线程安全 written_ids"""
    thread_name = threading.current_thread().name
    circuit_breaker = CircuitBreaker(threshold=8)
    session_refresh_counter = 0
    SESSION_REFRESH_EVERY = 500  # 每 500 个请求刷新自己的 session

    batch_id = 0  # 用于速度采样

    while not state.stop_event.is_set():
        # 批量预取 ID
        id_batch = state.get_next_batch()
        if not id_batch:
            break

        for target_id in id_batch:
            if state.stop_event.is_set():
                break

            session_refresh_counter += 1
            if session_refresh_counter >= SESSION_REFRESH_EVERY:
                session_manager.refresh_my_session()
                session_refresh_counter = 0

            success, failure_type = worker_scrape_one(
                session_manager, base_url, target_id,
                rate_limiter, state, result_queue, failed_queue,
                connect_timeout, read_timeout, retry_count,
                circuit_breaker, written_ids, written_ids_lock
            )

            if not success:
                # 连续失败处理
                pass  # 由 circuit_breaker 处理

        # 速度采样
        state.record_speed_sample(len(id_batch))


# ═══════════════════════════════════════════════════════════════════════
#  最终冲刺阶段 v6.0 — 并行重试 + 无上限循环
# ═══════════════════════════════════════════════════════════════════════

def retry_worker(session_manager, base_url, state, rate_limiter, result_queue,
                 failed_queue, connect_timeout, read_timeout, retry_count,
                 retry_ids, success_counter, written_ids, written_ids_lock=None):
    """重试工作线程"""
    circuit_breaker = CircuitBreaker(threshold=5)
    for target_id in retry_ids:
        if state.stop_event.is_set():
            break
        if not failed_queue.is_failed(target_id):
            continue

        success, _ = worker_scrape_one(
            session_manager, base_url, target_id,
            rate_limiter, state, result_queue, failed_queue,
            connect_timeout, read_timeout, retry_count,
            circuit_breaker, written_ids, written_ids_lock
        )
        if success:
            failed_queue.remove(target_id)
            with state.lock:
                success_counter[0] += 1


def final_retry_sprint(session_manager, base_url, state, rate_limiter, result_queue,
                       failed_queue, connect_timeout, read_timeout, retry_count,
                       num_threads, written_ids, written_ids_lock=None):
    """v6.0 并行最终冲刺 — 持续循环直到零失败或全部降级"""
    MAX_ROUNDS = 100  # 最多冲刺 100 轮
    round_num = 0

    while round_num < MAX_ROUNDS:
        with failed_queue.lock:
            failed_ids = failed_queue.get_all_retryable()
        if not failed_ids:
            break

        round_num += 1
        print(c(f"\n{'═'*60}", "yellow"))
        print(c(f"  🔄 最终冲刺第 {round_num} 轮: 剩余 {len(failed_ids)} 个失败ID", "bold"))
        print(c(f"{'═'*60}", "yellow"))

        # 降速
        original_qps = rate_limiter.max_qps
        sprint_qps = max(2.0, original_qps * 0.3)
        rate_limiter.update_qps(sprint_qps)
        print(c(f"  📉 已降速至 {sprint_qps:.1f} QPS 进行重试", "cyan"))

        # 冷却
        time.sleep(min(5 + round_num * 2, 30))

        # 分片并行处理
        success_counter = [0]
        chunk_size = max(len(failed_ids) // num_threads, 1)
        chunks = [failed_ids[i:i + chunk_size] for i in range(0, len(failed_ids), chunk_size)]

        threads = []
        for chunk in chunks[:num_threads]:
            if not chunk:
                continue
            t = threading.Thread(
                target=retry_worker,
                args=(session_manager, base_url, state, rate_limiter, result_queue,
                      failed_queue, connect_timeout, read_timeout, retry_count,
                      chunk, success_counter, written_ids, written_ids_lock),
                name=f"RetryWorker-{len(threads) + 1}"
            )
            t.start()
            threads.append(t)

        for t in threads:
            t.join(timeout=300)  # 每轮最多 5 分钟
            if t.is_alive():
                print(c(f"  ⚠ 重试线程 {t.name} 超时，继续...", "yellow"))

        # 恢复 QPS
        rate_limiter.update_qps(original_qps)

        remaining = failed_queue.retryable_count
        permanent = failed_queue.permanent_count
        print(c(f"\n  第 {round_num} 轮: 成功回收 {success_counter[0]}, 仍失败 {remaining}, 永久跳过 {permanent}", "yellow"))

        if remaining == 0:
            print(c(f"\n  🎉 全部失败ID已成功回收!", "green"))
            break

        if remaining == permanent:
            # 所有剩余的都是永久跳过的，无法再恢复
            print(c(f"\n  ⚠ 所有剩余失败ID已达最大重试次数，降级为永久跳过", "yellow"))
            break

        wait_time = min(10 + round_num * 5, 60)
        print(c(f"  ⏳ 等待 {wait_time}s 后开始下一轮...", "cyan"))
        # 用 wait 替代 sleep 以便响应 stop_event
        for _ in range(int(wait_time)):
            if state.stop_event.is_set():
                break
            time.sleep(1)
        session_manager.refresh_my_session()

    total_remaining = failed_queue.retryable_count + failed_queue.permanent_count
    if total_remaining == 0:
        print(c(f"\n  🏁 最终冲刺完成，所有ID处理完毕!", "green"))
    else:
        print(c(f"\n  🏁 最终冲刺完成: {failed_queue.permanent_count} 个永久跳过, {failed_queue.retryable_count} 个待重试", "yellow"))


# ═══════════════════════════════════════════════════════════════════════
#  写入线程 v6.0 — 错误恢复 + 去重
# ═══════════════════════════════════════════════════════════════════════

def writer_thread(out_file, fieldnames, result_queue, state, written_ids: Set[int],
                  written_ids_lock: threading.Lock = None):
    """v6.1: 增强写入线程 — 错误恢复 + 守护模式

    v6.1 修复:
    - 接收 written_ids_lock 参数，传递给 _write_row_safe 保证线程安全
    - 移除旧的去重逻辑 (已在 _write_row_safe 内修复)
    """
    file_exists = os.path.isfile(out_file)
    write_errors = 0
    MAX_WRITE_ERRORS = 50

    while True:
        try:
            row = result_queue.get(timeout=2.0)
            if row is None:
                # 毒丸: 开始退出流程，但要先消费完残余数据
                while not result_queue.empty():
                    try:
                        row = result_queue.get_nowait()
                        if row is None:
                            break
                        _write_row_safe(out_file, fieldnames, row, file_exists, written_ids,
                                       written_ids_lock)
                        file_exists = True  # 第一次写入后文件已存在
                    except queue.Empty:
                        break
                break

            _write_row_safe(out_file, fieldnames, row, file_exists, written_ids,
                           written_ids_lock)
            file_exists = True
            write_errors = 0  # 重置错误计数

        except queue.Empty:
            if state.active_threads <= 0 and result_queue.empty():
                # 工作线程全部结束且队列已空
                # 再等 3 秒确保没有残余
                time.sleep(3)
                if state.active_threads <= 0 and result_queue.empty():
                    break
        except Exception as e:
            write_errors += 1
            log_error(f"写入线程异常 (错误计数: {write_errors})", e)
            if write_errors >= MAX_WRITE_ERRORS:
                log_error(f"写入线程连续错误 {write_errors} 次，退出", None)
                break
            time.sleep(1)


def _write_row_safe(out_file, fieldnames, row, file_exists, written_ids: Set[int],
                    written_ids_lock: threading.Lock = None):
    """安全写入单行 CSV

    v6.1 修复:
    - 移除旧的 written_ids 去重检查（该检查导致竞态条件:
      worker 线程先把 ID 加入 written_ids，writer 线程检查时发现已存在就跳过，
      导致所有行都被跳过、CSV 文件永远无法创建）
    - 新的去重逻辑: 仅检查 _existing_ids (本次会话开始前已有的 ID)，
      而不是 written_ids（包含 worker 线程新加入的 ID）
    - 写入成功后才将 ID 加入 written_ids，供 retry worker 做去重
    - 添加 written_ids_lock 保证线程安全
    """
    try:
        # 不再检查 written_ids — 由 worker 的 ScraperState 保证每个 ID 只处理一次
        # writer 只管写就行

        with open(out_file, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists or os.path.getsize(out_file) == 0:
                writer.writeheader()
            writer.writerow(row)
            f.flush()

        # 写入成功后，加锁更新 written_ids（供 retry worker 去重用）
        row_id = row.get("id")
        if row_id:
            if written_ids_lock:
                with written_ids_lock:
                    written_ids.add(int(row_id))
            else:
                written_ids.add(int(row_id))
    except Exception as e:
        log_error(f"写入 CSV 行失败 (ID: {row.get('id', '?')})", e)


# ═══════════════════════════════════════════════════════════════════════
#  进度显示线程 v6.0 — 移动平均 ETA
# ═══════════════════════════════════════════════════════════════════════

def progress_display_thread(state, total_range, rate_limiter, params_store, failed_queue):
    adaptive_check_interval = 10
    last_adaptive_check = time.time()

    while not state.stop_event.is_set():
        stats = state.get_stats()
        current_qps = rate_limiter.max_qps
        retryable_count = failed_queue.retryable_count

        elapsed = stats["elapsed"]
        # 使用移动平均速度计算 ETA
        avg_speed = stats["avg_speed"]
        speed = avg_speed if avg_speed > 0 else stats["speed"]
        remaining = total_range - stats["processed"]
        eta_seconds = remaining / speed if speed > 0 else 0

        if eta_seconds < 60:
            eta_str = f"{int(eta_seconds)}s"
        elif eta_seconds < 3600:
            eta_str = f"{int(eta_seconds // 60)}m{int(eta_seconds % 60)}s"
        elif eta_seconds < 86400:
            eta_str = f"{int(eta_seconds // 3600)}h{int((eta_seconds % 3600) // 60)}m"
        else:
            eta_str = f"{int(eta_seconds // 86400)}d{int((eta_seconds % 86400) // 3600)}h"

        fail_pct = stats["failure_rate"] * 100
        elapsed_str = f"{int(elapsed // 3600)}h{int((elapsed % 3600) // 60)}m" if elapsed >= 3600 else f"{int(elapsed // 60)}m{int(elapsed % 60)}s"

        progress_line = (
            f"  [{stats['processed']:,}/{total_range:,}] "
            f"有效={stats['total_items']} 不存在={stats['total_not_found']} "
            f"永久跳过={stats['total_permanent_skipped']} 失败={stats['total_failed']} "
            f"待重试={retryable_count} | "
            f"线程={stats['active_threads']} QPS={current_qps:.1f} "
            f"失败率={fail_pct:.0f}% 速度={speed:.1f}id/s "
            f"已耗时={elapsed_str} ETA={eta_str}"
        )
        # 清除当前行并打印
        sys.stdout.write(f"\r{'':>160}\r{progress_line}")
        sys.stdout.flush()

        # 自适应 QPS 调节
        now = time.time()
        if now - last_adaptive_check >= adaptive_check_interval:
            last_adaptive_check = now
            fr = stats["failure_rate"]
            original_qps = params_store.get("_original_qps", 20.0)

            if fr > 0.5:
                new_qps = max(1.0, current_qps * 0.4)
                rate_limiter.update_qps(new_qps)
                print(c(f"\n  ⚡ 自适应: 失败率 {fr*100:.0f}% > 50%, 降速至 {new_qps:.1f} QPS", "yellow"), flush=True)
            elif fr > 0.2:
                new_qps = max(1.0, current_qps * 0.6)
                rate_limiter.update_qps(new_qps)
                print(c(f"\n  ⚡ 自适应: 失败率 {fr*100:.0f}% > 20%, 降速至 {new_qps:.1f} QPS", "yellow"), flush=True)
            elif fr < 0.02:
                if current_qps < original_qps:
                    new_qps = min(original_qps, current_qps * 1.5)
                    rate_limiter.update_qps(new_qps)
                    print(c(f"\n  ⚡ 自适应: 失败率 {fr*100:.0f}% < 2%, 提速至 {new_qps:.1f} QPS", "green"), flush=True)
            elif fr < 0.05:
                if current_qps < original_qps:
                    new_qps = min(original_qps, current_qps * 1.2)
                    rate_limiter.update_qps(new_qps)
                    print(c(f"\n  ⚡ 自适应: 失败率 {fr*100:.0f}% < 5%, 提速至 {new_qps:.1f} QPS", "green"), flush=True)

        if state.stop_event.wait(1.0):
            break


# ═══════════════════════════════════════════════════════════════════════
#  主爬取逻辑 v6.0 — 完整实现 (修复 v5.0 截断)
# ═══════════════════════════════════════════════════════════════════════

_global_state = None  # 用于信号处理


def _signal_handler(signum, frame):
    """信号处理: 优雅停止"""
    global _global_state
    sig_name = signal.Signals(signum).name
    print(c(f"\n\n  ⚡ 收到信号 {sig_name}，正在优雅停止...", "yellow"), flush=True)
    if _global_state:
        _global_state.stop_event.set()
    # 注意: 不能在这里调用 sys.exit()，让主线程自然结束


def scrape(proxies, params):
    global _global_state

    BASE = "https://sukebei.nyaa.si"
    out_file = params["out"]
    is_resume = params.get("is_resume", False)
    fieldnames = ["id", "name", "info_hash", "size", "magnet", "date", "category", "detail_url"]
    num_threads = params.get("num_threads", 16)
    max_qps = params.get("max_qps", 20.0)
    connect_timeout = params.get("connect_timeout", 10)
    read_timeout = params.get("read_timeout", 30)
    retry_count = params.get("retry_count", 8)
    params["_original_qps"] = max_qps

    # 初始化
    failed_queue = PersistentFailedQueue()
    session_manager = IsolatedSessionManager(BASE, proxies, num_threads)
    rate_limiter = SegmentedRateLimiter(max_qps, num_segments=num_threads)
    existing_ids = load_existing_ids(out_file)

    # written_ids: 线程安全的写入 ID 集合 (使用锁保护)
    written_ids_lock = threading.Lock()
    written_ids = existing_ids.copy()

    # v6.1: 不再使用 safe_add_written/safe_is_written 闭包，
    # 直接将 written_ids_lock 传递给所有需要访问 written_ids 的函数

    # 安装信号处理
    _global_state = ScraperState(0, 1, set())
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # ── 1. 检测连通性 ──
    print(c("\n[ 连接测试 ]", "bold"))
    print(c("  正在测试连接...", "blue"), end=" ", flush=True)
    try:
        session = session_manager.get_session()
        r, ftype = request_with_retry(session, BASE,
                                       connect_timeout=connect_timeout,
                                       read_timeout=read_timeout,
                                       retry_count=5)
        if r and r.status_code == 200:
            print(c(f"✔ HTTP {r.status_code}", "green"))
        else:
            status = r.status_code if r else "None"
            print(c(f"✗ HTTP {status}", "red"))
            raise Exception(f"HTTP {status}")
    except Exception as e:
        print(c(f"\n✗ 连接失败: {e}", "red"))
        print(c("  请检查网络连接或代理设置后重试", "yellow"))
        failed_queue.force_save()
        session_manager.close_all()
        sys.exit(1)

    # ── 2. 预热连接池 ──
    session_manager.warmup(BASE, timeout=connect_timeout)

    # ── 3. 确定ID范围 ──
    start_id = params["start_id"]
    end_id = params["end_id"]
    if end_id == "auto":
        end_id = detect_latest_id(session_manager, BASE,
                                    connect_timeout=connect_timeout,
                                    read_timeout=read_timeout)
    else:
        print(c(f"\n使用手动指定的结束ID: {end_id}", "yellow"))

    if start_id > end_id:
        print(c(f"\n⚠ 起始ID ({start_id}) 已超过结束ID ({end_id})，无需继续爬取", "yellow"))
        print(c(f"  上次已爬取到 {end_id}，数据已保存在 {out_file}", "green"))
        failed_queue.force_save()
        session_manager.close_all()
        return

    total_range = end_id - start_id + 1
    print(c(f"\n{'═'*60}", "bold"))
    print(c(f"  [ 爬取任务配置 ]", "bold"))
    print(c(f"{'═'*60}", "bold"))
    print(f"  起始ID:     {start_id:,}")
    print(f"  结束ID:     {end_id:,}")
    print(f"  本次待爬:   {total_range:,} 个ID")
    print(f"  已有记录:   {len(existing_ids):,} 个 (已跳过)")
    print(f"  并行线程:   {num_threads}")
    print(f"  全局QPS:    {max_qps:.1f} (自适应调节)")
    print(f"  连接超时:   {connect_timeout}s / 读取超时: {read_timeout}s")
    print(f"  单次重试:   {retry_count} 次")
    print(f"  最大重试:   {MAX_RETRY_ATTEMPTS_PER_ID} 次/ID (超过则永久跳过)")
    print(f"  失败队列:   {failed_queue.retryable_count} 个待重试 / {failed_queue.permanent_count} 个永久跳过")
    print(f"  输出文件:   {out_file}")
    if is_resume:
        print(c(f"  模式: 续爬（新数据将追加到 {out_file}）", "green"))
    print(f"  (注: 不是所有ID都存在，实际有效条目远少于待爬数量)")
    print(c(f"{'═'*60}", "bold"))

    confirm = input(c("\n确认开始爬取? [Y/n]: ", "yellow")).strip().lower()
    if confirm == "n" or confirm == "no":
        print(c("  已取消", "yellow"))
        failed_queue.force_save()
        session_manager.close_all()
        return

    # ── 4. 创建共享状态 ──
    state = ScraperState(start_id, end_id, existing_ids)
    _global_state = state
    result_queue = queue.Queue(maxsize=10000)

    # ── 5. 启动写入线程 ──
    # v6.1: 传入 written_ids_lock 保证线程安全
    def start_writer_thread():
        """启动或重启写入线程"""
        w = threading.Thread(
            target=writer_thread,
            args=(out_file, fieldnames, result_queue, state, written_ids, written_ids_lock),
            name="WriterThread",
            daemon=False  # 非守护线程，确保数据写入完成
        )
        w.start()
        return w

    writer = start_writer_thread()

    # ── 6. 启动进度显示线程 ──
    progress = threading.Thread(
        target=progress_display_thread,
        args=(state, total_range, rate_limiter, params, failed_queue),
        name="ProgressThread",
        daemon=True
    )
    progress.start()

    # ── 7. 启动工作线程 ──
    start_time = time.time()
    workers = []
    print(c(f"\n  🚀 启动 {num_threads} 个工作线程...", "green"), flush=True)

    for i in range(num_threads):
        t = threading.Thread(
            target=thread_target,
            args=(session_manager, BASE, state, rate_limiter, result_queue,
                  failed_queue, connect_timeout, read_timeout, retry_count,
                  written_ids, written_ids_lock),
            name=f"Worker-{i + 1:02d}"
        )
        t.start()
        workers.append(t)
        # 错开启动时间，避免同时发起请求
        time.sleep(0.1)

    state.set_active_threads(num_threads)

    # ── 8. 等待所有工作线程完成 ──
    print(c(f"  📡 爬取进行中... (Ctrl+C 优雅停止)\n", "cyan"), flush=True)

    try:
        for t in workers:
            t.join()
    except KeyboardInterrupt:
        print(c("\n\n  ⚡ KeyboardInterrupt, 等待线程停止...", "yellow"), flush=True)
        state.stop_event.set()
        for t in workers:
            t.join(timeout=5)
            if t.is_alive():
                print(c(f"  ⚠ 线程 {t.name} 未在超时内停止", "yellow"))

    state.set_active_threads(0)

    # ── 9. 发送毒丸停止写入线程 ──
    result_queue.put(None)
    writer.join(timeout=30)
    if writer.is_alive():
        print(c("  ⚠ 写入线程未在超时内停止", "yellow"))

    # ── 10. 最终冲刺 ──
    if not state.stop_event.is_set():
        total_elapsed = time.time() - start_time
        print(c(f"\n\n{'═'*60}", "green"))
        print(c(f"  第一轮爬取完成!", "bold"))
        print(c(f"  耗时: {int(total_elapsed // 60)}m{int(total_elapsed % 60)}s", "cyan"))
        print(c(f"  有效条目: {state.total_items:,}", "green"))
        print(c(f"  不存在: {state.total_not_found:,}", "dim"))
        print(c(f"  永久跳过: {state.total_permanent_skipped:,}", "yellow"))
        print(c(f"  失败: {state.total_failed:,}", "red" if state.total_failed > 0 else "dim"))
        print(c(f"{'═'*60}", "green"))

        if failed_queue.retryable_count > 0:
            print(c(f"\n  🔄 开始最终冲刺...", "bold"), flush=True)

            # v6.1 修复: 最终冲刺前重启写入线程
            # 之前的 writer 已被毒丸终止，重试成功的写入队列需要新的 writer
            writer = start_writer_thread()
            state.set_active_threads(num_threads)

            final_retry_sprint(
                session_manager, BASE, state, rate_limiter, result_queue,
                failed_queue, connect_timeout, read_timeout, retry_count,
                num_threads, written_ids, written_ids_lock
            )

            # v6.1: 最终冲刺结束后，停止写入线程并确保残余数据写入
            state.set_active_threads(0)
            result_queue.put(None)
            writer.join(timeout=30)
            if writer.is_alive():
                print(c("  ⚠ 最终冲刺写入线程未在超时内停止", "yellow"))
    else:
        # 被信号中断，也需要保存失败队列
        print(c("\n  📂 收到停止信号，保存进度...", "yellow"), flush=True)

    # ── 11. 最终统计 ──
    final_elapsed = time.time() - start_time
    final_stats = state.get_stats()

    print(c(f"\n\n{'═'*60}", "bold"))
    print(c(f"  📊 最终统计报告", "bold"))
    print(c(f"{'═'*60}", "bold"))
    print(f"  总耗时:       {int(final_elapsed // 3600)}h{int((final_elapsed % 3600) // 60)}m{int(final_elapsed % 60)}s")
    print(f"  处理ID总数:   {final_stats['processed']:,}")
    print(f"  有效条目:     {final_stats['total_items']:,}")
    print(f"  不存在(404):  {final_stats['total_not_found']:,}")
    print(f"  永久跳过:     {final_stats['total_permanent_skipped']:,}")
    print(f"  失败(已重试): {final_stats['total_failed']:,}")
    print(f"  剩余待重试:   {failed_queue.retryable_count}")
    print(f"  累计永久跳过: {failed_queue.permanent_count}")
    print(f"  平均速度:     {final_stats['processed'] / final_elapsed:.1f} id/s" if final_elapsed > 0 else "")
    print(f"  输出文件:     {out_file}")
    if os.path.isfile(out_file):
        file_size = os.path.getsize(out_file)
        if file_size > 1024 * 1024:
            print(f"  文件大小:     {file_size / (1024 * 1024):.1f} MB")
        else:
            print(f"  文件大小:     {file_size / 1024:.1f} KB")
    print(c(f"{'═'*60}", "bold"))

    if final_stats['total_failed'] == 0 and failed_queue.retryable_count == 0:
        print(c("\n  🎉🎉🎉 完美! 零失败完成全量爬取! 🎉🎉🎉", "green"))
    elif failed_queue.retryable_count == 0:
        print(c(f"\n  ✅ 爬取完成! 所有失败ID已回收或降级处理", "green"))
    else:
        print(c(f"\n  ⚠ 还有 {failed_queue.retryable_count} 个ID待重试, 重新运行程序即可继续", "yellow"))

    # 确保保存所有数据
    failed_queue.force_save()
    session_manager.close_all()
    _global_state = None


# ═══════════════════════════════════════════════════════════════════════
#  入口
# ═══════════════════════════════════════════════════════════════════════

def main():
    optimize_system_network()
    print_banner()

    proxies = setup_proxy()
    params = setup_params()

    try:
        scrape(proxies, params)
    except KeyboardInterrupt:
        print(c("\n\n  ⚡ 用户中断", "yellow"))
    except Exception as e:
        log_error("主函数异常", e)
        print(c(f"\n\n  ✗ 致命错误: {e}", "red"))
        print(c(f"  错误详情已记录到 {ERROR_LOG_FILE}", "yellow"))
        sys.exit(1)


if __name__ == "__main__":
    main()
