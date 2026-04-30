#!/usr/bin/env python3
"""
sukebei.nyaa.si 全量元数据爬虫  v2.0
策略变更：废弃列表页翻页，改为直接遍历 /view/{id} 链接爬取全站数据
自动检测最新条目ID，从最早(92)遍历到最新，支持断点续爬
爬取资源名称、信息哈希值、Magnet链接、资源大小，存储为 CSV
支持交互式配置代理（HTTP/HTTPS/SOCKS5）
"""
import requests
from bs4 import BeautifulSoup
import csv
import time
import sys
import os
import re
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
║     sukebei.nyaa.si 全量元数据爬虫  v2.0 直链遍历版          ║
║  策略: 自动检测最新ID → /view/{id} 直连遍历 → 全站爬取      ║
║  爬取: 资源名称 | InfoHash | Magnet | 大小 | 日期            ║
║  支持: 断点续爬 | 代理 | 自定义ID范围                       ║
╚══════════════════════════════════════════════════════════════╝
""", "cyan"))

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

    # 起始ID
    start_id = input(c("起始条目ID [默认 92]: ", "yellow")).strip()
    start_id = int(start_id) if start_id.isdigit() else 92

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
    max_consecutive_fail = input(c("连续失败多少次后暂停等待 [默认 50]: ", "yellow")).strip()
    max_consecutive_fail = int(max_consecutive_fail) if max_consecutive_fail.isdigit() else 50

    # 暂停等待秒数
    pause_wait = input(c("暂停等待秒数 [默认 300]: ", "yellow")).strip()
    try:
        pause_wait = float(pause_wait)
    except Exception:
        pause_wait = 300

    # 输出文件
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_out = f"sukebei_nyaa_data_{ts}.csv"
    out_file = input(c(f"输出文件名 [默认 {default_out}]: ", "yellow")).strip() or default_out

    return {
        "start_id": start_id,
        "end_id": end_id,
        "delay": delay,
        "max_consecutive_fail": max_consecutive_fail,
        "pause_wait": pause_wait,
        "out": out_file,
    }

# ─────────────────────────── 自动检测最新条目ID ──────────────────
def detect_latest_id(session, base_url):
    """访问首页，按ID降序排列，提取第一条目的ID作为最新ID"""
    print(c("\n[ 自动检测最新条目ID ]", "bold"))
    try:
        # 访问首页，按ID降序
        r = session.get(base_url, params={"s": "id", "o": "desc"}, timeout=15)
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
            r = session.get(f"{base_url}/view/{test_id}", timeout=10, allow_redirects=False)
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
                r = session.get(f"{base_url}/view/{check_id}", timeout=10, allow_redirects=False)
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
        # nyaa详情页标题格式通常是: "名称 | sukebei.nyaa.si"
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
                import base64
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
                    # 取最后一个（通常是文件列表中的最后一个，或总计）
                    # 简单策略：取包含 GB 或 TB 的
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
        except Exception:
            pass
    return ids

# ─────────────────────────── 主爬取逻辑 ─────────────────────────
def scrape(proxies, params):
    BASE = "https://sukebei.nyaa.si"
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
    fieldnames = ["id", "name", "info_hash", "magnet", "size", "date", "detail_url"]

    # ── 1. 检测连通性 ──
    print(c("\n正在测试连接...", "blue"))
    try:
        r = session.get(BASE, timeout=15)
        r.raise_for_status()
        print(c(f"✔ 连接成功 (HTTP {r.status_code})", "green"))
    except Exception as e:
        print(c(f"✗ 连接失败: {e}", "red"))
        sys.exit(1)

    # ── 2. 确定ID范围 ──
    start_id = params["start_id"]
    end_id = params["end_id"]

    if end_id == "auto":
        end_id = detect_latest_id(session, BASE)
    else:
        print(c(f"\n使用手动指定的结束ID: {end_id}", "yellow"))

    total_range = end_id - start_id + 1
    print(c(f"\n[ 爬取范围 ]", "bold"))
    print(f"  起始ID: {start_id}")
    print(f"  结束ID: {end_id}")
    print(f"  总范围: {total_range:,} 个ID")
    print(f"  (注: 不是所有ID都存在，实际有效条目会少于该数字)")

    confirm = input(c("\n确认开始爬取? [Y/n]: ", "yellow")).strip().lower()
    if confirm == "n":
        print(c("已取消。", "yellow"))
        sys.exit(0)

    # ── 3. 断点续爬：加载已爬取的ID ──
    existing_ids = load_existing_ids(out_file)
    if existing_ids:
        skipped_count = len(existing_ids)
        print(c(f"\n✔ 断点续爬: 检测到已有 {skipped_count} 条记录，将自动跳过已爬取的ID", "green"))
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
            # 断点续爬：跳过已爬取的ID
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
                r = session.get(detail_url, timeout=15, allow_redirects=False)
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
                print(c(f"  已保存 {total_items} 条有效数据到 {out_file}", "green"))
                print(c(f"  当前进度: ID {current_id} / {end_id}", "cyan"))
                print(c(f"  再次运行本脚本并指定相同输出文件即可断点续爬", "cyan"))
                break

            except requests.exceptions.ConnectionError as e:
                total_failed += 1
                consecutive_fail += 1
                print(c(f"\n  ✗ 连接错误 (ID {current_id}): {e}", "red"))

            except requests.exceptions.Timeout:
                total_failed += 1
                consecutive_fail += 1
                print(c(f"\n  ✗ 请求超时 (ID {current_id})", "red"))

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

    print(c(f"""
╔════════════════════════════════════════════════╗
║  爬取完成！                                     ║
║  ID 范围   : {start_id} ~ {end_id:<24}║
║  有效条目  : {total_items:<34}║
║  不存在ID  : {total_not_found:<34}║
║  请求失败  : {total_failed:<34}║
║  已跳过    : {skipped_count:<34}║
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
