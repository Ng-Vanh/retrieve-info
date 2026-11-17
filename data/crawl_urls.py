#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
HUST Recursive URL Crawler (Hỗ trợ Resume, lọc theo domain của Seed, tùy chọn depth)
"""

import os
import re
import time
import argparse
import threading
from queue import Queue, Empty
from urllib.parse import urlparse, urljoin, urldefrag
from html.parser import HTMLParser
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CẤU HÌNH MẶC ĐỊNH ---
DEFAULT_SEEDS = [
    "https://hust.edu.vn",
    "https://ctt.hust.edu.vn",
    "https://ts.hust.edu.vn",
    "https://ctsv.hust.edu.vn",
    "https://clc.hust.edu.vn",
    "https://library.hust.edu.vn",
    "https://soict.hust.edu.vn",
    "https://seee.hust.edu.vn",
    "https://sme.hust.edu.vn",
    "https://scls.hust.edu.vn",
    "https://smse.hust.edu.vn",
    "https://sem.hust.edu.vn",
    "https://fami.hust.edu.vn",
    "https://sep.hust.edu.vn",
    "https://sofl.hust.edu.vn",
    "https://fpt.hust.edu.vn",
    "https://congnghegiaoduc.hust.edu.vn",
    "https://gdqp.hust.edu.vn",
    "https://gdtc.hust.edu.vn",
    "https://svbk.hust.edu.vn",
]

# Loại bỏ trang đăng nhập, app nội bộ, logout
EXCLUDE_PATTERNS = [
    r"login",
    r"logout",
    r"sso\.hust\.edu\.vn",
    r"mail\.hust\.edu\.vn",
    r"dk-sis\.hust\.edu\.vn",
    r"dk-sie\.hust\.edu\.vn",
    r"qldt\.hust\.edu\.vn",
    r"lms\.hust\.edu\.vn",
    r"javascript:",
    r"mailto:",
    r"tel:",
]

# Các định dạng tài liệu có giá trị cho Search Engine (Lưu vào set/file, không cào HTML tiếp)
DOC_EXTENSIONS = (
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx"
)

# Các định dạng file rác/media/tài nguyên web (bỏ qua hoàn toàn)
IGNORE_EXTENSIONS = (
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".svg", ".webp", ".ico",
    ".mp3", ".mp4", ".avi", ".mkv", ".mov",
    ".zip", ".rar", ".7z", ".tar", ".gz",
    ".css", ".js", ".json", ".xml",
    ".woff", ".woff2", ".ttf", ".eot"
)


class LinkExtractor(HTMLParser):
    """Trích xuất tất cả các thẻ <a href="..."> từ nội dung HTML."""
    def __init__(self, base_url):
        super().__init__()
        self.base_url = base_url
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "a":
            for attr, val in attrs:
                if attr.lower() == "href" and val:
                    val = val.strip()
                    full_url = urljoin(self.base_url, val)
                    self.links.append(full_url)


class HustUnlimitedCrawler:
    def __init__(
        self,
        seeds=None,
        output_file="hust_urls.txt",
        visited_file=None,
        max_workers=10,
        max_depth=None,
        allow_all_hust=False,
        fresh=False,
    ):
        self.seeds = seeds or DEFAULT_SEEDS
        self.output_file = output_file
        self.visited_file = visited_file or f"{output_file}.visited.txt"
        self.max_workers = max_workers
        self.max_depth = max_depth
        self.allow_all_hust = allow_all_hust
        self.fresh = fresh

        # Xác định các domain được phép crawl dựa trên seeds truyền vào
        self.allowed_hosts = set()
        for s in self.seeds:
            h = urlparse(s).hostname
            if h:
                self.allowed_hosts.add(h.lower())

        # SET để khử trùng lặp hoàn toàn
        self.discovered_set = set()  # Chứa tất cả URL duy nhất đã phát hiện
        self.visited_set = set()     # Chứa URL đã được tải và quét thẻ <a>

        # Hàng đợi chứa tuple (url, depth)
        self.queue = Queue()

        self.lock = threading.Lock()
        self.running = True
        self.active_workers = 0

        self.session = requests.Session()
        retries = Retry(
            total=2,
            backoff_factor=0.3,
            status_forcelist=[500, 502, 503, 504],
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=max_workers, pool_maxsize=max_workers * 2)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })

    def is_valid_url(self, url: str) -> bool:
        """Kiểm tra URL có thuộc phạm vi domain cho phép và hợp lệ."""
        try:
            parsed = urlparse(url)
            if parsed.scheme not in ("http", "https"):
                return False

            hostname = parsed.hostname
            if not hostname:
                return False
            hostname = hostname.lower()

            if self.allow_all_hust:
                if hostname != "hust.edu.vn" and not hostname.endswith(".hust.edu.vn"):
                    return False
            else:
                # Chỉ cho phép đúng các host trong seeds (hoặc subdomain trực tiếp của nó)
                in_scope = any(
                    hostname == allowed or hostname.endswith("." + allowed)
                    for allowed in self.allowed_hosts
                )
                if not in_scope:
                    return False

            url_lower = url.lower()
            for pattern in EXCLUDE_PATTERNS:
                if re.search(pattern, url_lower):
                    return False

            path_lower = parsed.path.lower()
            if any(path_lower.endswith(ext) for ext in IGNORE_EXTENSIONS):
                return False

            return True
        except Exception:
            return False

    def normalize_url(self, url: str) -> str:
        """Chuẩn hóa URL: bỏ fragment (#anchor), đưa hostname về lowercase."""
        url, _ = urldefrag(url)
        parsed = urlparse(url)
        netloc = parsed.netloc.lower()
        path = parsed.path
        if path != "/" and path.endswith("/"):
            path = path[:-1]
        return parsed._replace(netloc=netloc, path=path).geturl()

    def is_doc_url(self, url: str) -> bool:
        path_lower = urlparse(url).path.lower()
        return any(path_lower.endswith(ext) for ext in DOC_EXTENSIONS)

    def load_previous_state(self):
        """Nạp lại các URL đã tìm thấy và đã duyệt từ lần chạy trước để resume."""
        if self.fresh:
            for path in (self.output_file, self.visited_file):
                if os.path.exists(path):
                    os.remove(path)
            return

        if os.path.exists(self.output_file):
            with open(self.output_file, "r", encoding="utf-8") as f:
                for line in f:
                    u = line.strip()
                    if u:
                        self.discovered_set.add(u)

        if os.path.exists(self.visited_file):
            with open(self.visited_file, "r", encoding="utf-8") as f:
                for line in f:
                    u = line.strip()
                    if u:
                        self.visited_set.add(u)

        resumed_queue_count = 0
        for u in self.discovered_set:
            if u not in self.visited_set and not self.is_doc_url(u) and self.is_valid_url(u):
                self.queue.put((u, 0))
                resumed_queue_count += 1

        if self.discovered_set:
            print(f"[*] RESUME: Đã nạp {len(self.discovered_set)} URLs từ lần chạy trước.")
            print(f"[*] RESUME: Đã duyệt {len(self.visited_set)} trang | Cần cào tiếp: {resumed_queue_count} trang.")

    def worker_thread(self, f_out, f_visited):
        """Hàm thực thi của từng luồng worker."""
        while self.running:
            try:
                url, depth = self.queue.get(timeout=2)
            except Empty:
                with self.lock:
                    if self.active_workers == 0:
                        break
                continue

            with self.lock:
                self.active_workers += 1

            try:
                should_crawl_children = (self.max_depth is None) or (depth < self.max_depth)
                if should_crawl_children and not self.is_doc_url(url):
                    self.process_html_page(url, depth, f_out)
            except Exception:
                pass
            finally:
                with self.lock:
                    self.active_workers -= 1
                    if url not in self.visited_set:
                        self.visited_set.add(url)
                        f_visited.write(url + "\n")
                        f_visited.flush()
                self.queue.task_done()

    def process_html_page(self, url: str, depth: int, f_out):
        """Tải trang HTML và bóc tách các link con."""
        try:
            resp = self.session.get(url, timeout=10, allow_redirects=True)
            if resp.status_code != 200:
                return

            content_type = resp.headers.get("Content-Type", "").lower()
            if "text/html" not in content_type:
                return

            parser = LinkExtractor(resp.url)
            parser.feed(resp.text)

            new_links = []
            next_depth = depth + 1
            enqueue_children = (self.max_depth is None) or (next_depth < self.max_depth)

            for raw_link in parser.links:
                norm_link = self.normalize_url(raw_link)
                if self.is_valid_url(norm_link):
                    with self.lock:
                        if norm_link not in self.discovered_set:
                            self.discovered_set.add(norm_link)
                            new_links.append(norm_link)
                            if enqueue_children and not self.is_doc_url(norm_link):
                                self.queue.put((norm_link, next_depth))
                            f_out.write(norm_link + "\n")

            if new_links:
                with self.lock:
                    f_out.flush()

        except Exception:
            pass

    def run(self):
        depth_label = "Không giới hạn" if self.max_depth is None else str(self.max_depth)
        scope_label = "Toàn bộ *.hust.edu.vn" if self.allow_all_hust else ", ".join(sorted(self.allowed_hosts))

        print(f"[*] Khởi động Crawler (Độ sâu: {depth_label})")
        print(f"[*] Phạm vi domain cho phép: {scope_label}")
        print(f"[*] Số luồng song song: {self.max_workers}")
        print(f"[*] File xuất URL: {os.path.abspath(self.output_file)}")
        print(f"[*] File lưu trạng thái đã duyệt: {os.path.abspath(self.visited_file)}")
        print(f"[*] Nhấn Ctrl+C bất kỳ lúc nào để dừng; chạy lại cùng lệnh để cào tiếp.\n" + "="*70)

        self.load_previous_state()

        with open(self.output_file, "a", encoding="utf-8") as f_out, \
             open(self.visited_file, "a", encoding="utf-8") as f_visited:

            for seed in self.seeds:
                norm = self.normalize_url(seed)
                if self.is_valid_url(norm):
                    if norm not in self.discovered_set:
                        self.discovered_set.add(norm)
                        if not self.is_doc_url(norm):
                            self.queue.put((norm, 0))
                        f_out.write(norm + "\n")
            f_out.flush()

            threads = []
            for _ in range(self.max_workers):
                t = threading.Thread(target=self.worker_thread, args=(f_out, f_visited))
                t.daemon = True
                t.start()
                threads.append(t)

            start_time = time.time()
            try:
                while any(t.is_alive() for t in threads):
                    with self.lock:
                        visited_count = len(self.visited_set)
                        total_found = len(self.discovered_set)
                        queue_size = self.queue.qsize()

                    elapsed = int(time.time() - start_time)
                    print(
                        f"  [Đã duyệt: {visited_count:>5} trang | "
                        f"Tìm thấy: {total_found:>6} URLs | "
                        f"Còn trong hàng đợi: {queue_size:>5} | "
                        f"Thời gian: {elapsed}s]",
                        end="\r"
                    )
                    time.sleep(1)

            except KeyboardInterrupt:
                print("\n\n[!] Nhận tín hiệu dừng (Ctrl+C). Đang lưu trạng thái để lần sau chạy tiếp...")
                self.running = False

            for t in threads:
                t.join(timeout=1)

        print("\n" + "="*70)
        print(f"[HOÀN TẤT] Tổng số URL duy nhất trong Set: {len(self.discovered_set)}")
        print(f"[HOÀN TẤT] Số trang đã duyệt xong: {len(self.visited_set)}")
        print(f"[HOÀN TẤT] Danh sách URL: {os.path.abspath(self.output_file)}")


def main():
    parser = argparse.ArgumentParser(description="HUST Recursive URL Crawler (with Resume & Domain Scoping)")
    parser.add_argument("--seeds", nargs="*", help="Danh sách suburl truyền vào", default=None)
    parser.add_argument("--seeds-file", type=str, help="File txt chứa danh sách seed URLs", default=None)
    parser.add_argument("--depth", type=int, help="Giới hạn độ sâu (bỏ trống = không giới hạn)", default=None)
    parser.add_argument("--workers", type=int, help="Số luồng song song (mặc định: 10)", default=10)
    parser.add_argument("--output", type=str, help="Tên file txt xuất (mặc định: hust_urls.txt)", default="hust_urls.txt")
    parser.add_argument("--visited-file", type=str, help="File lưu danh sách URL đã duyệt", default=None)
    parser.add_argument("--allow-all-hust", action="store_true", help="Cho phép nhảy sang mọi subdomain *.hust.edu.vn ngoài seeds")
    parser.add_argument("--fresh", action="store_true", help="Xóa dữ liệu cũ và crawl lại từ đầu")

    args = parser.parse_args()

    seeds = []
    if args.seeds:
        seeds.extend(args.seeds)
    elif args.seeds_file and os.path.exists(args.seeds_file):
        with open(args.seeds_file, "r", encoding="utf-8") as f:
            seeds = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    else:
        seeds = DEFAULT_SEEDS

    crawler = HustUnlimitedCrawler(
        seeds=seeds,
        output_file=args.output,
        visited_file=args.visited_file,
        max_workers=args.workers,
        max_depth=args.depth,
        allow_all_hust=args.allow_all_hust,
        fresh=args.fresh
    )
    crawler.run()


if __name__ == "__main__":
    main()
