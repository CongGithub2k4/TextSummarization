import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import os
import json
from datetime import datetime
from tqdm import tqdm
import logging
import re
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import sys

import democraw_1_web
import demotakelink

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)


class NewsScraperVietnam:
    def __init__(self, output_dir="data"):
        self.output_dir = output_dir
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'vi,en-US;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Referer': 'https://vietnamnet.vn/',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
        }
        self.data = []
        self.session = requests.Session()

        try:
            # Tạo thư mục đầu ra nếu chưa tồn tại
            os.makedirs(output_dir, exist_ok=True)
            for subdir in ["raw", "processed", "train", "validation", "test"]:
                os.makedirs(os.path.join(output_dir, subdir), exist_ok=True)
        except Exception as e:
            logging.error(f"Lỗi khi tạo thư mục: {str(e)}")
            sys.exit(1)

    '''
    Truy cập từng chủ đề, mỗi chủ đề báo có nhiều trang nên ta phải truy lùng twufng chủ đề và từng trang
    '''

    def scrape_vietnamnet(self, num_pages):
        """Thu thập dữ liệu từ Vietnamnet"""
        logging.info("Bắt đầu thu thập dữ liệu từ Vietnamnet")
        article_links = []

        # Cập nhật danh sách chuyên mục theo cấu trúc mới của vietnamnet.vn
        # categories = ["thoi-su", "chinh-tri", "giao-thong", "moi-truong","kinh-doanh", "tai-chinh", "bat-dong-san", "doanh-nghiep","giai-tri", "sao", "phim", "nhac","the-gioi", "phan-tich", "quan-su", "tu-lieu","doi-song", "du-lich", "am-thuc", "cong-dong","giao-duc", "tuyen-sinh", "du-hoc", "guong-mat","suc-khoe", "lam-dep", "dinh-duong","ban-doc", "phap-luat", "the-thao", "cong-nghe"]
        categories = [
            "tieu-diem", "dau-tu", "tai-chinh", "kinh-te-so", "kinh-te-xanh",
            "thi-truong", "nhip-cau-doanh-nghiep", "dia-oc", "kinh-te-the-gioi", "dan-sinh"
        ]

        try:
            # Kiểm tra kết nối internet
            response = self.session.get("https://vneconomy.vn", timeout=10, headers=self.headers)
            if response.status_code != 200:
                logging.error(f"Không thể kết nối đến Vneconomy: Mã trạng thái {response.status_code}")
                return
            logging.info("Kết nối thành công đến vneconomy.vn")
        except requests.exceptions.RequestException as e:
            logging.error(f"Không thể kết nối đến Vneconomy: {str(e)}")
            return

        for category in categories:
            for page in range(3, num_pages + 1):
                try:
                    # Thử nhiều cấu trúc URL khác nhau
                    urls_to_try = [
                        # f"https://vietnamnet.vn/{category}-page{page}",  # Cấu trúc cũ
                        # f"https://vietnamnet.vn/{category}/trang{page}",  # Cấu trúc mới
                        # f"https://vietnamnet.vn/tin-tuc/{category}/trang{page}",  # Thêm tin-tuc
                        f"https://vneconomy.vn/{category}?trang={page}"  # Cấu trúc query param
                    ]

                    success = False
                    for url in urls_to_try:
                        logging.info(f"Đang thử truy cập: {url}")
                        response = self.session.get(url, headers=self.headers, timeout=15)

                        if response.status_code == 200:
                            logging.info(f"Truy cập thành công: {url}")
                            success = True
                            #hàm cạo web phần zone--featured và lấy link các bài vit trong trang đó
                            links_not_http = demotakelink.craw(response)
                            for href in links_not_http:
                                # Đảm bảo URL đầy đủ
                                if not href.startswith('http'):
                                    if href.startswith('/'):
                                        href = f"https://vneconomy.vn{href}"
                                    else:
                                        href = f"https://vneconomy.vn/{href}"

                                # Kiểm tra trùng lặp
                                if not any(item['url'] == href for item in article_links):
                                    article_links.append({
                                        'url': href,
                                        'source': 'vneconomy',
                                        'category': category
                                    })

                            logging.info(
                                f"Đã thu thập {len(article_links)} liên kết từ Vneconomy - chuyên mục {category} - trang {page}")
                            break  # Thoát khỏi vòng lặp urls_to_try nếu thành công
                        else:
                            logging.warning(f"Không thể truy cập trang {url}, mã trạng thái: {response.status_code}")

                    if not success:
                        logging.error(f"Không thể truy cập trang nào cho chuyên mục {category} trang {page}")

                    time.sleep(random.uniform(2, 4))  # Tăng thời gian chờ để tránh bị block
                except Exception as e:
                    logging.error(f"Lỗi khi thu thập liên kết từ chuyên mục {category} trang {page}: {str(e)}")
                    time.sleep(random.uniform(5, 10))

        # Loại bỏ các liên kết trùng lặp từ acticle_links: vì nó cạo các link bài từ hàng nghìn trang
        unique_links = []
        unique_urls = set()
        for link in article_links:
            if link['url'] not in unique_urls:
                unique_urls.add(link['url'])
                unique_links.append(link)

        logging.info(f"Tổng số liên kết duy nhất: {len(unique_links)}")

        # Thu thập nội dung từ các liên kết
        for article_info in tqdm(unique_links, desc="Thu thập bài viết vneconomy"):
            try:
                article_data = self._scrape_vietnamnet_article(article_info['url'], article_info['category'])
                if article_data:
                    #self.data.append(article_data)
                    # Lưu dữ liệu thô
                    self._save_raw_article(article_data)
                    time.sleep(random.uniform(1, 3))
            except Exception as e:
                logging.error(f"Lỗi khi thu thập bài viết từ {article_info['url']}: {str(e)}")

        logging.info(f"Đã hoàn thành thu thập dữ liệu từ Vneconomy: {len(self.data)} bài viết")

    def _scrape_vietnamnet_article(self, url, category):
        """Thu thập thông tin từ một bài viết Vietnamnet"""
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                response = self.session.get(url, headers=self.headers, timeout=15)
                if response.status_code != 200:
                    logging.warning(f"Không thể truy cập {url}, mã trạng thái: {response.status_code}")
                    retry_count += 1
                    time.sleep(random.uniform(2, 5))
                    continue

                [detail_summary, detail_content] = democraw_1_web.craw1web(response)
                return {
                    'category': category,
                    'url': url,
                    'source': 'vneconomy',
                    'scraped_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'summary': detail_summary,
                    'content': detail_content
                }
            except requests.exceptions.RequestException as e:
                retry_count += 1
                logging.warning(f"Lỗi kết nối khi thu thập bài viết (lần {retry_count}/{max_retries}): {url}")
                if retry_count == max_retries:
                    logging.error(f"Lỗi khi thu thập bài viết sau {max_retries} lần thử: {url}")
                    return None
                time.sleep(random.uniform(3, 6))
            except Exception as e:
                logging.error(f"Lỗi không xác định khi xử lý bài viết {url}: {str(e)}")
                return None

        return None

    def _save_raw_article(self, article_data):
        """Lưu dữ liệu thô của một bài viết"""
        try:
            filename = f"{article_data['source']}_{int(time.time())}_{article_data['category']}.json"
            with open(os.path.join(self.output_dir, "raw", filename), 'w', encoding='utf-8') as f:
                json.dump(article_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error(f"Lỗi khi lưu dữ liệu thô: {str(e)}")

    def preprocess_data(self):
        """Tiền xử lý dữ liệu đã thu thập"""
        logging.info("Bắt đầu tiền xử lý dữ liệu")

        try:
            # Nếu không có dữ liệu, đọc từ thư mục raw
            if not self.data:
                self._load_raw_data()

            processed_data = []

            for article in tqdm(self.data, desc="Tiền xử lý dữ liệu"):
                processed_article = self._clean_article(article)
                if processed_article:
                    processed_data.append(processed_article)

            # Lưu dữ liệu đã xử lý
            df = pd.DataFrame(processed_data)
            df.to_csv(os.path.join(self.output_dir, "processed", "all_articles.csv"), index=False, encoding='utf-8')

            logging.info(f"Đã hoàn thành tiền xử lý dữ liệu: {len(processed_data)} bài viết")
            return processed_data
        except Exception as e:
            logging.error(f"Lỗi trong quá trình tiền xử lý dữ liệu: {str(e)}")
            return []

    def _load_raw_data(self):
        """Đọc dữ liệu thô từ thư mục raw"""
        logging.info("Đọc dữ liệu thô từ thư mục")
        self.data = []
        raw_dir = os.path.join(self.output_dir, "raw")

        for filename in os.listdir(raw_dir):
            if filename.endswith('.json'):
                try:
                    with open(os.path.join(raw_dir, filename), 'r', encoding='utf-8') as f:
                        article_data = json.load(f)
                        self.data.append(article_data)
                except Exception as e:
                    logging.error(f"Lỗi khi đọc file {filename}: {str(e)}")

        logging.info(f"Đã đọc {len(self.data)} bài viết từ thư mục raw")

    def _clean_article(self, article):
        """Làm sạch dữ liệu của một bài viết"""
        try:
            # Kiểm tra dữ liệu có đầy đủ không
            if not all(k in article for k in ['title', 'summary', 'content']):
                return None

            # Loại bỏ các ký tự HTML và ký tự đặc biệt
            title = self._clean_text(article['title'])
            summary = self._clean_text(article['summary'])
            content = self._clean_text(article['content'])

            # Kiểm tra dữ liệu sau khi làm sạch
            if not title or not content or len(content.split()) < 50:
                return None

            # Nếu tóm tắt trống, tạo tóm tắt từ đoạn đầu của nội dung
            if not summary:
                content_words = content.split()
                summary = ' '.join(content_words[:min(50, len(content_words))])

            # Tạo bài viết đã được làm sạch
            cleaned_article = {
                'id': f"{article['source']}_{int(time.time())}_{random.randint(10000, 99999)}",
                'title': title,
                'summary': summary,
                'content': content,
                'pub_date': article.get('pub_date', ''),
                'category': article.get('category', ''),
                'url': article.get('url', ''),
                'source': article.get('source', '')
            }

            return cleaned_article
        except Exception as e:
            logging.error(f"Lỗi khi làm sạch bài viết: {str(e)}")
            return None

    def _clean_text(self, text):
        """Làm sạch văn bản"""
        if not text:
            return ""

        # Loại bỏ thẻ HTML
        text = re.sub(r'<.*?>', '', text)

        # Loại bỏ ký tự đặc biệt và dư thừa
        text = re.sub(r'\s+', ' ', text)  # Thay thế nhiều khoảng trắng bằng một khoảng trắng
        text = re.sub(r'[^\w\s\.,;:!?""()-]', '', text)  # Giữ lại dấu câu cơ bản

        return text.strip()

    def tokenize_vietnamese(self):
        """Tách từ tiếng Việt bằng underthesea"""
        logging.info("Bắt đầu tách từ tiếng Việt")

        try:
            # Kiểm tra và cài đặt underthesea nếu chưa có
            try:
                from underthesea import word_tokenize
            except ImportError:
                logging.info("Cài đặt underthesea...")
                os.system("pip install underthesea")
                from underthesea import word_tokenize

            # Đọc dữ liệu đã xử lý
            processed_file = os.path.join(self.output_dir, "processed", "all_articles.csv")
            if not os.path.exists(processed_file):
                logging.error("Không tìm thấy file dữ liệu đã xử lý")
                return

            df = pd.read_csv(processed_file, encoding='utf-8')

            # Tách từ
            logging.info("Tách từ cho tiêu đề...")
            df['tokenized_title'] = df['title'].apply(
                lambda x: ' '.join(word_tokenize(x)) if isinstance(x, str) else '')

            logging.info("Tách từ cho tóm tắt...")
            df['tokenized_summary'] = df['summary'].apply(
                lambda x: ' '.join(word_tokenize(x)) if isinstance(x, str) else '')

            logging.info("Tách từ cho nội dung...")
            df['tokenized_content'] = df['content'].apply(
                lambda x: ' '.join(word_tokenize(x)) if isinstance(x, str) else '')

            # Lưu dữ liệu đã tách từ
            df.to_csv(os.path.join(self.output_dir, "processed", "tokenized_articles.csv"), index=False,
                      encoding='utf-8')

            logging.info("Đã hoàn thành tách từ tiếng Việt")
        except Exception as e:
            logging.error(f"Lỗi trong quá trình tách từ tiếng Việt: {str(e)}")

    def split_dataset(self):
        """Chia tập dữ liệu thành train/validation/test (70%/15%/15%)"""
        logging.info("Bắt đầu chia tập dữ liệu")

        try:
            # Đọc dữ liệu đã tách từ
            tokenized_file = os.path.join(self.output_dir, "processed", "tokenized_articles.csv")
            if os.path.exists(tokenized_file):
                df = pd.read_csv(tokenized_file, encoding='utf-8')
            else:
                processed_file = os.path.join(self.output_dir, "processed", "all_articles.csv")
                if not os.path.exists(processed_file):
                    logging.error("Không tìm thấy file dữ liệu đã xử lý")
                    return
                df = pd.read_csv(processed_file, encoding='utf-8')

            # Xáo trộn dữ liệu
            df = df.sample(frac=1, random_state=42).reset_index(drop=True)

            # Chia tập dữ liệu
            n = len(df)
            train_idx = int(0.7 * n)
            val_idx = int(0.85 * n)

            train_df = df[:train_idx]
            val_df = df[train_idx:val_idx]
            test_df = df[val_idx:]

            # Lưu các tập dữ liệu
            train_df.to_csv(os.path.join(self.output_dir, "train", "train.csv"), index=False, encoding='utf-8')
            val_df.to_csv(os.path.join(self.output_dir, "validation", "validation.csv"), index=False, encoding='utf-8')
            test_df.to_csv(os.path.join(self.output_dir, "test", "test.csv"), index=False, encoding='utf-8')

            # Ghi thông tin về kích thước các tập dữ liệu
            logging.info(f"Kích thước tập huấn luyện: {len(train_df)} bài viết")
            logging.info(f"Kích thước tập kiểm định: {len(val_df)} bài viết")
            logging.info(f"Kích thước tập kiểm tra: {len(test_df)} bài viết")

            # Lưu thống kê
            stats = {
                'total': n,
                'train': len(train_df),
                'validation': len(val_df),
                'test': len(test_df),
                'sources': df['source'].value_counts().to_dict(),
                'categories': df['category'].value_counts().to_dict()
            }

            with open(os.path.join(self.output_dir, "dataset_stats.json"), 'w', encoding='utf-8') as f:
                json.dump(stats, f, ensure_ascii=False, indent=2)

            logging.info("Đã hoàn thành chia tập dữ liệu")
        except Exception as e:
            logging.error(f"Lỗi trong quá trình chia tập dữ liệu: {str(e)}")

    def run_scraper(self, target_count=5000, pages_per_source=200):
        """Chạy toàn bộ quá trình thu thập và xử lý dữ liệu"""
        start_time = time.time()
        logging.info(f"Bắt đầu quá trình thu thập dữ liệu với mục tiêu {target_count} bài viết")

        # Thu thập dữ liệu từ các nguồn tin tức
        self.scrape_vietnamnet(num_pages=pages_per_source)

        '''
        # Tiền xử lý dữ liệu
        self.preprocess_data()

        # Tách từ tiếng Việt
        self.tokenize_vietnamese()

        # Chia tập dữ liệu
        self.split_dataset()
        '''
        end_time = time.time()
        logging.info(f"Đã hoàn thành toàn bộ quá trình trong {(end_time - start_time) / 60:.2f} phút")
        logging.info(f"Tổng số bài viết đã thu thập: {len(self.data)}")


# Khởi chạy scraper
if __name__ == "__main__":
    try:
        '''
        # Kiểm tra Python version
        if sys.version_info[0] < 3:
            raise Exception("Yêu cầu Python 3 trở lên")

        # Kiểm tra thư viện
        required_packages = ['requests', 'beautifulsoup4', 'pandas', 'tqdm', 'underthesea']
        missing_packages = []
        for package in required_packages:
            try:
                __import__(package)
            except ImportError:
                missing_packages.append(package)

        if missing_packages:
            print(f"Vui lòng cài đặt các thư viện sau: {', '.join(missing_packages)}")
            print("Sử dụng lệnh: pip install -r requirements.txt")
            sys.exit(1)
        '''
        scraper = NewsScraperVietnam()
        scraper.run_scraper(target_count=5000, pages_per_source=200)

    except KeyboardInterrupt:
        print("\nĐã dừng chương trình theo yêu cầu của người dùng")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Lỗi không xác định: {str(e)}")
        sys.exit(1)
