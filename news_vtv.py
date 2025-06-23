import csv
import re

import requests
import pandas as pd
import time
import random
import os
import json
from datetime import datetime

from bs4 import BeautifulSoup
from tqdm import tqdm
import logging
import sys

from DemoEachFunction import demoTakeLinkPerPage

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper_vtvgo.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)


class NewsScraperVTV:
    def __init__(self, output_dir="data_vtv"):
        self.output_dir = output_dir
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'vi,en-US;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Referer': 'https://vtv.vn/',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
        }
        self.session = requests.Session()
        try:
            # Tạo thư mục đầu ra nếu chưa tồn tại
            os.makedirs(output_dir, exist_ok=True)
            for subdir in ["raw", "processed"]:
                os.makedirs(os.path.join(output_dir, subdir), exist_ok=True)
        except Exception as e:
            logging.error(f"Lỗi khi tạo thư mục: {str(e)}")
            sys.exit(1)

    '''
    Truy cập từng chủ đề, mỗi chủ đề báo có nhiều trang nên ta phải truy lùng từng chủ đề và từng trang
    '''

    def scrape_vtv(self, num_pages):
        """Thu thập dữ liệu từ vtv"""
        logging.info("Bắt đầu thu thập dữ liệu từ VTV")
        article_links = []

        categories = [
            #"chinh-tri"
             "xa-hoi", "phap-luat", "the-gioi", "kinh-te", "the-thao", "truyen-hinh",
            "van-hoa-giai-tri", "doi-song", "cong-nghe", "giao-duc"
        ]

        try:
            response = self.session.get("https://vtv.vn", timeout=10, headers=self.headers)
            if response.status_code != 200:
                logging.error(f"Không thể kết nối đến VTV: Mã trạng thái {response.status_code}")
                return None
            logging.info("Kết nối thành công đến vtv.vn")
        except requests.exceptions.RequestException as e:
            logging.error(f"Không thể kết nối đến VTV: {str(e)}")
            return None

        for category in categories:
            try:
                url_to_try = f"https://vtv.vn/{category}.htm"
                logging.info(f"Đang thử truy cập: {url_to_try}")

                response = self.session.get(url_to_try, headers=self.headers, timeout=15)
                if response.status_code != 200:
                    logging.warning(f"Không thể truy cập trang {url_to_try}, mã trạng thái: {response.status_code}")
                    continue

                logging.info(f"Truy cập thành công: {url_to_try}")

                options = Options()
                options.add_argument("--headless")
                options.add_argument("--disable-gpu")
                options.add_argument("--window-size=1920,1080")

                driver = webdriver.Chrome(options=options)
                driver.get(url_to_try)
                time.sleep(15)

                last_height = driver.execute_script("return document.body.scrollHeight")

                for _ in range(10):
                    # Cuộn xuống cuối trang
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(8)

                    # Thử click vào nút "Xem thêm" nếu có
                    try:
                        xem_them = WebDriverWait(driver, 2).until(
                            EC.element_to_be_clickable((By.XPATH, "//a[contains(text(),'Xem thêm')]"))
                        )
                        driver.execute_script("arguments[0].scrollIntoView(true);", xem_them)
                        time.sleep(1)
                        driver.execute_script("arguments[0].click();", xem_them)
                        time.sleep(2)
                    except:
                        pass  # Không thấy hoặc không click được nút

                    new_height = driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        break
                    last_height = new_height

                # Lấy tất cả liên kết bài viết
                articles = driver.find_elements(By.XPATH, "//div[@class='list_news timeline']//a[@href]")
                links = [a.get_attribute('href') for a in articles if a.get_attribute('href').endswith('.htm')]
                driver.quit()

                links_not_http = list(set(links))

                if not links_not_http:
                    logging.warning(f"Không thu thập được liên kết nào từ chuyên mục {category}")
                    continue

                for href in links_not_http:
                    if not href.startswith('http'):
                        if href.startswith('/'):
                            href = f"https://vtv.vn{href}"
                        else:
                            href = f"https://vtv.vn/{href}"

                    if not any(item['url'] == href for item in article_links):
                        article_links.append({
                            'url': href,
                            'source': 'vtv',
                            'category': category
                        })

                logging.info(f"Đã thu thập {len(article_links)} liên kết từ vtv - chuyên mục {category}")
                time.sleep(random.uniform(2, 4))

            except Exception as e:
                logging.error(f"Lỗi khi thu thập liên kết từ chuyên mục {category}: {str(e)}")
                time.sleep(random.uniform(5, 10))

        # Loại bỏ liên kết trùng
        unique_links = []
        unique_urls = set()
        for link in article_links:
            if link['url'] not in unique_urls:
                unique_urls.add(link['url'])
                unique_links.append(link)

        logging.info(f"Tổng số liên kết duy nhất: {len(unique_links)}")

        self._claim_content(unique_links)

    def _save_links_to_csv(self, links, filename="article_links.csv"):
        """
        Lưu danh sách các liên kết vào file CSV.
        Args:
            links (list): Danh sách các liên kết (dict) để lưu.
            filename (str): Tên file CSV. Mặc định là "article_links.csv".
        """
        try:
            # Đảm bảo thư mục tồn tại
            os.makedirs(self.output_dir, exist_ok=True)
            file_path = os.path.join(self.output_dir, filename)
            file_exists = os.path.exists(file_path)  # Kiểm tra xem file có tồn tại không
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['url', 'source', 'category']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                if not file_exists:
                    writer.writeheader()  # Chỉ ghi header nếu file mới
                for link in links:
                    writer.writerow(link)
            logging.info(f"Đã lưu các liên kết vào file: {file_path}")
        except Exception as e:
            logging.error(f"Lỗi khi lưu vào file CSV: {str(e)}")

    def _claim_content(self, unique_links):
        ''', filename="article_links.csv"'''
        number_of_claim_article = 0
        # Thu thập nội dung từ các liên kết
        for article_info in tqdm(unique_links, desc="Thu thập bài viết vtv"):
            try:
                article_data = self._scrape_vtv_article(article_info['url'], article_info['category'])
                if article_data:
                    number_of_claim_article += 1
                    # Lưu dữ liệu thô
                    self._save_raw_article(article_data)
                    time.sleep(random.uniform(1, 3))
            except Exception as e:
                logging.error(f"Lỗi khi thu thập bài viết từ {article_info['url']}: {str(e)}")
        logging.info(f"Đã hoàn thành thu thập dữ liệu từ VTV: {number_of_claim_article} bài viết")

    def _scrape_vtv_article(self, url, category):
        """Thu thập thông tin từ một bài viết vtv"""
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
                # Cạo dữ liệu 1 link Web bài viết cụ thể
                response.encoding = 'utf-8'  # Đảm bảo encoding đúng
                soup = BeautifulSoup(response.text, 'html.parser')
                detail_summary = soup.find('div', class_='noidung').find('h2', class_='sapo').get_text().strip()
                detail_content = soup.find('div', class_='noidung').find('div', class_='ta-justify').get_text().strip()
                #print(detail_summary)
                #print(detail_content)

                return {
                    'category': category,
                    'url': url,
                    'source': 'vtv',
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
        fail = 0
        try:
            data = self.load_raw_data()
            processed_data = []
            for article in tqdm(data, desc="Tiền xử lý dữ liệu"):
                processed_article = self.clean_article(article)
                if processed_article:
                    fail += 1
                    processed_data.append(processed_article)

            # Lưu dữ liệu đã xử lý
            df = pd.DataFrame(processed_data)
            df.to_csv(os.path.join(self.output_dir, "processed", "vtv_article.csv"), index=False, encoding='utf-8')

            logging.info(f"Đã hoàn thành tiền xử lý dữ liệu: {len(processed_data)} bài viết")
            logging.info(f"Đã hỏng tiền xử lý dữ liệu: {fail} bài viết")
            return len(processed_data)
        except Exception as e:
            logging.error(f"Lỗi trong quá trình tiền xử lý dữ liệu: {str(e)}")
            return []

    def load_raw_data(self):
        """Đọc dữ liệu thô từ thư mục raw"""
        logging.info("Đọc dữ liệu thô từ thư mục")
        data = []
        raw_dir = os.path.join(self.output_dir, "raw")
        num=0
        for filename in os.listdir(raw_dir):
            if filename.endswith('.json'):
                try:
                    with open(os.path.join(raw_dir, filename), 'r', encoding='utf-8') as f:
                        num += 1
                        article_data = json.load(f)
                        data.append(article_data)
                except Exception as e:
                    logging.error(f"Lỗi khi đọc file {filename}: {str(e)}")
        logging.info(f"Đã đọc {len(data)} bài viết từ thư mục raw")
        logging.info(f"đọc {num} file json")
        return data

    def clean_article(self, article):
        """Làm sạch dữ liệu của một bài viết"""
        try:
            # Kiểm tra dữ liệu có đầy đủ không
            if not all(k in article for k in ['url', 'summary', 'content']):
                logging.info("thiếu url, summ, content")
                return None
            # Loại bỏ các ký tự HTML và ký tự đặc biệt
            summary = self.clean_text(article['summary'])
            content = self.clean_text(article['content'])

            # Kiểm tra dữ liệu sau khi làm sạch
            if not summary or not content or len(content.split()) < 50:
                return None
            # Nếu tóm tắt trống, tạo tóm tắt từ đoạn đầu của nội dung
            if not summary:
                content_words = content.split()
                summary = ' '.join(content_words[:min(50, len(content_words))])

            # Tạo bài viết đã được làm sạch
            cleaned_article = {
                'url': article['url'],
                'summary': summary,
                'content': content,
            }
            return cleaned_article
        except Exception as e:
            logging.error(f"Lỗi khi làm sạch bài viết: {str(e)}")
            return None
    def clean_text(self, text):
        """Làm sạch văn bản"""
        if not text:
            return ""
        # Thay \n đứng một mình hoặc có nhiều khoảng trắng thành dấu chấm (nếu chưa có dấu câu phía trước)
        text = re.sub(r'(?<=[^\.\?\!])\n+', '. ', text)  # Thêm dấu chấm nếu trước \n không có dấu câu
        # Nếu đã có dấu chấm trước \n thì chỉ thay \n bằng khoảng trắng
        text = re.sub(r'(?<=[\.\?\!])\n+', ' ', text)
        # Loại bỏ dấu nháy kép
        text = re.sub(r'[“”"]', '', text)

        # 1. Vẫn xoá các đoạn (Ảnh: ...), thường không cần thiết
        text = re.sub(r'\(Ảnh:.*?\)', '', text)
        # 2. Chỉ xoá phần "VTV.vn -" ở đầu, giữ lại nội dung phía sau
        text = re.sub(r'^VTV\.vn\s*-\s*', '', text)  # Dấu ^ đảm bảo chỉ xử lý nếu ở dòng đầu
        # 3. Cẩn trọng khi xoá các dòng kiểu quảng bá
        # Dùng \Z để chỉ tìm ở cuối
        text = re.sub(r'\*.*?VTVGo.*?\.\s*\Z', '', text, flags=re.MULTILINE)
        # 4. Xử lý "VTV. vn" thành "VTV.vn" thay vì xóa
        text = re.sub(r'VTV\.\s?vn', 'VTV.vn', text)

        # Xoá khoảng trắng dư thừa
        text = re.sub(r'\s+', ' ', text).strip()
        #return text
        return self.FitToShorter256Token(text)

    def FitToShorter256Token(self,text):
        sentences = [s.strip() for s in text.split('.') if s.strip()]
        text = ""
        result = []
        for sentence in sentences:
            if len(sentence.split()) <= 128:
                result.append(sentence)
            else:
                # Nếu câu dài hơn max_len, tách tiếp theo dấu phẩy
                parts = [p.strip() for p in sentence.split(',')]
                temp = ""
                for part in parts:
                    l = len(part.split())

                    if len(temp.split()) + l + 1 <= 128:
                        # cộng phần này vào temp, thêm dấu phẩy nếu temp ko rỗng
                        if temp != "":
                            temp += ', ' + part
                        else:
                            temp = part
                    else:
                        # temp đạt giới hạn, lưu lại và bắt đầu temp mới
                        if temp != "":
                            result.append(temp)
                        words = part.split()
                        for i in range(0, len(words), 128):
                            for j in range(i, min(i + 128, len(words))):
                                temp = temp + words[j] + ' '
                            result.append(temp)
                            temp = ""
                        temp = ""
                # Lưu phần cuối cùng
                if temp != "":
                    result.append(temp)
        for s in result:
            text = text + s + '. '
        return text

    def split_dataset(self):
        """Chia tập dữ liệu thành train/validation/test (70%/15%/15%)"""
        logging.info("Bắt đầu chia tập dữ liệu")

        try:
            # Đọc dữ liệu đã tách từ
            tokenized_file = os.path.join(self.output_dir, "processed", "tokenized_articles.csv")
            if os.path.exists(tokenized_file):
                df = pd.read_csv(tokenized_file, encoding='utf-8')
            else:
                processed_file = os.path.join(self.output_dir, "processed", "old_all_articles.csv")
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

    def run_scraper(self, target_count=2000, pages_per_source=100):
        """Chạy toàn bộ quá trình thu thập và xử lý dữ liệu"""
        start_time = time.time()
        logging.info(f"Bắt đầu quá trình thu thập dữ liệu với mục tiêu {target_count} bài viết")
        # Thu thập dữ liệu từ các nguồn tin tức
        self.scrape_vtv(num_pages=pages_per_source)
        end_time = time.time()
        logging.info(f"Đã hoàn thành toàn bộ quá trình trong {(end_time - start_time) / 60:.2f} phút")


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
        scraper = NewsScraperVTV()
        #scraper.run_scraper(target_count=5000, pages_per_source=200)
        scraper.preprocess_data()
    except KeyboardInterrupt:
        print("\nĐã dừng chương trình theo yêu cầu của người dùng")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Lỗi không xác định: {str(e)}")
        sys.exit(1)
