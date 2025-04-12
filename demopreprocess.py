import json
import logging
import os
import re
from datetime import time
from random import random

import pandas as pd
from tqdm import tqdm


def preprocess_data():
    """Tiền xử lý dữ liệu đã thu thập"""
    logging.info("Bắt đầu tiền xử lý dữ liệu")

    try:
        data = load_raw_data()
        processed_data = []
        for article in tqdm(data, desc="Tiền xử lý dữ liệu"):
            processed_article = clean_article(article)
            if processed_article:
                processed_data.append(processed_article)

        # Lưu dữ liệu đã xử lý
        df = pd.DataFrame(processed_data)
        df.to_csv(os.path.join("data", "processed", "all_articles.csv"), index=False, encoding='utf-8')

        logging.info(f"Đã hoàn thành tiền xử lý dữ liệu: {len(processed_data)} bài viết")
        return len(processed_data)
    except Exception as e:
        logging.error(f"Lỗi trong quá trình tiền xử lý dữ liệu: {str(e)}")
        return []


def load_raw_data():
    """Đọc dữ liệu thô từ thư mục raw"""
    logging.info("Đọc dữ liệu thô từ thư mục")
    data = []
    raw_dir = os.path.join("data", "raw")

    for filename in os.listdir(raw_dir):
        if filename.endswith('.json'):
            try:
                with open(os.path.join(raw_dir, filename), 'r', encoding='utf-8') as f:
                    article_data = json.load(f)
                    data.append(article_data)
            except Exception as e:
                logging.error(f"Lỗi khi đọc file {filename}: {str(e)}")
    logging.info(f"Đã đọc {len(data)} bài viết từ thư mục raw")
    return data

def clean_article(article):
    """Làm sạch dữ liệu của một bài viết"""
    try:
        # Kiểm tra dữ liệu có đầy đủ không
        if not all(k in article for k in ['url', 'summary', 'content']):
            return None
        # Loại bỏ các ký tự HTML và ký tự đặc biệt
        summary = clean_text(article['summary'])
        content = clean_text(article['content'])

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


def clean_text(text):
    """Làm sạch văn bản"""
    if not text:
        return ""

    # Thay \n đứng một mình hoặc có nhiều khoảng trắng thành dấu chấm (nếu chưa có dấu câu phía trước)
    text = re.sub(r'(?<=[^\.\?\!])\n+', '. ', text)  # Thêm dấu chấm nếu trước \n không có dấu câu
    # Nếu đã có dấu chấm trước \n thì chỉ thay \n bằng khoảng trắng
    text = re.sub(r'(?<=[\.\?\!])\n+', ' ', text)
    # Xoá khoảng trắng dư thừa
    text = re.sub(r'\s+', ' ', text).strip()
    return text


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
    """Chia tập dữ liệu thành train/validation/test (80%/10%/10%)"""
    logging.info("Bắt đầu chia tập dữ liệu")

    try:
        # Đọc dữ liệu đã tách từ
        tokenized_file = os.path.join("data", "processed", "tokenized_articles.csv")
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
        train_df.to_csv(os.path.join("data", "train", "train.csv"), index=False, encoding='utf-8')
        val_df.to_csv(os.path.join("data", "validation", "validation.csv"), index=False, encoding='utf-8')
        test_df.to_csv(os.path.join("data", "test", "test.csv"), index=False, encoding='utf-8')

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


if __name__ == '__main__':
    #preprocess_data()
    '''with open('data/raw/vneconomy_1744001772_dau-tu.json', 'r', encoding='utf-8') as f:
        article_data = json.load(f)
        print(article_data['content'])
        cleaned_data = clean_article(article_data)
        print(cleaned_data['url'])
        print(cleaned_data['summary'])
        print(cleaned_data['content'])'''
    df = pd.read_csv('data/processed/all_articles.csv')
    print(len(df))
