from news_scraper import NewsScraperVietnam
import logging
import os

def main():
    # Thiết lập logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("scraper.log", encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    # Kiểm tra xem đã có dữ liệu chưa
    output_dir = "data"
    if os.path.exists(os.path.join(output_dir, "dataset_stats.json")):
        logging.info("Dữ liệu đã tồn tại. Kiểm tra thống kê trong file dataset_stats.json")
        return
    
    try:
        # Khởi tạo scraper
        scraper = NewsScraperVietnam(output_dir=output_dir)
        
        # Chạy quá trình thu thập và xử lý dữ liệu
        # Giảm số lượng trang để test trước
        scraper.run_scraper(target_count=5000, pages_per_source=10)
        
        logging.info("Hoàn thành quá trình thu thập và xử lý dữ liệu!")
        
    except Exception as e:
        logging.error(f"Lỗi trong quá trình chạy scraper: {str(e)}")

if __name__ == "__main__":
    main() 