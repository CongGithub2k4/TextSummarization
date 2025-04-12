import datetime
import sys
import re
import requests
from bs4 import BeautifulSoup


def craw1web(response):
    response.encoding = 'utf-8'  # Đảm bảo encoding đúng
    soup = BeautifulSoup(response.text, 'html.parser')

    detail_summary = soup.find('h2', class_='detail__summary')
    detail_content = soup.find('div', class_='detail__content')
    print(detail_summary.text)
    print(detail_content.text)
    return [detail_summary.get_text(), detail_content.get_text()]

if __name__ == '__main__':
    craw1web(requests.get('https://vneconomy.vn/bo-ngoai-giao-quyet-dinh-ap-thue-quan-cua-my-voi-hang-hoa-viet-nam-chua-phu-hop-voi-thuc-te-hop-tac-giua-hai-nuoc.htm'))
