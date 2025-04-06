import sys
import re
import requests
from bs4 import BeautifulSoup

if __name__ == '__main__':
    htmlpage = requests.get(
        'https://vneconomy.vn/dieu-chinh-thue-tieu-thu-dac-biet-voi-bia-ruou-can-xem-xet-than-trong-toan-dien.htm')
    data = BeautifulSoup(htmlpage.text, 'html.parser')


    detail_summary = data.find('h2', class_='detail__summary')
    detail_content = data.find('div', class_='detail__content')

    print(detail_content.getText())
    #print(detail_summary.getText())

    lst = re.split(r"(\s)*\.(\n)*",detail_content.get_text())
    print(*lst,sep='\n')

