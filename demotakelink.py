import sys
import re
import requests
from bs4 import BeautifulSoup


def craw(response):
    #htmlpage = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    soup = soup.find(class_='zone zone--featured')
    # Lấy tất cả các thẻ <a> và trích xuất href
    links = [a['href'] for a in soup.find_all('a', href=True)]
    filtered_links = [link for link in links if link.endswith('.htm')]
    unique_links = list(set(filtered_links))
    return unique_links
if __name__ == '__main__':
    htmlpage = requests.get('https://vneconomy.vn/tai-chinh.htm?trang=3')
    soup = BeautifulSoup(htmlpage.text, 'html.parser')
    soup = soup.find(class_='zone zone--featured')

    # Lấy tất cả các thẻ <a> và trích xuất href
    links = [a['href'] for a in soup.find_all('a', href=True)]

    filtered_links = [link for link in links if link.endswith('.htm')]

    unique_links = list(set(filtered_links))

    #for link in unique_links:
    #    print(link)


