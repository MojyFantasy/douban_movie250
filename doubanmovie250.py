# coding: utf-8

"""
    采用requests模块与re模块爬取豆瓣电影TOP250的内容
"""
import traceback
from multiprocessing.pool import Pool
from urllib.parse import urlencode

import pymongo
import requests
import re

from config import MONGO_URL, MONGO_DB, MONGO_TABLE

client = pymongo.MongoClient(MONGO_URL)
db = client[MONGO_DB]


def get_page_content(url, offset):
    data = {
        'start': offset,
    }
    url = url + '/?' + urlencode(data)

    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.text
    except:
        return None


def parse_page_content(content):
    pattern = '<div class="pic">.*?<em class="">(?P<top_num>\d+).*?<img.*?alt="(?P<pic_name>.*?)".*?src="(?P<pic_src>.' \
              '*?)".*?>.*?<div class="info">.*?<div class="hd">.*?<a href="(?P<detail_src>.*?)".*?>.*?<span class' \
              '="title">(?P<movie_name>.*?)</span>.*?' \
              '.*?<div class="star">.*?<span class="rating_num".*?>(?P<score>' \
              '.*?)</span>.*?<span>(?P<comment_num>.*?)</span>.*?<p class="quote">.*?<span class="inq">(?P<quote>.*?)' \
              '</span>'
    pattern = re.compile(pattern, re.S)
    res = re.findall(pattern, content)
    for item in res:
        yield {
            'index': item[0],
            'pic_name': item[1],
            'pic_link': item[2],
            'movie_detail_link': item[3],
            'movie_name': item[4],
            # 'movie_name2': item[5][13:],
            # 'movie_other_name': item[6][13:],
            'score': item[5],
            'comment_num': item[6][:-2],
            'movie_quote': item[7]
        }


def download_picture(url, pic_name):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            with open(r'pictures/{}.jpg'.format(pic_name), 'wb') as fp:
                fp.write(response.content)
            print("图片下载成功 ", pic_name)
    except:
        print(traceback.format_exc())
        print("图片下载失败！", pic_name, url)


def save_to_db(result):
    if db[MONGO_TABLE].insert(result):
        print("存储数据到MONGODB成功！", result)
        return True
    return False


def main(offset):
    url = "https://movie.douban.com/top250"
    content = get_page_content(url, offset)
    # print(content)
    # for item in parse_page_content(content):
    #     print(item)
    for item in parse_page_content(content):
        if save_to_db(item):
            download_picture(item['pic_link'], item['pic_name'])


if __name__ == "__main__":
    pool = Pool()
    pool.map(main, [x for x in range(0, 250, 25)])
    # main(0)
    # offset = 0
    # while offset < 250:
    #     main(offset)
    #     offset += 25
