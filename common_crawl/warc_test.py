'''
Author: Zhan
Date: 2021-03-12 14:59:58
LastEditTime: 2021-06-14 12:20:38
LastEditors: Please set LastEditors
Description: In User Settings Edit
FilePath: /undefined/Users/zhansu/Documents/phd/privacy_lost/second_week/warc_test.py
'''
from bs4 import BeautifulSoup
from urllib.parse import urlparse,urlsplit
# coding: utf-8

from time import time

import warc
from bs4 import BeautifulSoup
from selectolax.parser import HTMLParser
from urllib.parse import urlparse
import pandas as pd 

# 获得trackers

thirdparties = pd.read_csv("labeled-thirdparties.csv",sep = '\t',names = ['domain','registration_org','registration_country','num_embeddings','num_embeddings_javascript','num_embeddings_iframe','num_embeddings_image','num_embeddings_link','category','company'])

trackers = thirdparties['domain'].to_list()
print(trackers)

def get_text_bs(html):
    tree = BeautifulSoup(html, 'lxml')

    body = tree.body
    if body is None:
        return None

    for tag in body.select('style'):
        tag.decompose()

    text = body.get_text(separator='\n')
    a = body.find_all('a')
    for aa in a:
        url = aa.get('href')
        domain = urlparse(url).netloc
        if domain in trackers:
            print(domain)
        # print(aa.get('href'))
    s = body.find_all('script')
    for ss in s:
        url = ss.get('src')
        domain = urlparse(url).netloc
        if domain in trackers:
            print(domain)
        # print(ss.get('src'))
    return text


def get_text_selectolax(html):
    tree = HTMLParser(html)

    if tree.body is None:
        return None

    for tag in tree.css('style'):
        tag.decompose()

    text = tree.body.text(separator='\n')


    return text


def read_doc(record, parser=get_text_selectolax):
    url = record.url
    text = None


    if url:
        payload = record.payload.read()
        header, html = payload.split(b'\r\n\r\n', maxsplit=1)
        html = html.strip()

        if len(html) > 0:
            text = parser(html)


    return url, text


def process_warc(file_name, parser, limit=10000):
    warc_file = warc.open(file_name, 'rb')
    t0 = time()
    n_documents = 0
    for i, record in enumerate(warc_file):
        url, doc = read_doc(record, parser)
        if not doc or not url:
            continue

        n_documents += 1

        if i > limit:
            break

    warc_file.close()
    print('Parser: %s' % parser.__name__)
    print('Parsing took %s seconds and produced %s documents\n' % (time() - t0, n_documents))


# import re 

# with open('CC-MAIN-20210513173321-20210513203321-00163.warc',encoding='utf-8',errors='ignore') as fin:
#     for line in fin:

#         try:
#             result = re.findall('<a[^>]*href="([^>]*)">',line,re.I)
#             print(result)
#         except Exception as e:
#             print(e)

process_warc('CC-MAIN-20210518125638-20210518155638-00357.warc.gz',get_text_bs,100)