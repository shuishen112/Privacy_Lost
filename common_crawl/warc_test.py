'''
Author: Zhan
Date: 2021-03-12 14:59:58
LastEditTime: 2021-03-12 16:35:37
LastEditors: Please set LastEditors
Description: In User Settings Edit
FilePath: /undefined/Users/zhansu/Documents/phd/privacy_lost/second_week/warc_test.py
'''
import warc 
from urllib.parse import urlparse,urlsplit
with warc.open("example.warc") as f:
    for record in f:
        url = record['WARC-Target-URI']
        domain = urlsplit(url)[1]
        print(urlsplit(url))
        print(domain)
