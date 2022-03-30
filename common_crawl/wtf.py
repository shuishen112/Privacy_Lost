'''
Author: your name
Date: 2022-03-08 11:27:28
LastEditTime: 2022-03-15 23:07:32
LastEditors: Please set LastEditors
Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
FilePath: /common_crawl/wtf.py
'''

from warcio.capture_http import capture_http
from warcio import WARCWriter
from warcio.statusandheaders import StatusAndHeaders
import requests  
import pandas as pd 
from tqdm import tqdm
import time
import numpy as np
from multiprocessing import  Pool
tqdm.pandas()

# from pandarallel import pandarallel
# pandarallel.initialize(progress_bar=True)

def collect_dataset(row):
    url = row['url']
    history_url = row['history_url']
    try:
        with open('dataset_archive/{}.gz'.format(url), 'wb') as output:
            writer = WARCWriter(output, gzip=True)

            resp = requests.get(history_url,
                                headers={'Accept-Encoding': 'identity'},
                                stream=True)

            # get raw headers from urllib3
            headers_list = resp.raw.headers.items()

            http_headers = StatusAndHeaders('200 OK', headers_list, protocol='HTTP/1.0')

            record = writer.create_warc_record(history_url, 'response',
                                                payload=resp.raw,
                                                http_headers=http_headers)

            writer.write_record(record)
    except Exception as e:
        print(e)
# t = time.time()


# df = pd.read_csv("dataset_archive/edu_history_2021.csv")

# df.progress_apply(collect_dataset,axis = 1)

# print(time.time() - t)
# exit()

# get the tracking information

t = time.time()
import glob
from warc_test import get_text_selectolax,process_warc_from_archive
files = glob.glob("dataset_archive/*.gz")

urls = []
trackesr = []
for file in files:
    result = process_warc_from_archive(file,parser=get_text_selectolax)
    if result:
        url,tracker_list = result
        urls.append(url)
        trackesr.append(tracker_list)
    else:
        print(file)

df_edu_history = pd.DataFrame({'url':urls,'3p-domain':trackesr})
df_edu_history.to_csv("dataset_archive/edu_trackers_202110.csv",index = None)
print(time.time() - t)

# process_warc_from_archive("dataset_archive/www.calhoun.org",parser=get_text_selectolax)
