'''
Author: your name
Date: 2022-03-07 15:30:59
LastEditTime: 2022-03-30 10:33:56
LastEditors: Please set LastEditors
Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
FilePath: /common_crawl/waybackpy.py
'''
from email import header
from warnings import catch_warnings
import waybackpy
from waybackpy import WaybackMachineCDXServerAPI
from waybackpy import WaybackMachineAvailabilityAPI
import pandas as pd 
from tqdm import tqdm
import time
tqdm.pandas()

from warcio.capture_http import capture_http
from warcio import WARCWriter
from warc_test import get_text_selectolax,process_warc_from_archive
from urllib.parse import urlparse
import requests

from warcio.capture_http import capture_http
from warcio import WARCWriter
import requests  

# all the websites
df = pd.read_csv("resource/edu_repair/sample_domain_dmoz.csv")
# df = df.head(100)
# print(df['url'].head())


t = time.time()
# url = "https://web.archive.org/web/20211010070014/https://fujita-hu.ac.jp/"
# print('https://{}'.format(urlparse(url).path.split("//")[-1]))
# exit()
# find the url in specific time


def get_old_new_time(row):
    try:
        url = row['edu_domain']
        user_agent = "Mozilla/5.0 (Windows NT 5.1; rv:40.0) Gecko/20100101 Firefox/40.0"

        cdx_api = WaybackMachineCDXServerAPI(url, user_agent)
        oldest = cdx_api.oldest()
        oldest_time = oldest.timestamp
        newest = cdx_api.newest()
        newest_time = newest.timestamp
    except Exception as e:
        oldest_time = "null"
        newest_time = "null"
        print(e)
    return oldest_time,newest_time

def get_specific_time_url(row,url_year):
    url = row['edu_domain']
    
    user_agent = "Mozilla/5.0 (Windows NT 5.1; rv:40.0) Gecko/20100101 Firefox/40.0"
    try:

        cdx_api = WaybackMachineCDXServerAPI(url, user_agent)
        near = cdx_api.near(year=url_year, month=10, day=10, hour=10, minute=10)
        archive_url = near.archive_url

    except Exception as e:
        print(e)
        archive_url = "www.example.com"
    return archive_url

df[['old_edu','new_edu']] = df.progress_apply(get_old_new_time,axis = 1,result_type='expand')

df.to_csv("resource/edu_repair/edu_time_series.csv",index = None)
print("time:",time.time() - t)
exit()
# print(df[['old','new']].head())

# df['history_url'] = df.progress_apply(get_specific_time_url,axis = 1,url_year=2021)
# print(df['history_url'].head())

# df[['edu_domain','history_url']].to_csv("resource/edu_repair/edu_domain_historical.csv",index = None)


# 收集数据

exit()

def collect_dataset(row):
    url = row['url']
    history_url = row['history_url']
    try:

        with open('dataset_archive/{}.gz'.format(url), 'wb') as fh:
            warc_writer = WARCWriter(fh)
            with capture_http(warc_writer):
                requests.get(history_url)
    except Exception as e:
        print(e)
    

# df.apply(collect_dataset,axis = 1)



# 收集tracker

# process_warc_from_archive("dataset_archive/www.fujita-hu.ac.jp.gz",parser=get_text_selectolax)
