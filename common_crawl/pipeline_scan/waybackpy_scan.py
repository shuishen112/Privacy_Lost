"""
Author: your name
Date: 2022-03-07 15:30:59
LastEditTime: 2022-03-30 10:33:56
LastEditors: Please set LastEditors
Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
FilePath: /common_crawl/waybackpy.py
"""
from email import header
import waybackpy
from waybackpy import WaybackMachineCDXServerAPI
from waybackpy import WaybackMachineAvailabilityAPI
import pandas as pd
from tqdm import tqdm
import time

tqdm.pandas()

from warcio.capture_http import capture_http
from warcio import WARCWriter

# from warc_test import get_text_selectolax,process_warc_from_archive
from urllib.parse import urlparse
import requests

# from pandarallel import pandarallel
# pandarallel.initialize(progress_bar=True,nb_workers = 5)


# all the websites
df = pd.read_csv("resource/edu_repair/sample_all_domain_dmoz.csv")

df = df.sort_values(by="rank")
df = df.head(20000)

df.to_csv("resource/edu_repair/have_scaned_websites.csv", index=None)

# print(df['edu_domain'].head())


# find the url in specific time

collect_key = "edu_domain"


def get_old_new_time(row):
    """get the oldest and newest time of urls

    Args:
        row (df): pandas df

    Returns:
        _type_: oldest time and newest time
    """
    try:
        url = row[collect_key]
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
    return oldest_time, newest_time


def get_specific_time_url(row, url_year):
    """get specific time

    Args:
        row (df): df row
        url_year (str): year

    Returns:
        str: get the historical time
    """
    url = row[collect_key]

    user_agent = "Mozilla/5.0 (Windows NT 5.1; rv:40.0) Gecko/20100101 Firefox/40.0"
    try:

        cdx_api = WaybackMachineCDXServerAPI(
            url, user_agent, start_timestamp=url_year, end_timestamp=url_year
        )
        near = cdx_api.near(year=url_year)
        archive_url = near.archive_url

    except Exception as e:
        print(e)
        archive_url = None
    return archive_url


def get_specific_time_url(url, year):
    """get historical url

    Args:
        url (str): url
        year (int): year
    """
    user_agent = "Mozilla/5.0 (Windows NT 5.1; rv:40.0) Gecko/20100101 Firefox/40.0"

    cdx_api = WaybackMachineCDXServerAPI(
        url, user_agent, start_timestamp=year, end_timestamp=year
    )
    near = cdx_api.near(year=year)
    archive_url = near.archive_url

    return archive_url


def judge_whether_it_can_occur_in_each_year(url, begin_year, end_year):

    user_agent = "Mozilla/5.0 (Windows NT 5.1; rv:40.0) Gecko/20100101 Firefox/40.0"

    for year in range(begin_year, end_year):
        print(year)
        cdx_api = WaybackMachineCDXServerAPI(
            url, user_agent, start_timestamp=year, end_timestamp=year
        )
        near = cdx_api.near(year=year)
        archive_url = near.archive_url
        print(archive_url)


start = time.time()
judge_whether_it_can_occur_in_each_year("baidu.com", 2012, 2022)
print(time.time() - start)

# df[['old_edu','new_edu']] = df.iprogress_apply(get_old_new_time,axis = 1,result_type='expand')

# df.to_csv("resource/edu_repair/edu_time_series_3000_later.csv",index = None)

# for year in range(2014, 2022):
#     t = time.time()
#     df["history_url"] = df.progress_apply(get_specific_time_url, axis=1, url_year=year)
#     print("time:", time.time() - t)
#     df[[collect_key, "history_url"]].to_csv(
#         "resource/edu_repair/{}_historical_year_{}.csv".format(collect_key, str(year)),
#         index=None,
#     )
# print(get_specific_time_url("google.com", 2012))
