"""
Author: your name
Date: 2022-03-08 11:27:28
LastEditTime: 2022-03-15 23:07:32
LastEditors: Please set LastEditors
Description: collect dataset from Archive
FilePath: /common_crawl/wtf.py
"""
from warcio import WARCWriter
from warcio.statusandheaders import StatusAndHeaders
import requests
import pandas as pd
from tqdm import tqdm
import time
import glob
import os

tqdm.pandas()

# from pandarallel import pandarallel
# pandarallel.initialize(progress_bar=True)


def collect_dataset(row, year):
    """
    This is collecting process over the year

    Args:
        row (df row): df row used by df.apply()
        year (str): specific year we need to collect dataset.
    """
    url = row["sample_domain"]
    history_url = row["history_url"]
    try:
        with open(
            "dataset_archive/control_archive_repair/{}/{}.gz".format(year, url),
            "wb",
        ) as output:
            writer = WARCWriter(output, gzip=True)

            resp = requests.get(
                history_url, headers={"Accept-Encoding": "identity"}, stream=True
            )

            # get raw headers from urllib3
            headers_list = resp.raw.headers.items()

            http_headers = StatusAndHeaders("200 OK", headers_list, protocol="HTTP/1.0")

            record = writer.create_warc_record(
                history_url, "response", payload=resp.raw, http_headers=http_headers
            )

            writer.write_record(record)
    except Exception as e:
        print(e)


def collect_dataset_from_url(url, path):
    """collect individual url and store it in a path

    Args:
        url (str): url
        path (str): path
    """
    history_url = url
    try:
        with open(path, "wb") as output:
            writer = WARCWriter(output, gzip=True)

            resp = requests.get(
                history_url, headers={"Accept-Encoding": "identity"}, stream=True
            )

            # get raw headers from urllib3
            headers_list = resp.raw.headers.items()

            http_headers = StatusAndHeaders("200 OK", headers_list, protocol="HTTP/1.0")

            record = writer.create_warc_record(
                history_url, "response", payload=resp.raw, http_headers=http_headers
            )

            writer.write_record(record)
    except Exception as e:
        print(e)


##################### collecting archived dataset ##########
t = time.time()

df_final = pd.read_csv("resource/edu_repair/df_final.csv")
files = sorted(
    glob.glob("resource/edu_repair/sample_domain_historical_year_*_repair.csv")
)

for file in files:
    df = pd.read_csv(file)
    year = file.split("_")[-2]
    if not os.path.exists("dataset_archive/control_archive_repair/{}".format(year)):
        os.makedirs("dataset_archive/control_archive_repair/{}".format(year))
    print(year)
    df_collect = df[df["sample_domain"].isin(df_final["control_domain"])].head(3000)

    df_collect.progress_apply(collect_dataset, axis=1, year=year)

print(time.time() - t)

##################### collecting archived dataset ##########


################################## get the tracker information

'''
t = time.time()

from warc_utils import get_text_selectolax, process_warc_from_archive


def collect_trackers(type, year):
    """collect trackers from specific year

    Args:
        type (_type_): edu or non-edu
        year (_type_): specific year
    """

    files = glob.glob(f"dataset_archive/{type}/{year}/*.gz")

    urls = []
    trackesr = []
    for file in tqdm(files):
        result = process_warc_from_archive(file, parser=get_text_selectolax)
        if result:
            url, tracker_list = result
            urls.append(url)
            trackesr.append(tracker_list)
        else:
            # if result is None, it means this file can not be archved by the waybackpy
            url = file.split("/")[-1][:-3]
            tracker_list = None
            urls.append(url)
            trackesr.append(tracker_list)
            print("empty file", file)

    df_edu_history = pd.DataFrame({"url": urls, "3p-domain": trackesr})
    df_edu_history.to_csv(f"dataset_archive/{type}_{year}.csv", index=None)
    print(time.time() - t)


# years = list(range(2012, 2022))
# for year in years:
#     collect_trackers("edu_archive",year)

# process_warc_from_archive("dataset_archive/www.calhoun.org",parser=get_text_selectolax)

'''
