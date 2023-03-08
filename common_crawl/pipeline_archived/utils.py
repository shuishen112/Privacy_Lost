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
import os
from warcio.archiveiterator import ArchiveIterator

tqdm.pandas()

# from pandarallel import pandarallel
# pandarallel.initialize(progress_bar=True)


def get_historical_url(type, url, year):
    """get historical url for specific url in specific year

    Args:
        type (_type_): domain type (edu or non-edu)
        url (_type_):  (url)
        year (_type_): which year we want to obtain
    """

    filename = f"resource/edu_repair/{type}_domain_historical_year_{year}_repair.csv"
    df = pd.read_csv(filename)

    if type == "edu":
        edu_url_dict = dict(
            zip(df["edu_domain"].to_list(), df["history_url"].to_list())
        )
        historical_url = edu_url_dict[url]
    elif type == "sample":
        control_url_dict = dict(
            zip(df["sample_domain"].to_list(), df["history_url"].to_list())
        )
        historical_url = control_url_dict[url]
    return historical_url


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
            "dataset_archive/edu_archive_repair/{}/{}.gz".format(year, url),
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


def extract_trackers_from_internet_archive(url, parser):
    try:
        resp = requests.get(url, headers={"Accept-Encoding": "identity"}, stream=True)
        text = resp.text
        trackers = parser(text, source="ia")
        trackers = list(set(trackers))
        return trackers

    except Exception as e:
        print(e)


def download_dataset_from_url(url: str):
    """download individual url and store zip file it in a path

    Args:
        url (str): url
    """
    history_url = url
    try:
        resp = requests.get(
            history_url, headers={"Accept-Encoding": "identity"}, stream=True
        )
        return resp.text

    except Exception as e:
        print(e)
        return None


def collect_dataset_from_url(url: str, path: str):
    """collect individual url and store zip file it in a path

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


def collect_dataset_from_ali(row, data_path, task_type=""):
    url = row["url"]
    years = list(range(2012, 2022))
    for year in years:
        historical_url = row[year]
        path = os.path.join(data_path, f"{task_type}_archive_ali/{year}")
        if not os.path.exists(path):
            os.makedirs(path)

        url_path = path + f"/{url}.gz"
        collect_dataset_from_url(historical_url, url_path)
