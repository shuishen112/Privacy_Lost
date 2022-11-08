"""
Author: Zhan
Date: 2021-03-12 14:59:58
LastEditTime: 2022-03-27 21:39:54
LastEditors: Please set LastEditors
Description: warc utils. We can use warc feature
FilePath: /undefined/Users/zhansu/Documents/phd/privacy_lost/second_week/warc_test.py
"""
# coding: utf-8

import re
from selectolax.parser import HTMLParser
from urllib.parse import urlparse
import pandas as pd
import boto3

from warcio.archiveiterator import ArchiveIterator
import tldextract
import logging
import validators
from validators import ValidationFailure
from config import args

logger = logging.getLogger("webtracking.warc_tracking")

# 获得trackers
tracker_type = args["tracker_type"]
print(tracker_type)
thirdparties = pd.read_csv(
    "resource/labeled-thirdparties.csv",
    sep="\t",
    names=[
        "domain",
        "registration_org",
        "registration_country",
        "num_embeddings",
        "num_embeddings_javascript",
        "num_embeddings_iframe",
        "num_embeddings_image",
        "num_embeddings_link",
        "category",
        "company",
    ],
)

whotracksme = pd.read_csv("resource/whotracksme_trackers.txt", names=["domain"])
intersection = pd.read_csv("resource/CommonListOfTrackers.txt", names=["domain"])

if tracker_type == "who":
    thirdparties = whotracksme
elif tracker_type == "inter":
    thirdparties = intersection
regex = "((?<=[^a-zA-Z0-9])(?:https?\:\/\/|[a-zA-Z0-9]{1,}\.{1}|\b)(?:\w{1,}\.{1}){1,5}(?:com|org|edu|gov|uk|net|ca|de|jp|fr|au|us|ru|ch|it|nl|se|no|es|mil|iq|io|ac|ly|sm){1}(?:\/[a-zA-Z0-9]{1,})*)"

tracker_list = thirdparties["domain"].to_list()
tracker_list = list(map(lambda x: tldextract.extract(x).domain, tracker_list))

# print(trackers)

domain_url = {}
# fout = open("tracker_url_edu.txt", "w")


def get_domain(url):
    if not url:
        return None
    url = "https://{}".format(urlparse(url).path.split("//")[-1])
    if is_string_an_url(url):
        domain = str(urlparse(url).netloc)
        domain = tldextract.extract(str(urlparse(url).netloc)).domain
        # logger.info("url:{}-----domain:{}".format(url, domain))
        if domain not in domain_url:
            domain_url[domain] = url
            # fout.write(domain + "\t" + url + "\n")
    else:
        domain = None
    return domain


def get_domain_from_cc(url):
    if not url:
        return None
    if is_string_an_url(url):
        domain = str(urlparse(url).netloc)
        domain = tldextract.extract(str(urlparse(url).netloc)).domain
        # logger.info("url:{}-----domain:{}".format(url, domain))
        if domain not in domain_url:
            domain_url[domain] = url
            # fout.write(domain + "\t" + url + "\n")
    else:
        domain = None
    return domain


# def get_domain_suffix(url):
#     domain = tldextract.extract(url).domain
#     # suffix = tldextract.extract(url).suffix
#     return domain


def is_string_an_url(url_string: str) -> bool:
    result = validators.url(url_string)

    if isinstance(result, ValidationFailure):
        return False

    return result


# now is to collecting from cc
def get_text_selectolax(html):
    trackers = []
    # try:
    tree = HTMLParser(html)
    if tree.body is None:
        return trackers
    for node in tree.tags("style"):
        node.decompose()

    #         找到a
    try:
        for node in tree.css("a,link,script,iframe,img"):
            text = node.text()
            if "google-analytics" in text:
                trackers.append("google-analytics")
            if "href" in node.attributes:
                url = node.attributes["href"]
                domain = get_domain(url)
                if domain:
                    if tracker_type == "all":
                        trackers.append(domain)
                    elif domain in tracker_list:
                        trackers.append(domain)
            if "src" in node.attributes:
                url = node.attributes["src"]
                domain = get_domain(url)

                if domain:
                    if tracker_type == "all":
                        trackers.append(domain)
                    elif domain in tracker_list:
                        trackers.append(domain)

            if (
                "type" in node.attributes
                and node.attributes["type"] == "text/javascript"
            ):

                result = re.findall(regex, text)
                for url in result:
                    domain = get_domain(url)

                    if domain:
                        if tracker_type == "all":
                            trackers.append(domain)
                        elif domain in tracker_list:
                            trackers.append(domain)
    except Exception as e:
        print(e)
    return trackers

    # except Exception as e:
    #     print(e)
    # finally:
    #     return trackers


def read_doc(record, parser=get_text_selectolax):
    url = record.url
    text = None

    if url:
        payload = record.payload.read()
        header, html = payload.split(b"\r\n\r\n", maxsplit=1)
        html = html.strip()

        if len(html) > 0:
            text = parser(html)

    return url, text


# 从s3中抽取网页trackers


def process_warc_from_archive(filename, offset=None, length=None, parser=None):
    with open(filename, "rb") as stream:
        for record in ArchiveIterator(stream):
            url = record.rec_headers.get_header("WARC-Target-URI")
            text = record.content_stream().read()
            trackers = parser(text)
            # print(trackers)
            trackers = list(set(trackers))
            if "archive" in trackers:
                trackers.remove("archive")
            # print(trackers)
            return (url, ",".join(trackers))


def process_warc_froms3(file_name, offset=None, length=None, parser=None):

    s3 = boto3.client("s3")
    # Count the range
    offset_end = offset + length - 1
    byte_range = "bytes={offset}-{end}".format(offset=offset, end=offset_end)
    resp = s3.get_object(Bucket="commoncrawl", Key=file_name, Range=byte_range)["Body"]

    for record in ArchiveIterator(resp):
        url = record.rec_headers.get_header("WARC-Target-URI")
        text = record.content_stream().read()
        trackers = parser(text)
        trackers = list(set(trackers))
        # print(url, set(trackers))
        return (url, ",".join(trackers))


# process_warc_from_archive("example_from_s3.warc", parser=get_text_selectolax)

# import re

# with open('CC-MAIN-20210513173321-20210513203321-00163.warc',encoding='utf-8',errors='ignore') as fin:
#     for line in fin:

#         try:
#             result = re.findall('<a[^>]*href="([^>]*)">',line,re.I)
#             print(result)
#         except Exception as e:
#             print(e)

# process_warc('CC-MAIN-20210513173321-20210513203321-00163.warc',get_text_selectolax,100000)

##################### 把s3文件下载下来分析 ###############


# filename = "crawl-data/CC-MAIN-2015-35/segments/1440645167592.45/warc/CC-MAIN-20150827031247-00093-ip-10-171-96-226.ec2.internal.warc.gz,2015"
# offset = 631275067
# length = 4394


########### 把s3 文件下载下来放到本地

"""
import boto3 

from botocore import UNSIGNED
from botocore.client import Config
import json
from warcio.archiveiterator import ArchiveIterator
import gzip

def collect_from_s3(url,filename,offset,length):
    # Boto3 anonymour login to common crawl
    s3 = boto3.client('s3',config = Config(signature_version = UNSIGNED))
    # Count the range
    offset_end = offset + length - 1
    byte_range = 'bytes={offset}-{end}'.format(offset=offset, end=offset_end)
    gzipped_text = s3.get_object(Bucket='commoncrawl', Key=filename, Range = byte_range)['Body']

    data = gzip.decompress(gzipped_text.read())
    text = data.decode('utf-8')
    with open("unit_test/{}.warc".format(tldextract.extract(url).domain),"w") as fout:
        fout.write(text)

import json
with open("cc_url.json") as f:
    for line in f:
        data = json.loads(line.strip())
        print(data['filename'],data['offset'],data['length'])
        collect_from_s3(data['url'],data['filename'],int(data['offset']),int(data['length']))
"""
#######
