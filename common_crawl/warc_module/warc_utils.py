"""
Author: Zhan
Date: 2021-03-12 14:59:58
LastEditTime: 2022-03-27 21:39:54
LastEditors: Please set LastEditors
Description: warc utils. We can use warc feature"""

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

# add config path
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from config import args
import gzip
from botocore.client import Config
from botocore import UNSIGNED

logger = logging.getLogger("webtracking.warc_tracking")

# get trackers
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

domain_url = {}


class Example:
    trackers = None
    outgoing_links = None
    descriptions = None

    def __init__(self, trackers):
        self.trackers = trackers

    def set_outgoing_links(self, outgoing_links):
        self.outgoing_links = outgoing_links

    def set_descriptions(self, descriptions):
        self.descriptions = descriptions


def get_domain_from_ia(url, is_register_domain=False):
    """
    extract the domain from Internet Archive resource

    Args:
        url (_type_): _description_

    Returns:
        _type_: _description_
    """
    if not url:
        return None
    if is_string_an_url(url):
        # if http in path
        # path = urlparse(url).path
        # if "http" in path:
        #     url = "https://{}".format(path.split("//")[-1])
        domain = str(urlparse(url).netloc)
        if is_register_domain:
            domain = tldextract.extract(str(urlparse(url).netloc)).domain
        # ignore the "archive"
        # if "archive." in domain:
        #     return None
        if domain not in domain_url:
            domain_url[domain] = url
    else:
        domain = None
    return domain


def get_domain_from_cc(url, is_register_domain=False):
    """
    extract the domain from Common Crawl

    Args:
        url (_type_): _description_

    Returns:
        _type_: _description_
    """
    if not url:
        return None
    if is_string_an_url(url):
        domain = str(urlparse(url).netloc)
        if is_register_domain:
            domain = tldextract.extract(str(urlparse(url).netloc)).domain
        if domain not in domain_url:
            domain_url[domain] = url
    else:
        domain = None
    return domain


def is_string_an_url(url_string: str) -> bool:
    result = validators.url(url_string)

    if isinstance(result, ValidationFailure):
        return False

    return result


def get_outer_link_from_ia(url):
    """
    extract the domain from Internet Archive resource

    Args:
        url (_type_): _description_

    Returns:
        _type_: _description_
    """
    if not url:
        return None
    url = "https://{}".format(urlparse(url).path.split("//")[-1])
    if is_string_an_url(url):
        domain = str(urlparse(url).netloc)
        tld = domain.split(".")[-1]
        if tld == "tj":
            return domain
        else:
            return None
    else:
        domain = None
    return domain


def get_outer_link_from_cc(url):
    """
    extract the domain from Common Crawl

    Args:
        url (_type_): _description_

    Returns:
        _type_: _description_
    """
    if not url:
        return None
    if is_string_an_url(url):
        domain = str(urlparse(url).netloc)
        tld = domain.split(".")[-1]
        if tld == "tj":
            return domain
        else:
            return None
    else:
        domain = None
    return domain


def get_outer_link(html):
    """get outer link of the file.

    Args:
        html (_type_): _description_

    Returns:
        _type_: _description_
    """
    outer_links = []
    tree = HTMLParser(html)
    if tree.body is None:
        return outer_links
    for node in tree.tags("style"):
        node.decompose()

    try:
        for node in tree.css("a,link,script,iframe,img"):
            text = node.text()

            if "href" in node.attributes:
                url = node.attributes["href"]
                domain = get_outer_link_from_cc(url)
                if domain is not None:
                    outer_links.append(domain)

            if "src" in node.attributes:
                url = node.attributes["src"]
                domain = get_outer_link_from_cc(url)
                if domain is not None:
                    outer_links.append(domain)

            if (
                "type" in node.attributes
                and node.attributes["type"] == "text/javascript"
            ):
                result = re.findall(regex, text)
                for url in result:
                    domain = get_outer_link_from_cc(url)
                    if domain is not None:
                        outer_links.append(domain)

    except Exception as e:
        print(e)
    return outer_links


# get the description from the html file
def get_description_from_html(html):
    """get description from the html file

    Args:
        html (_type_): _description_

    Returns:
        _type_: _description_
    """
    description = None
    tree = HTMLParser(html)
    if tree.body is None:
        return description
    for node in tree.tags("style"):
        node.decompose()

    try:
        description_meta_tag = tree.css_first('meta[name="description"]')
        description = (
            description_meta_tag.attrs.get("content") if description_meta_tag else None
        )
    except Exception as e:
        print(e)
    return description


# now is to collecting from cc
def get_text_selectolax(html, source="cc", outgoing_link=False):
    """extracting the tracker domain from html file

    Args:
        html (_type_): _description_

    Returns:
        _type_: _description_
    """
    trackers = []
    links = []
    tree = HTMLParser(html)
    if tree.body is None:
        return trackers, None  # none means the outgoing links is None
    for node in tree.tags("style"):
        node.decompose()

    if source == "cc":
        get_domain = get_domain_from_cc
    elif source == "ia":
        get_domain = get_domain_from_ia

    try:
        for node in tree.css("a,link,script,iframe,img"):
            text = node.text()
            if "google-analytics" in text:
                trackers.append("google-analytics.com")

            if "href" in node.attributes:
                url = node.attributes["href"]
                if url and "http" in url:
                    links.append(url)
                domain = get_domain(url)
                if domain:
                    if tracker_type == "all":
                        trackers.append(domain)
                    elif domain in tracker_list:
                        trackers.append(domain)
            if "src" in node.attributes:
                url = node.attributes["src"]
                if url and "http" in url:
                    links.append(url)
                domain = get_domain(url)
                if domain:
                    if tracker_type == "all":
                        trackers.append(domain)
                    elif domain in tracker_list:
                        trackers.append(domain)

            if "onclick" in node.attributes:
                url = node.attributes["onclick"]
                if url and "http" in url:
                    links.append(url)
                if url and "window.open" in url:
                    url = url.replace("window.open(", "")
                    url = url.replace("+", "")
                    url = url.replace(")", "")
                    url = url.replace("'", "")
                    url = url.replace(" ", "")
                    url = url.split(";")[0]
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
                    if url and "http" in url:
                        links.append(url)
                    domain = get_domain(url)

                    if domain:
                        if tracker_type == "all":
                            trackers.append(domain)
                        elif domain in tracker_list:
                            trackers.append(domain)
    except Exception as e:
        print("error is in get_text_selectolax", e)
    if outgoing_link:
        return trackers, links
    return trackers, None


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


def process_warc_from_archive(
    filename, offset=None, length=None, parser=None, get_description=False
):
    with open(filename, "rb") as stream:
        for record in ArchiveIterator(stream):
            url = record.rec_headers.get_header("WARC-Target-URI")
            text = record.content_stream().read()
            trackers = parser(text, source="ia")
            trackers = list(set(trackers))
            if "archive" in trackers:
                trackers.remove("archive")
            return (url, ",".join(trackers))


def process_warc_froms3(
    file_name,
    offset=None,
    length=None,
    parser=None,
    get_description=False,
    outgoing_link=False,
):
    s3 = boto3.client("s3")
    offset_end = offset + length - 1
    byte_range = "bytes={offset}-{end}".format(offset=offset, end=offset_end)
    resp = s3.get_object(Bucket="commoncrawl", Key=file_name, Range=byte_range)["Body"]
    try:
        for record in ArchiveIterator(resp):
            url = record.rec_headers.get_header("WARC-Target-URI")
            text = record.content_stream().read()
            fields = parser(text)
            trackers = fields[0]
            outgoing_links = fields[1]
            trackers = list(set(trackers))
            example = Example(trackers)
            if get_description:
                description = get_description_from_html(text)
                example.set_descriptions(description)
            if outgoing_link:
                example.set_outgoing_links(outgoing_links)
            return example
    except Exception as e:
        print("error is process_warc_froms3", e)
        print(fields)


def download_warc_froms3(filename, offset, length):
    # Boto3 anonymour login to common crawl
    try:
        s3 = boto3.client("s3")
        # Count the range
        offset_end = offset + length - 1
        byte_range = "bytes={offset}-{end}".format(offset=offset, end=offset_end)
        gzipped_text = s3.get_object(
            Bucket="commoncrawl", Key=filename, Range=byte_range
        )["Body"]

        data = gzip.decompress(gzipped_text.read())
        text = data.decode("latin-1")
        return text

    except Exception as e:
        print(e)
    return None


def collect_from_s3(filename, offset, length):
    # Boto3 anonymour login to common crawl
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    # Count the range
    offset_end = offset + length - 1
    byte_range = "bytes={offset}-{end}".format(offset=offset, end=offset_end)
    gzipped_text = s3.get_object(Bucket="commoncrawl", Key=filename, Range=byte_range)[
        "Body"
    ]

    data = gzip.decompress(gzipped_text.read())
    text = data.decode("utf-8")
    print(text)


if __name__ == "__main__":
    # filename = "crawl-data/CC-MAIN-2015-35/segments/1440645167592.45/warc/CC-MAIN-20150827031247-00093-ip-10-171-96-226.ec2.internal.warc.gz"
    # offset = 631275067
    # length = 4394
    # collect_from_s3(filename, offset, length)

    trackers = process_warc_from_archive(
        "common_crawl_bbc_sample.warc", parser=get_text_selectolax
    )
    print(trackers)
    # import re

    # with open(
    #     "CC-MAIN-20210513173321-20210513203321-00163.warc",
    #     encoding="utf-8",
    #     errors="ignore",
    # ) as fin:
    #     for line in fin:
    #         try:
    #             result = re.findall('<a[^>]*href="([^>]*)">', line, re.I)
    #             print(result)
    #         except Exception as e:
    #             print(e)

# process_warc('CC-MAIN-20210513173321-20210513203321-00163.warc',get_text_selectolax,100000)
