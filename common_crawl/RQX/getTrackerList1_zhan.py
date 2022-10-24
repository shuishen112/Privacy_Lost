from selectolax.parser import HTMLParser
from urllib.parse import urlparse
import validators
from validators import ValidationFailure
import re
import tldextract
import urllib.request
from urllib.request import Request, urlopen
from tqdm import tqdm

import multiprocessing as mp

# from pandarallel import pandarallel

# # Initialization
# pandarallel.initialize()
tqdm.pandas()
import time
import pandas as pd


regex = "((?<=[^a-zA-Z0-9])(?:https?\:\/\/|[a-zA-Z0-9]{1,}\.{1}|\b)(?:\w{1,}\.{1}){1,5}(?:com|org|edu|gov|uk|net|ca|de|jp|fr|au|us|ru|ch|it|nl|se|no|es|mil|iq|io|ac|ly|sm){1}(?:\/[a-zA-Z0-9]{1,})*)"
domain_url = {}


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


def is_string_an_url(url_string: str) -> bool:
    result = validators.url(url_string)

    if isinstance(result, ValidationFailure):
        return False

    return result


def get_domain_suffix(url):
    domain = tldextract.extract(url).domain
    # suffix = tldextract.extract(url).suffix
    return domain


# get black list of domains
def black_list_domains(url):
    black_list = []
    for u in re.findall(
        "(?:(?:https?|http):\/\/)?[\w/\-?=%.]+\.[\w/\-&?=%.]+",
        url.replace("/https", " /https").replace("/http", " /http"),
    ):
        black_list.append(get_domain_suffix(u))
    black_list.append(None)
    black_list.append("archive")
    return black_list


def time_it(func):
    def inner():
        start = time.time()
        func()
        end = time.time()
        print("time:{}seconds".format(end - start))

    return inner


def get_text_selectolax(html, black_list):
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
                if domain not in black_list:
                    trackers.append(domain)
            if "src" in node.attributes:
                url = node.attributes["src"]
                domain = get_domain(url)
                if domain not in black_list:
                    trackers.append(domain)

            if (
                "type" in node.attributes
                and node.attributes["type"] == "text/javascript"
            ):

                result = re.findall(regex, text)
                for url in result:
                    domain = get_domain(url)
                    if domain not in black_list:
                        trackers.append(domain)
    except Exception as e:
        print(e)
    # list(set(trackers))
    return list(set(trackers))


# defining header
header = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.11 (KHTML, like Gecko) "
    "Chrome/23.0.1271.64 Safari/537.11",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Charset": "ISO-8859-1,utf-8;q=0.7,*;q=0.3",
    "Accept-Encoding": "none",
    "Accept-Language": "en-US,en;q=0.8",
    "Connection": "keep-alive",
}

df = pd.read_csv("missing_archive_clean.csv")
print(df.head())


def task(row):
    try:
        timestamp = row["snap_list"]
        hostname = row["hostname"]
        suff = hostname.split(".")[len(hostname.split(".")) - 1]
        new_url = "https://web.archive.org/web/" + str(timestamp) + "/" + hostname
        black_list = black_list_domains(hostname)
        start_time = time.time()
        req = urllib.request.Request(url=new_url, headers=header)
        page = urllib.request.urlopen(req).read()

        tracker_list = str(get_text_selectolax(page, black_list))
        end_time = time.time()

        print("time:{}".format(end_time - start_time))
        # print(str(u1[0:4]) + "\t" + str(get_text_selectolax(page,black_list)))
        f1 = open("trackers_output_temp.txt", "a")
        f1.write(
            hostname
            + "\t"
            + tracker_list
            + "\t"
            + str(timestamp)[0:4]
            + "\t"
            + suff
            + "\n"
        )
        f1.close()
        # print(get_text_selectolax(page,black_list))

    except Exception as e:
        print(e)
        pass


pool = mp.Pool(5)

# pool.map(collect_trackers_from_map_cc, list(v))
for _ in tqdm(pool.imap_unordered(task, df), total=len(df)):
    pass
pool.close()
pool.join()


# df.progress_apply(task, axis=1)
