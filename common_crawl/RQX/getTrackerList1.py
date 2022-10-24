from selectolax.parser import HTMLParser
from urllib.parse import urlparse
import validators
from validators import ValidationFailure
import re
import tldextract
import urllib.request
from urllib.request import Request, urlopen
from tqdm import tqdm
import time

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
finished_snaps = {}
with open("trackers_output.txt", "r", encoding="latin-1") as file1:
    urls1 = file1.readlines()
    urls1 = [line.rstrip() for line in urls1]
    for u in urls1:
        line = u.split("\t")
        if len(line) > 2:
            finished_snaps[line[0] + line[2]] = 0
print(str(len(finished_snaps)))
count = 0
urls = []
# start = input("Enter the starting line number: ")
# end = input("Enter the ending line number: ")
start = 600000
end = 700000
with open("missing_archive_snapshots.txt", "r", encoding="utf8") as file:
    urls = file.readlines()
    urls = [line.rstrip() for line in urls]
    for u in tqdm(urls[int(start) : int(end)]):
        count = count + 1
        line = u.split("\t")
        # print(str(count))
        try:
            url = line[0]
            url_list = line[1].strip("][").replace("'", "").split(", ")
            snaps = line[2]
            suff = url.split(".")[len(url.split(".")) - 1]
            if snaps == "0" and line[1] != "[]":
                for u1 in url_list:
                    if str(url + u1[0:4]) not in finished_snaps:
                        new_url = "https://web.archive.org/web/" + u1 + "/" + url
                        print(new_url)
                        black_list = black_list_domains(url)
                        # print (u.split("/")[4][0:4])

                        req = urllib.request.Request(url=new_url, headers=header)
                        page = urllib.request.urlopen(req).read()
                        start_time = time.time()
                        tracker_list = str(get_text_selectolax(page, black_list))
                        end_time = time.time()
                        print("using time:{}", (end_time - start_time))
                        # print(str(u1[0:4]) + "\t" + str(get_text_selectolax(page,black_list)))
                        f1 = open("trackers_output.txt", "a")
                        f1.write(
                            url
                            + "\t"
                            + tracker_list
                            + "\t"
                            + str(u1[0:4])
                            + "\t"
                            + suff
                            + "\n"
                        )
                        f1.close()
                        # print(get_text_selectolax(page,black_list))

        except Exception as e:
            print(e)
            pass
