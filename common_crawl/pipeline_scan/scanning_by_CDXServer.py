from waybackpy import WaybackMachineCDXServerAPI
from urllib.request import urlopen
import json, requests
import time
import pandas as pd
import multiprocessing as mp
from tqdm import tqdm
import cdx_toolkit
import argparse
import os 
parser = argparse.ArgumentParser()
parser.add_argument(
    "--input_data_path",
    type=str,
    default="websci/goverment_websites.csv",
    help="sampe path",
)

parser.add_argument(
    "--output_path",
    type=str,
    default="websci/IA/",
    help="output path",
)

args = parser.parse_args()

def get_specific_time_url(url, year_from, year_to):
    link = (
        "http://web.archive.org/cdx/search/cdx?url="
        + url
        + "&fl=timestamp,original&mimetype=text/html&output=json&from="
        + year_from
        + "&to="
        + year_to
        + "&collapse=timestamp:4&showSkipCount=true&lastSkipTimestamp=true&limit=1"
    )
    try:
        f = urlopen(link)
        myfile = f.read()
        list_archive = json.loads(myfile)
        # print(f"url: {url}----- archive_url:{archive_url}")
        if len(list_archive) == 2:
            timestamp = list_archive[1][0]
            origin = list_archive[1][1]
            historical_url = f"https://web.archive.org/web/{timestamp}/{origin}"
            return historical_url
    except Exception as e:
        print(e)

    return None

    # snap_list = {}
    # for line in archive_url[1:]:
    #     snap_list[line[0][0:4]] = line[0]
    # return archive_url


fout = open("resource/top10Msample10000timegap.txt", "w")


def get_timespan(domain):

    try:
        old_f = urlopen(
            f"https://web.archive.org/cdx/search/cdx?url={domain}&limit=1&output=json"
        )
        myfile = old_f.read()
        old = json.loads(myfile)

        new_f = urlopen(
            f"https://web.archive.org/cdx/search/cdx?url={domain}&limit=1&fastLatest=True&output=json"
        )
        myfile = new_f.read()
        new = json.loads(myfile)
        # print(old[1][1], new[1][1])

        fout.write(domain + "\t" + old[1][1] + "\t" + new[1][1] + "\n")
        fout.flush()
        return old[1][1], new[1][1]
    except Exception as e:
        print(e)


def get_timescan_fromCC(domain):
    link = (
        f"https://index.commoncrawl.org/CC-MAIN-2022-43-index?url={domain}&output=json"
    )
    # link = "https://index.commoncrawl.org/CC-MAIN-2021-43-index?url=coursera.com&output=json"
    try:
        f = urlopen(link)

        myfile = f.read()

        archive_url = json.loads(myfile)
        print(archive_url)
    except Exception as e:
        print(e)



df = pd.read_csv(args.input_data_path, sep="\t",names = ['host_name'])
list_host_name = df["host_name"].to_list()
# if os.path.exists(args.output_path):
#     print("output path exist")
# else:
#     os.mkdir(args.output_path)
# for year in range(2014, 2023):
#     fout = open(
#         f"{args.output_path}domain_historical_year_{str(year)}.csv", "w"
#     )
#     print(year)
#     for item in tqdm(list_host_name):
#         historical_url = get_specific_time_url(item, str(year), str(year))
#         if historical_url:
#             fout.write(item + "\t" + historical_url + "\n")
#         else:
#             fout.write(item + "\n")
#         fout.flush()


pool = mp.Pool(5)

for _ in tqdm(
    pool.imap_unordered(get_timespan, list(list_host_name)), total=len(list_host_name)
):
    pass
pool.close()
pool.join()
# loop here
# url = "ku.dk"
# year_from = "1985"
# year_to = "2022"
# print(get_specific_time_url(url, year_from, year_to))
# get_timespan("archive.org")
