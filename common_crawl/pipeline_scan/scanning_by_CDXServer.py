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
    default="websci/government_websites.csv",
    help="sampe path",
)

parser.add_argument(
    "--output_path",
    type=str,
    default="websci/IA/GOV/",
    help="output path",
)

parser.add_argument(
    "--year_begin",
    type=int,
    default=2023,
    help="year",
)

parser.add_argument(
    "--year_end",
    type=int,
    default=2024,
    help="year",
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
        if len(list_archive) == 2:
            timestamp = list_archive[1][0]
            origin = list_archive[1][1]
            historical_url = f"https://web.archive.org/web/{timestamp}/{origin}"
            return historical_url
    except Exception as e:
        print(e)

    return None


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

        fout.write(domain + "\t" + old[1][1] + "\t" + new[1][1] + "\n")
        fout.flush()
        return old[1][1], new[1][1]
    except Exception as e:
        print(e)


def get_timescan_fromCC(domain):
    link = (
        f"https://index.commoncrawl.org/CC-MAIN-2022-43-index?url={domain}&output=json"
    )
    try:
        f = urlopen(link)

        myfile = f.read()

        archive_url = json.loads(myfile)
        print(archive_url)
    except Exception as e:
        print(e)


def multi_threading():
    pool = mp.Pool(5)

    for _ in tqdm(
        pool.imap_unordered(get_timespan, list(list_host_name)),
        total=len(list_host_name),
    ):
        pass
    pool.close()
    pool.join()


def unit_test():
    url = "ku.dk"
    year_from = "1985"
    year_to = "2022"
    print(get_specific_time_url(url, year_from, year_to))
    get_timespan("archive.org")


if args.input_data_path.endswith("csv"):
    df = pd.read_csv(args.input_data_path, sep="\t", names=["host_name"])
elif args.input_data_path.endswith("jsonl"):
    df = pd.read_json(args.input_data_path, lines=True)
list_host_name = df["host_name"].to_list()
if os.path.exists(args.output_path):
    print("output path exist")
else:
    os.mkdir(args.output_path)
for year in range(args.year_begin, args.year_end):
    fout = open(f"{args.output_path}domain_historical_year_{str(year)}.csv", "w")
    print(year)
    for item in tqdm(list_host_name):
        time.sleep(1)
        historical_url = get_specific_time_url(item, str(year), str(year))
        if historical_url:
            fout.write(item + "\t" + historical_url + "\n")
        else:
            fout.write(item + "\t" + "NAN" + "\n")
        fout.flush()
    fout.close()
