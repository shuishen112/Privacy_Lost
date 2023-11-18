################################## get the tracker information ##########
import time
import glob
from tqdm import tqdm
import pandas as pd
import sys
from os import path
import logging
import os

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from config import args

from warc_module.warc_utils import (
    get_text_selectolax,
    process_warc_from_archive,
    get_outer_link,
)
from utils import (
    extract_trackers_from_internet_archive,
    collect_dataset_from_url,
    download_dataset_from_url,
)
import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
    "--output_path",
    type=str,
    default="websci/IA/GOV/",
    help="output path",
)

parser.add_argument(
    "--year",
    type=str,
    default="2014",
    help="year",
)

args_ = parser.parse_args()


###### add log ######
logging.basicConfig(
    filename="logs/all_tracker.log",
    level=logging.WARNING,
    format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
)
logger = logging.getLogger("webtracking")
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter("%(asctime)s|%(levelname)s|%(name)s|%(message)s")
)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

t = time.time()


df = pd.read_csv(
    args["data_path"],
    names=["edu_url", "url", "historical_url"],
    sep="\t",
).dropna()

logger.info(f"task_type:{args['task_type']}")

if args["task_type"] == "edu":
    filter_set = set(df["edu_url"].unique())
else:
    filter_set = set(df["url"].unique())


def collect_trackers(type, year):
    """collect trackers from specific year

    Args:
        type (_type_): edu or non-edu
        year (_type_): specific year
    """

    files = sorted(glob.glob(f"dataset_archive/{type}/{year}/*.gz"))

    urls = []
    trackesr = []
    for file in tqdm(files):
        file_name = file.split("/")[-1].replace(".gz", "")
        if file_name not in filter_set:
            continue
        result = process_warc_from_archive(file, parser=get_text_selectolax)
        if result:
            url, tracker_list = result
            urls.append(file_name)
            trackesr.append(tracker_list)
        else:
            # if result is None, it means this file can not be archved by the waybackpy
            url = file.split("/")[-1][:-3]
            tracker_list = None
            urls.append(file_name)
            trackesr.append(tracker_list)
            print("empty file", file)

    df_edu_history = pd.DataFrame({"url": urls, "3p-domain": trackesr})
    df_edu_history.to_csv(
        f"dataset_archive/{type}_{args['tracker_type']}_{year}.csv", index=None
    )


################################### store the zip file from historical trackers from Internet Archive ###############################

# for year in range(2017, 2023):
#     year = str(year)
#     store_path = f"RQX/10000sample_analysis/IA_archive/{year}"
#     if not os.path.exists(store_path):
#         os.makedirs(store_path)
#     start_time = time.time()
#     # whether it has been archived
#     dir_list = os.listdir(store_path)
#     dir_list = [name[:-5] for name in dir_list]
#     df_sample_500 = pd.read_csv(
#         f"RQX/10000sample_analysis/IA/domain_historical_year_{year}.csv",
#         sep="\t",
#         names=["domain", "historical_url"],
#     ).dropna()
#     for e, item in df_sample_500.iterrows():

#         domain = item["domain"]
#         if domain in dir_list:
#             # it has been archived
#             continue
#         history_url = item["historical_url"]
#         text = download_dataset_from_url(history_url)
#         if text is not None:
#             output = f"{store_path}/{domain}.warc"
#             fout = open(output, "w")
#             fout.write(text)
#     end_time = time.time()
#     print("time consuming:{}".format(end_time - start_time))

#################################### collecting trackers from stored local machines #################################################

# fout = open("RQX/2015_500_extract_trackers_archive_internet.csv", "w")
# file_list = os.listdir("RQX/IA2015/")

# for file in tqdm(file_list):
#     file_name = file[:-5]
#     text = open(os.path.join("RQX/IA2015/", file)).read()
#     trackers = get_text_selectolax(text, source="ia")
#     trackers = list(set(trackers))
#     if trackers is not None:
#         fout.write(file_name + "\t" + ",".join(trackers) + "\n")
#     else:
#         fout.write(file_name + "\n")
#     fout.flush()

# fout = open("RQX/10000sample_analysis/IA_2014_2019_trackers.csv", "w")
# for year in range(2014, 2020):
#     year = str(year)
#     file_list = os.listdir(f"RQX/10000sample_analysis/IA_archive/{year}")

#     for file in tqdm(file_list):
#         file_name = file[:-5]
#         text = open(
#             os.path.join(f"RQX/10000sample_analysis/IA_archive/{year}", file)
#         ).read()
#         trackers = get_text_selectolax(text, source="ia")
#         trackers = list(set(trackers))
#         if trackers is not None:
#             fout.write(year + "\t" + file_name + "\t" + ",".join(trackers) + "\n")
#         else:
#             fout.write(year + "\t" + file_name + "\n")
#         fout.flush()


#################################### collecting from historical trackers from Internet Archive ###############################


def get_dataframe(year):
    df = pd.read_csv(
        f"websci/scanning_websites/government_websites_page_rank_{year}_top_500_historical_url.csv",
        sep=",",
    )
    return df


# only test one historical snapshot
def test_archive():
    history_url = "https://web.archive.org/web/20151127040830/http://www.region-orlickehory.cz:80/"
    trackers = extract_trackers_from_internet_archive(history_url, get_text_selectolax)
    print(f"len:{len(trackers)}", trackers)


if __name__ == "__main__":
    df = get_dataframe(args_.year)

    fout = open(args_.output_path + f"/archived_websites_{args_.year}", "w")
    for e, item in df.iterrows():
        hostname = item["hostname"]
        history_url = item["historical_url"]
        print(hostname, history_url)
        if isinstance(history_url, float):
            fout.write(hostname + "\t" + "EMPTY_URL" + "\n")
            continue
        time.sleep(1)
        logger.info(f"collecting number:{e}:{hostname}")
        trackers = extract_trackers_from_internet_archive(
            history_url, get_text_selectolax
        )
        if trackers is not None:
            fout.write(hostname + "\t" + ",".join(trackers) + "\n")
        else:
            fout.write(hostname + "\t" + "NO_TRACKERS" + "\n")
        fout.flush()

#################################### colllecting outlinks from Internet Archive ###############################

# fout = open("RQX/tj_outlinks_of_old_outlinks.csv", "w")
# df_tj = pd.read_csv("RQX/tj_outlinkes_old_new.csv")
# for _, item in df_tj.iterrows():
#     try:
#         domain = item["outlinks"]

#         history_url = item["old_url"]
#         print(history_url)
#         outer_links = extract_trackers_from_internet_archive(history_url, get_outer_link)
#         fout.write(",".join(outer_links) + "\n")
#         # print(outer_links)å
#         fout.flush()

#     except Exception as e:
#         print(e)

########################### collecting from archive from ali


# years = list(range(2012, 2022))
# for year in years:
#     collect_trackers(f"{args['task_type']}_archive_ali", year)
# logger.info(time.time() - t)

########################### only test one historical snapshot ###########################
