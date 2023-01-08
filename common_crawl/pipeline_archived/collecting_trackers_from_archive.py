################################## get the tracker information ##########
import time
import glob
from tqdm import tqdm
import pandas as pd
import sys
from os import path
import logging

from utils import (
    extract_trackers_from_internet_archive,
    collect_dataset_from_url,
    download_dataset_from_url,
)


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
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from config import args

from warc_module.warc_utils import (
    get_text_selectolax,
    process_warc_from_archive,
    get_outer_link,
)


df = pd.read_csv(
    f"resource/available-control-urls.txt",
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
df_sample_500 = pd.read_csv("RQX/domain_historical_year_2016.csv")

for e, item in df_sample_500.iterrows():
    domain = item["domain"]
    history_url = item["history_url"]
    print(e)
    text = download_dataset_from_url(history_url)
    if text is not None:
        output = f"RQX/IA2016/{domain}.warc"
        fout = open(output, "w")
        fout.write(text)

#################################### colllecting from historical trackers from Internet Archive ###############################
# df_sample_500 = pd.read_csv("RQX/domain_historical_year_2016.csv")

# fout = open("RQX/2016_500_extract_trackers_archive_internet.csv", "w")
# for _, item in df_sample_500.iterrows():
#     domain = item["domain"]
#     history_url = item["history_url"]

#     trackers = extract_trackers_from_internet_archive(history_url, get_text_selectolax)
#     if trackers is not None:
#         fout.write(domain + "\t" + ",".join(trackers) + "\n")
#     else:
#         fout.write(domain + "\n")
#     fout.flush()

#################################### colllecting from historical trackers from Internet Archive ###############################

# fout = open("RQX/tj_outlinks_old_one.csv", "w")
# df_tj = pd.read_csv("RQX/tj_old_new.csv")
# for _, item in df_tj.iterrows():
#     try:
#         domain = item["domain"]

#         history_url = item["old_url"]
#         outer_links = extract_trackers_from_internet_archive(
#             history_url, get_outer_link
#         )
#         fout.write(",".join(outer_links) + "\n")
#         print(outer_links)
#         fout.flush()

#     except Exception as e:
#         print(e)

# years = list(range(2012, 2022))
# for year in years:
#     collect_trackers(f"{args['task_type']}_archive_ali", year)
# logger.info(time.time() - t)
