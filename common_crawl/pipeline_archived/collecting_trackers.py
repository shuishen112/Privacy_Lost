################################## get the tracker information ##########
import time
import glob
from tqdm import tqdm
import pandas as pd
import sys
from os import path
import logging


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

from warc_module.warc_utils import get_text_selectolax, process_warc_from_archive


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


years = list(range(2012, 2022))
for year in years:
    collect_trackers(f"{args['task_type']}_archive_ali", year)
logger.info(time.time() - t)
