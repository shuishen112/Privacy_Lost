################################## get the tracker information
import time
import glob
from tqdm import tqdm
import pandas as pd
import sys
from os import path

t = time.time()
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from warc_module.warc_utils import get_text_selectolax, process_warc_from_archive


def collect_trackers(type, year):
    """collect trackers from specific year

    Args:
        type (_type_): edu or non-edu
        year (_type_): specific year
    """

    files = glob.glob(f"dataset_archive/{type}/{year}/*.gz")

    urls = []
    trackesr = []
    for file in tqdm(files):
        result = process_warc_from_archive(file, parser=get_text_selectolax)
        if result:
            url, tracker_list = result
            urls.append(url)
            trackesr.append(tracker_list)
        else:
            # if result is None, it means this file can not be archved by the waybackpy
            url = file.split("/")[-1][:-3]
            tracker_list = None
            urls.append(url)
            trackesr.append(tracker_list)
            print("empty file", file)

    df_edu_history = pd.DataFrame({"url": urls, "3p-domain": trackesr})
    df_edu_history.to_csv(f"dataset_archive/{type}_{year}.csv", index=None)
    print(time.time() - t)


years = list(range(2012, 2022))
for year in years:
    collect_trackers("control_archive", year)
