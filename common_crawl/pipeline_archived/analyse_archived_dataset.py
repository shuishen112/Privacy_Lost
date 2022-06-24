import glob
import os
import pandas as pd
from utils import collect_dataset_from_url
from utils import get_historical_url
from tqdm import tqdm

"""
This script will analyse the archived dataset. are there lots of empty files? 

As there are some dataset are 0, we need recollect them.
"""
df = pd.read_csv(
    f"resource/available-control-urls.txt",
    names=["edu_url", "url", "historical_url"],
    sep="\t",
).dropna()

control_url_set = set(df["url"].unique())

edu_url_set = set(df["edu_url"].unique())

for year in range(2012, 2022):
    files = glob.glob(f"dataset_archive/edu_archive_ali/{year}/*.gz")

    count = 0
    file_count = 0
    for file in tqdm(files):
        file_name = file.split("/")[-1].replace(".gz", "")
        if file_name in edu_url_set:
            file_count += 1
        stat_info = os.stat(file)
        file_size = stat_info.st_size
        if file_size == 0:
            count += 1
    print(count)
    print("file_count", file_count)

# historical_url = get_historical_url("edu", "artandwriting.org", 2014)
# collect_dataset_from_url(
#     historical_url, "dataset_archive/edu_archive/2014/artandwriting.org.gz"
# )
