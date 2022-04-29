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

for year in range(2012, 2022):
    files = glob.glob(f"dataset_archive/control_archive/{year}/*.gz")
    count = 0
    for file in tqdm(files):
        stat_info = os.stat(file)
        file_size = stat_info.st_size
        if file_size == 0:
            count += 1
    print(count)

# historical_url = get_historical_url("edu", "artandwriting.org", 2014)
# collect_dataset_from_url(
#     historical_url, "dataset_archive/edu_archive/2014/artandwriting.org.gz"
# )
