##################### collecting archived dataset ##########

# This is the file to collecting the dataset from internet archive and store the zip file in local machine.
import time
import glob
import pandas as pd
import os
import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from pipeline_archived.utils import collect_dataset
from pipeline_archived.utils import collect_dataset_from_ali
from tqdm import tqdm

tqdm.pandas()
t = time.time()
# df = pd.read_csv(
#     "resource/available-edu-urls.txt", names=["url", "historical_url"], sep="\t"
# )
# df = df.dropna()

df = pd.read_csv("resource/df_have_no_archived_control.csv")


def split_historical_url(row):
    historical_url = row["historical_url"].split(",")
    columns = list(range(2012, 2022))
    return pd.Series(dict(zip(columns, historical_url)))


# get historical url
print("split historical url")
df_historical = df.progress_apply(split_historical_url, axis=1)
df_historical["url"] = df["url"]

df_historical[:100].progress_apply(
    collect_dataset_from_ali,
    axis=1,
    data_path="dataset_archive",
    task_type="control",
)
# df_final = pd.read_csv("resource/edu_repair/df_final.csv")
# files = sorted(
#     glob.glob("resource/edu_repair/sample_domain_historical_year_*_repair.csv")
# )

# for file in files:
#     df = pd.read_csv(file)
#     year = file.split("_")[-2]
#     if not os.path.exists("dataset_archive/control_archive_repair/{}".format(year)):
#         os.makedirs("dataset_archive/control_archive_repair/{}".format(year))
#     print(year)
#     df_collect = df[df["sample_domain"].isin(df_final["control_domain"])][3000:]

#     df_collect.progress_apply(collect_dataset, axis=1, year=year)

print(time.time() - t)
