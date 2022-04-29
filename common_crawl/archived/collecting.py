##################### collecting archived dataset ##########
import time
import glob
import pandas as pd
import os

from archived.utils import collect_dataset

t = time.time()

df_final = pd.read_csv("resource/edu_repair/df_final.csv")
files = sorted(
    glob.glob("resource/edu_repair/sample_domain_historical_year_*_repair.csv")
)

for file in files:
    df = pd.read_csv(file)
    year = file.split("_")[-2]
    if not os.path.exists("dataset_archive/control_archive_repair/{}".format(year)):
        os.makedirs("dataset_archive/control_archive_repair/{}".format(year))
    print(year)
    df_collect = df[df["sample_domain"].isin(df_final["control_domain"])][3000:]

    df_collect.progress_apply(collect_dataset, axis=1, year=year)

print(time.time() - t)
