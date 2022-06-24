import pandas as pd
import os

df = pd.read_csv(
    f"resource/available-control-urls.txt",
    names=["edu_url", "url", "historical_url"],
    sep="\t",
).dropna()
all_list = df["url"].to_list()

# print(len(df["edu_url"]))
# print(len(df["edu_url"].unique()))

# print(len(df["url"]))
# print(len(df["url"].unique()))
url_set = set(df["url"].unique())

edu_url_set = set(df["edu_url"].unique())

file_list = os.listdir("dataset_archive/control_archive_ali/2012")
edu_file_list = os.listdir("dataset_archive/edu_archive_ali/2012")
file_list = list(map(lambda x: x.replace(".gz", ""), file_list))
file_set = set(file_list)
edu_file_set = set(list(map(lambda x: x.replace(".gz", ""), edu_file_list)))

print(len(file_set))
print(len(url_set))
inter_set = file_set - url_set
print(inter_set)

# df.loc[df.url.isin(inter_set)].to_csv("resource/df_have_no_archived_control.csv",index = None)

# print(len(edu_file_set))
# print(len(edu_url_set))
# inter_set = edu_file_set.intersection(edu_url_set)
# print(len(inter_set))
