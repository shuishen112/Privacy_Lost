import pandas as pd


task_type = "control"
df_have_archived = pd.read_csv("resource/edu_repair/df_final.csv")

archived_list = df_have_archived[f"{task_type}_domain"].to_list()
print(df_have_archived)

# df_edu = pd.read_csv(
#     "resource/available-edu-urls.txt", names=["url", "historical_url"], sep="\t"
# ).dropna()

df = pd.read_csv(
    f"resource/available-{task_type}-urls.txt",
    names=["edu_url", "url", "historical_url"],
    sep="\t",
).dropna()
all_list = df["url"].to_list()

print(len(df["edu_url"]))
print(len(df["edu_url"].unique()))

print(len(df["url"]))
print(len(df["url"].unique()))

have_archived = set(all_list).intersection(set(archived_list))
df_have_archived = df.loc[df.url.isin(have_archived)]
df_have_archived.to_csv(f"resource/df_have_archived_{task_type}.csv", index=None)
have_no_archhived = set(all_list) - set(archived_list)

print("how many websites has been archived")
print(len(df_have_archived))

df_have_no_archhived = df.loc[df.url.isin(have_no_archhived)]
df_have_no_archhived.to_csv(f"resource/df_have_no_archived_{task_type}.csv", index=None)
