import pandas as pd
from collections import Counter
import glob

files = sorted(glob.glob("resource/edu_repair/edu_domain_historical_year_*_repair.csv"))

final_file = files[0]
df_final = pd.read_csv(final_file)
print(len(df_final))

key_domain = "edu_domain"
for file in files[1:]:
    # df = pd.read_csv(file).dropna()
    df = pd.read_csv(file)
    # print(set(df_final['edu_domain'].to_list()) - set(df['edu_domain'].to_list()))
    df_final = df_final.merge(df, how="inner", on=key_domain)

df_final = df_final.drop_duplicates()

columns_names = list(map(str, range(2012, 2022)))

df_final["nan_count"] = df_final.isna().sum(axis=1)
print(df_final.columns)
df_final.columns = [key_domain] + columns_names + ["nan_count"]
print(df_final.columns)
print(len(df_final))

word_count = Counter(df_final["nan_count"].to_list())
print(word_count.most_common())
# df_final[['edu_domain','nan_count']].to_csv('resource/edu_repair/edu_set.csv',index = None)

print("how many urls are not missing ", len(df_final.dropna()))

df_final = df_final.fillna(0)
df_final = df_final.replace(
    to_replace=r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)",
    value=1,
    regex=True,
)

df_final.to_csv("resource/edu_repair/edu_set.csv", index=None)

# print(df_final.head())
