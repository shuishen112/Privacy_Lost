import pandas as pd
from collections import Counter
import matplotlib.pyplot as plt

df_control = pd.read_csv("resource/edu_repair/control_set.csv")
df_control = df_control[['sample_domain','nan_count']]
df_edu = pd.read_csv("resource/edu_repair/edu_set.csv")
df_edu = df_edu[['edu_domain','nan_count']]
df_concat = pd.concat([df_edu, df_control], axis=1)
df_concat.columns = [
    "edu_domain",
    "nan_count_edu",
    "control_domain",
    "nan_count_control",
]
print(df_concat.columns)


df_final = df_concat[(df_concat["nan_count_control"] == 0) & (df_concat["nan_count_edu"] == 0)]

df_final.to_csv("resource/edu_repair/df_final.csv",index = None)

# ax = df_concat.plot.hist(bins=12, alpha=0.5)


edu_count = sorted(Counter(df_concat["nan_count_edu"].to_list()).most_common())

control_count = sorted(Counter(df_concat["nan_count_control"].to_list()).most_common())

print(edu_count)
print(control_count)
