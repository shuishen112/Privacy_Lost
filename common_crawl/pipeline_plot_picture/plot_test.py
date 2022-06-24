import pandas as pd


df = pd.read_csv("dataset_archive/frame_edu_count.csv", sep="\t")

print(df.std() + df.mean())

print(df.std(ddof=0))
print(df.mean())
print(df.median())
