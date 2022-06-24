import pandas as pd
from tqdm import tqdm

tqdm.pandas()

df = pd.read_csv(
    "resource/available-edu-urls.txt", names=["url", "historical_url"], sep="\t"
)
df = df.dropna()


def split_historical_url(row):
    historical_url = row["historical_url"].split(",")
    columns = list(range(2012, 2022))
    return pd.Series(dict(zip(columns, historical_url)))


df_historical = df.progress_apply(split_historical_url, axis=1)
print(df_historical.head())
