import pandas as pd

import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
    "--year",
    type=int,
    default=None,
    help="year",
)
args = parser.parse_args()


def get_dataset(year):
    df = pd.read_csv(
        f"../IA/GOV/domain_historical_year_{year}.csv",
        sep="\t",
        names=["hostname", "historical_url"],
    )
    # remove all the historical_url = 'NAN'
    df_unique = df[df["historical_url"] != "NAN"]
    df_unique

    df_seed = pd.read_csv("../seed_urls/government_websites_page_rank_2022_top_500.csv")

    df_result = df_seed["hostnames"]
    # change the column name to hostname
    df_result = df_result.rename("hostname")

    df_merge = pd.merge(df_result, df_unique, on="hostname", how="left")

    # print nan rate
    print(
        "nan rate: ",
        df_merge["historical_url"].isnull().sum() / len(df_merge["historical_url"]),
    )
    # print dead rate
    print(
        "dead rate: ",
        len(df_merge[df_merge["historical_url"] == "DEAD"])
        / len(df_merge["historical_url"]),
    )
    # print alive rate
    print("alive rate: ", len(df_merge.dropna()) / len(df_merge["historical_url"]))
    # print refused rate
    print(
        "refused rate: ",
        len(df_merge[df_merge["historical_url"] == "REFUSED"])
        / len(df_merge["historical_url"]),
    )

    df_merge.to_csv(
        f"government_websites_page_rank_{year}_top_500_historical_url.csv", index=False
    )
    return df_merge


if __name__ == "__main__":
    df_merge = get_dataset(args.year)
