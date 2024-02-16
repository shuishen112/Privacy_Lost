# this file is to process the data from the government website
# we want to create the node embedding dataset for COPA

import pandas as pd
import argparse

argparser = argparse.ArgumentParser()
argparser.add_argument("--country", type=str, default="DK")

argparser.add_argument(
    "--snapshot_output_dir",
    type=str,
    default="archived_trackers_to_node_embedding/DK_gov_snapshot.csv",
)

argparser.add_argument(
    "--trackers_input_dir",
    type=str,
    default="archived_trackers_to_node_embedding/DK_gov_trackers.json",
)

argparser.add_argument(
    "--node_embedding_output_dir",
    type=str,
    default="archived_trackers_to_node_embedding/DK_node_emb.csv",
)

argparser.add_argument(
    "--get_snapshot",
    action="store_true",
    help="get snapshot",
)

argparser.add_argument(
    "--get_node_embedding",
    action="store_true",
    help="get node embedding",
)


args = argparser.parse_args()


def get_trackers(row):
    if isinstance(row["trackers"], str):
        tracker_list = row["trackers"].split(",")
        return set(tracker_list)
    else:
        return "NAN"


def get_node_embedding():
    df = pd.read_json(args.trackers_input_dir, orient="records", lines=True)
    df["trackers_"] = df.apply(get_trackers, axis=1)
    df_explode = df.explode("trackers_")
    dict_websites = {item: e + 1 for e, item in enumerate(df["url_host_name"].unique())}
    dict_trackers = {
        item: e + 1 for e, item in enumerate(df_explode["trackers_"].unique())
    }
    print(len(dict_websites)), print(len(dict_trackers))

    df_explode["website_id"] = df_explode["url_host_name"].map(dict_websites)
    df_explode["tracker_id"] = df_explode["trackers_"].map(dict_trackers)

    df_explode = df_explode.sort_values(by=["year"])

    df_explode[["website_id", "tracker_id", "year"]].to_csv(
        args.node_embedding_output_dir, index=False, header=False
    )


def get_merge_data(year, country="ALL"):
    df = pd.read_csv("seed_urls/dataset_for_amazon.csv")

    df["url_host_registered_domain"] = (
        df["hostnames"].str.split(".").str[-2]
        + "."
        + df["hostnames"].str.split(".").str[-1]
    )

    df_cc = pd.read_csv(f"CC/{year}_gov.csv")
    df_cc.head()

    df_cc_merge = pd.merge(
        df, df_cc, how="right", left_on="hostnames", right_on="url_host_name"
    )

    df_cc_merge = df_cc_merge.dropna()
    if country != "ALL":
        df_cc_merge = df_cc_merge[df_cc_merge["Alpha-2"] == country]
    return df_cc_merge


def get_all_year():
    df_all = pd.DataFrame()
    for year in range(2013, 2024):

        df = get_merge_data(year, args.country)
        df["year"] = year
        df_all = pd.concat([df_all, df])
        print(df_all)
    df_all.to_csv(args.snapshot_output_dir, index=False)


if __name__ == "__main__":
    if args.get_snapshot:
        get_all_year()
    if args.get_node_embedding:
        get_node_embedding()
# merge all the year
