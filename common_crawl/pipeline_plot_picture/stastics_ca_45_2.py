import pandas as pd
import tldextract
from tqdm import tqdm

import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from config import args

tracker_type = args["tracker_type"]
element_type = args["element_type"]
frame_base = pd.read_csv(
    f"dataset_archive/frame_control_{tracker_type}{element_type}.csv", sep="\t"
)
frame_edu = pd.read_csv(
    f"dataset_archive/frame_edu_{tracker_type}{element_type}.csv", sep="\t"
)

# frame_base = frame_base.sample(n=500)
# frame_edu = frame_edu.sample(n=500)

# rename the columns of dataframe
x_base = [
    "2012",
    "2013",
    "2014",
    "2015",
    "2016",
    "2017",
    "2018",
    "2019",
    "2020",
    "2021",
]


def get_trackers_distribution(frame_edu):
    frame_edu.columns = ["url"] + x_base
    frame_list_edu = [frame_edu[["url", year]] for year in x_base]

    for item in frame_list_edu:
        item.columns = ["url", "trackers"]

    def count_trackers(row):
        # if row is nan, return 0
        if row != row:
            return 0
        return len(row.split(","))

    trackers_count_list = []

    for frame, year in zip(frame_list_edu, x_base):
        df_year = pd.DataFrame({"year": [year] * len(frame_edu)})
        df_temp = pd.concat([frame["trackers"].apply(count_trackers), df_year], axis=1)
        trackers_count_list.append(df_temp)
    df_count_all_year = pd.concat(trackers_count_list)
    return df_count_all_year


def get_frame_list(frame_edu):
    """split the edu_frame to several years

    Args:
        frame_edu (df): edu or non-edu frame

    Returns:
        _type_: a list of frame
    """
    frame_edu.columns = ["url"] + x_base
    frame_list_edu = [frame_edu[["url", year]] for year in x_base]

    for item in frame_list_edu:
        item.columns = ["url", "trackers"]
    return frame_list_edu


def plot_get_df_count_all_year():
    df_count_all_year_edu = get_trackers_distribution(frame_edu)
    df_count_all_year_base = get_trackers_distribution(frame_base)
    return df_count_all_year_edu, df_count_all_year_base


def get_trackers_count_average(frame_edu):
    """get number of frame in each year

    Args:
        frame_edu (df): df frame

    Returns:
        _type_: a list of average number
    """
    frame_edu.columns = ["url"] + x_base
    frame_list_edu = [frame_edu[["url", year]] for year in x_base]

    for item in frame_list_edu:
        item.columns = ["url", "trackers"]

    def count_trackers(row):
        # if row is nan, return 0
        if row != row:
            return 0
        return len(row.split(","))

    trackers_std_edu = [
        frame["trackers"].apply(count_trackers).std() for frame in frame_list_edu
    ]
    trackers_average_edu = [
        frame["trackers"].apply(count_trackers).mean() for frame in frame_list_edu
    ]

    return trackers_std_edu, trackers_average_edu


def plot_get_trackers_average():
    trackers_average_edu = get_trackers_count_average(frame_edu)
    trackers_average_base = get_trackers_count_average(frame_base)

    return trackers_average_edu, trackers_average_base


########################
df_domain_third_party = pd.read_csv(
    "resource/labeled-thirdparties.csv",
    sep="\t",
    names=[
        "domain",
        "registration_org",
        "registration_country",
        "num_embeddings",
        "num_embeddings_javascript",
        "num_embeddings_iframe",
        "num_embeddings_image",
        "num_embeddings_link",
        "category",
        "company",
    ],
)

# df_domain = pd.read_csv("trackers_domain.csv")
df_domain = df_domain_third_party


# df_domain = df_domain.merge(df_domain_third_party,how = "left",on = "domain")[['domain',"registration_country","category"]]
print(df_domain.head())

from collections import Counter

trackers_list = df_domain["domain"].to_list()
trackers_list = set(
    list(map(lambda x: tldextract.extract(x).domain, df_domain["domain"].to_list()))
)


def trackers_count(trackers, web_list):
    """calculate the trackers which occur in how many websites.

    Args:
        trackers (list): all the trackers
        web_list (df): websites in a specific year

    Returns:
        Counter: IDF of the trackers
    """
    trackers_count_dict = Counter()
    try:
        for t in trackers:
            for l in web_list:
                if l != l:
                    continue
                if t in l:
                    if t in trackers_count_dict:
                        trackers_count_dict[t] += 1
                    else:
                        trackers_count_dict[t] = 1
    except Exception as e:
        print(e)

    return trackers_count_dict


def get_trackers_count(frame):
    """
    get the tracker dict for each frame
        :param frame:
    """
    frame["trackers_list"] = frame["trackers"].str.split(",")
    trackers_count_frame = trackers_count(trackers_list, frame["trackers_list"])

    return trackers_count_frame


# 计算比例
def trackers_crawl_rate(trackers, list_len):
    # print("len df is:{}".format(list_len))
    tracker, tracker_count = zip(*trackers)
    tracker_rate = list(map(lambda x: x[1] / list_len, trackers))

    df = pd.DataFrame({"tracker": tracker, "tracker_rate": tracker_rate})
    return df


def trackers_rate_dict(trackers, list_len):
    d = {key: trackers[key] / list_len for key in trackers}
    return d


def plot_get_rate_over_year():

    frame_list_edu = get_frame_list(frame_edu)
    df_trackers_frame_list_edu = []
    for frame in tqdm(frame_list_edu):
        df_trackers_count = get_trackers_count(frame)
        df_trackers_frame = trackers_crawl_rate(
            df_trackers_count.most_common(1000), len(frame)
        )
        df_trackers_frame_list_edu.append(df_trackers_frame)

    frame_list_base = get_frame_list(frame_base)
    df_trackers_frame_list_base = []
    for frame in tqdm(frame_list_base):
        df_trackers_count = get_trackers_count(frame)
        df_trackers_frame = trackers_crawl_rate(
            df_trackers_count.most_common(1000), len(frame)
        )
        df_trackers_frame_list_base.append(df_trackers_frame)

    df_rate_merge_edu = df_trackers_frame_list_edu[0]
    for e, df in enumerate(df_trackers_frame_list_edu[1:]):
        df_rate_merge_edu = df_rate_merge_edu.merge(df, how="outer", on="tracker")

    df_rate_merge_base = df_trackers_frame_list_base[0]
    for e, df in enumerate(df_trackers_frame_list_base[1:]):
        df_rate_merge_base = df_rate_merge_base.merge(df, how="outer", on="tracker")

    df_rate_merge_edu.columns = ["trackers"] + x_base
    df_rate_merge_edu.to_csv(
        f"dataset_archive/df_rate_merge_edu_{tracker_type}{element_type}.csv",
        index=None,
    )
    # print(df_rate_merge_edu.head())

    df_rate_merge_base.columns = ["trackers"] + x_base
    df_rate_merge_base.to_csv(
        f"dataset_archive/df_rate_merge_base_{tracker_type}{element_type}.csv",
        index=None,
    )
    # print(df_rate_merge_base.head())


def KS_test_over_the_year():
    # KS-test
    from scipy import stats

    stat = []
    p_value = []

    trackers_average_base = pd.read_csv(
        "dataset_archive/frame_control_count.csv", sep="\t"
    )
    trackers_average_edu = pd.read_csv("dataset_archive/frame_edu_count.csv", sep="\t")

    names = trackers_average_base.columns
    for n in names[1:]:
        result = stats.ks_2samp(trackers_average_base[n], trackers_average_edu[n])
        stat.append(float("%.2g" % result.statistic))
        p_value.append(float("%.2g" % result.pvalue))

    df_result = pd.DataFrame({"year": names[1:], "statistic": stat, "pvalue": p_value})
    df_result.to_csv("pipeline_plot_picture/ks_test.csv", index=None, sep=",")


plot_get_rate_over_year()