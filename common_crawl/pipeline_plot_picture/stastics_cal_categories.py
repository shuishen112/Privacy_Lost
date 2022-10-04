# claculate the dataset we will used in figure one and two.
# use the dataset in 2021
import pandas as pd
import tldextract
from collections import Counter
import sys
from os import path
import argparse

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from config import args

tracker_type = args["tracker_type"]
element_type = args["element_type"]

parser = argparse.ArgumentParser()
parser.add_argument(
    "--year",
    default="2012",
    help="collecting year",
)

args_command = parser.parse_args()
year = args_command.year

if element_type == "exclude":

    control_df = pd.read_csv(
        f"dataset_archive/control_archive_ali_exclude_{tracker_type}_{year}.csv"
    )
    edu_df = pd.read_csv(
        f"dataset_archive/edu_archive_ali_exclude_{tracker_type}_{year}.csv"
    )
else:
    control_df = pd.read_csv(
        f"dataset_archive/control_archive_ali_{tracker_type}_{year}.csv"
    )
    edu_df = pd.read_csv(f"dataset_archive/edu_archive_ali_{tracker_type}_{year}.csv")


def get_whotracksme():
    df = pd.read_csv("resource/whotracksme_trackers.txt", names=["domain"])
    trackers = df["domain"].to_list()
    trackers = set(list(map(lambda x: tldextract.extract(x).domain, trackers)))
    return trackers


def get_domain(row):
    return tldextract.extract(row["tracker_domain"]).domain


if tracker_type == "who":
    trackers = get_whotracksme()
else:
    raise Exception("wrong tracker type")

df_trackers_whotracksme = pd.read_csv(
    "resource/new_trackerList_with_categories.txt",
    sep="\t",
    names=["tracker_domain", "tracker_name", "category", "company"],
)
# note that the categoires are from whotracksme list
categories = set(df_trackers_whotracksme["category"].to_list())
company = set(df_trackers_whotracksme["company"].to_list())

print("len company", len(company))
print("len categories", len(categories))

df_trackers_whotracksme["tld"] = df_trackers_whotracksme["tracker_domain"].apply(
    lambda x: tldextract.extract(x).domain
)

tld_category = dict(
    zip(
        df_trackers_whotracksme["tld"].to_list(),
        df_trackers_whotracksme["category"].to_list(),
    )
)
tld_company = dict(
    zip(
        df_trackers_whotracksme["tld"].to_list(),
        df_trackers_whotracksme["company"].to_list(),
    )
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


def company_count(company, web_list):
    company_dict = Counter()
    try:
        for s in company:
            for e, l in enumerate(web_list):
                if l != l:
                    continue
                company_list = []
                for ll in l:
                    if ll in tld_company:
                        company_list.append(tld_company[ll])
                # company_list = list(map(lambda x: tld_company[x], l))
                # print(company_list)
                if s in company_list:
                    if s in company_dict:
                        company_dict[s] += 1
                    else:
                        company_dict[s] = 1

    except Exception as e:
        print(e)
    return company_dict


def categories_count(categories, web_list):
    categories_dict = Counter()
    try:
        for s in categories:
            for e, l in enumerate(web_list):
                if l != l:
                    continue
                categories_list = []
                for ll in l:
                    if ll in tld_category:
                        categories_list.append(tld_category[ll])
                # categories_list = list(map(lambda x: tld_categories[x], l))
                # print(categories_list)
                if s in categories_list:
                    if s in categories_dict:
                        categories_dict[s] += 1
                    else:
                        categories_dict[s] = 1

    except Exception as e:
        print(e)
    return categories_dict


def get_trackers_count(frame):
    """
    获得每个frame中trackers的字典
        :param frame:
    """
    frame["trackers_list"] = frame["3p-domain"].str.split(",")
    t_count = trackers_count(trackers, frame["trackers_list"])
    c_count = company_count(company, frame["trackers_list"])
    category_count = categories_count(categories, frame["trackers_list"])

    return t_count, c_count, category_count


trackers_count_edu, company_count_edu, category_count_edu = get_trackers_count(edu_df)
trackers_count_base, company_count_base, category_count_base = get_trackers_count(
    control_df
)


def trackers_crawl_rate(trackers, list_len):
    # print("len df is:{}".format(list_len))
    tracker, tracker_count = zip(*trackers)
    tracker_rate = list(map(lambda x: x / list_len, tracker_count))

    df = pd.DataFrame({"tracker": tracker, "tracker_rate": tracker_rate})
    return df


# companys, c_count = zip(*company_count_edu.most_common(1000))
# companys_rate = list(map(lambda x: x / len(edu_df), c_count))
# df_company_edu = pd.DataFrame({"company": companys, "rate": companys_rate})

# companys, c_count = zip(*company_count_base.most_common(1000))
# companys_rate = list(map(lambda x: x / len(control_df), c_count))
# df_company_base = pd.DataFrame({"company": companys, "rate": companys_rate})

# df_company_rate = df_company_edu.merge(df_company_base, on="company", how="outer")
# df_company_rate.columns = ["company", "edu", "no-edu"]
# df_company_rate.to_csv(
#     f"dataset_archive/df_company_rate_compare_{tracker_type}{element_type}.csv",
#     index=None,
# )

categorys, c_count = zip(*category_count_edu.most_common(1000))
categorys_rate = list(map(lambda x: x / len(edu_df), c_count))
df_category_edu = pd.DataFrame({"category": categorys, "rate": categorys_rate})

categorys, c_count = zip(*category_count_base.most_common(1000))
categorys_rate = list(map(lambda x: x / len(control_df), c_count))
df_category_base = pd.DataFrame({"category": categorys, "rate": categorys_rate})

df_category_rate = df_category_edu.merge(df_category_base, on="category", how="outer")
df_category_rate.columns = ["category", "edu", "no-edu"]
df_category_rate.to_csv(
    f"dataset_archive/df_category_rate_compare_{tracker_type}{element_type}_{year}.csv",
    index=None,
)

# df_trackers_rate_edu = trackers_crawl_rate(
#     trackers_count_edu.most_common(1000), len(edu_df)
# )
# df_trackers_rate_base = trackers_crawl_rate(
#     trackers_count_base.most_common(1000), len(control_df)
# )

# df_trackers_rate = df_trackers_rate_edu.merge(
#     df_trackers_rate_base, on="tracker", how="outer"
# )
# print(df_trackers_rate.head())
# df_trackers_rate.columns = ["tracker", "edu", "no-edu"]
# df_trackers_rate.to_csv(
#     f"dataset_archive/tracker_edu_non_edu_compare_{tracker_type}{element_type}.csv",
#     index=None,
# )
