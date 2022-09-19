# claculate the dataset we will used in figure one and two.
# use the dataset in 2021
import pandas as pd
import tldextract
from collections import Counter
import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from config import args

tracker_type = args["tracker_type"]
element_type = args["element_type"]

if element_type == "exclude":

    control_df = pd.read_csv(
        f"dataset_archive/control_archive_ali_exclude_{tracker_type}_2021.csv"
    )
    edu_df = pd.read_csv(
        f"dataset_archive/edu_archive_ali_exclude_{tracker_type}_2021.csv"
    )
else:
    control_df = pd.read_csv(
        f"dataset_archive/control_archive_ali_{tracker_type}_2021.csv"
    )
    edu_df = pd.read_csv(f"dataset_archive/edu_archive_ali_{tracker_type}_2021.csv")

# 获得所有的trackers
df_domain = pd.read_csv(
    "resource/labeled-thirdparties.csv",
    sep="\t",
    names="Domain,Registration_org,Registration_country,num_embeddings,num_embeddings_javascript,num_embeddings_iframe,num_embeddings_image,num_embeddings_link,Category,Company".split(
        ","
    ),
)


def get_all_tracker():
    if element_type == "exclude":
        df_all_trackers_edu = pd.read_csv("resource/all_trackerseduexclude.csv")
        df_all_trackers_control = pd.read_csv("resource/all_trackerscontrolexclude.csv")
    df_all_trackers_edu = pd.read_csv("resource/all_trackersedu.csv")
    df_all_trackers_control = pd.read_csv("resource/all_trackerscontrol.csv")
    trackers = set(
        df_all_trackers_edu["trackers"].to_list()
        + df_all_trackers_control["trackers"].to_list()
    )
    print(f"there are {len(trackers)} trackers in the dataset")
    return trackers


def get_tracker(df):

    # trackers category 类别
    trackers = df["Domain"].to_list()
    trackers = set(list(map(lambda x: tldextract.extract(x).domain, trackers)))

    return trackers


def get_whotracksme():
    df = pd.read_csv("resource/whotracksme_trackers.txt", names=["domain"])
    trackers = df["domain"].to_list()
    trackers = set(list(map(lambda x: tldextract.extract(x).domain, trackers)))
    return trackers


def get_domain(row):
    return tldextract.extract(row["tracker_domain"]).domain


def get_whotracksme_categories():
    df_trackers_categories = pd.read_csv(
        "resource/new_trackerList_with_categories.txt",
        sep="\t",
        names=["tracker_domain", "tracker_name", "category", "company"],
    )

    df_trackers_categories["tracker_domain"] = df_trackers_categories.apply(
        get_domain, axis=1
    )
    df_trackers_categories = df_trackers_categories.drop_duplicates()
    tld_categories = dict(
        zip(
            df_trackers_categories["tracker_domain"].to_list(),
            df_trackers_categories["category"].to_list(),
        )
    )
    return tld_categories


def get_intersection():
    df = pd.read_csv("resource/CommonListOfTrackers.txt", names=["domain"])
    trackers = df["domain"].to_list()
    trackers = set(list(map(lambda x: tldextract.extract(x).domain, trackers)))
    return trackers


if tracker_type == "all":
    trackers = get_all_tracker()
elif tracker_type == "1375":
    trackers = get_tracker(df_domain)
elif tracker_type == "who":
    trackers = get_whotracksme()
elif tracker_type == "inter":
    trackers = get_intersection()
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

df_domain["tld"] = df_domain["Domain"].apply(lambda x: tldextract.extract(x).domain)

tld_category = dict(zip(df_domain["tld"].to_list(), df_domain["Category"].to_list()))
tld_company = dict(zip(df_domain["tld"].to_list(), df_domain["Company"].to_list()))
tld_category = get_whotracksme_categories()


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
    cate_count = categories_count(categories, frame["trackers_list"])

    return t_count, c_count


trackers_count_edu, company_count_edu = get_trackers_count(edu_df)
trackers_count_base, company_count_base = get_trackers_count(control_df)


def trackers_crawl_rate(trackers, list_len):
    # print("len df is:{}".format(list_len))
    tracker, tracker_count = zip(*trackers)
    tracker_rate = list(map(lambda x: x / list_len, tracker_count))

    df = pd.DataFrame({"tracker": tracker, "tracker_rate": tracker_rate})
    return df


companys, c_count = zip(*company_count_edu.most_common(20))
companys_rate = list(map(lambda x: x / len(edu_df), c_count))
df_company_edu = pd.DataFrame({"company": companys, "services_rate": companys_rate})

companys, c_count = zip(*company_count_base.most_common(20))
companys_rate = list(map(lambda x: x / len(control_df), c_count))
df_company_base = pd.DataFrame({"company": companys, "services_rate": companys_rate})

df_company_rate = df_company_edu.merge(df_company_base, on="company", how="outer")
df_company_rate.columns = ["company", "edu", "no-edu"]
df_company_rate.to_csv(
    f"dataset_archive/df_company_rate_compare_{tracker_type}{element_type}.csv",
    index=None,
)


df_trackers_rate_edu = trackers_crawl_rate(
    trackers_count_edu.most_common(1000), len(edu_df)
)
df_trackers_rate_base = trackers_crawl_rate(
    trackers_count_base.most_common(1000), len(control_df)
)

df_trackers_rate = df_trackers_rate_edu.merge(
    df_trackers_rate_base, on="tracker", how="outer"
)
print(df_trackers_rate.head())
df_trackers_rate.columns = ["tracker", "edu", "no-edu"]
df_trackers_rate.to_csv(
    f"dataset_archive/tracker_edu_non_edu_compare_{tracker_type}{element_type}.csv",
    index=None,
)
