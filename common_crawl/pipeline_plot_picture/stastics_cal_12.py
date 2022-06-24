# claculate the dataset we will used in figure one and two.
# use the dataset in 2021
import pandas as pd
import tldextract
from collections import Counter

control_df = pd.read_csv("dataset_archive/control_archive_ali_all_2021.csv")
edu_df = pd.read_csv("dataset_archive/edu_archive_ali_all_2021.csv")

# 获得所有的trackers
df_domain = pd.read_csv(
    "resource/labeled-thirdparties.csv",
    sep="\t",
    names="Domain,Registration_org,Registration_country,num_embeddings,num_embeddings_javascript,num_embeddings_iframe,num_embeddings_image,num_embeddings_link,Category,Company".split(
        ","
    ),
)


def get_all_tracker():
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


# trackers = get_tracker(df_domain)
trackers = get_all_tracker()


service_type = set(df_domain["Category"].to_list())
company = set(df_domain["Company"].to_list())

print("len company", len(company))
print("len service_type", len(service_type))


df_domain["tld"] = df_domain["Domain"].apply(lambda x: tldextract.extract(x).domain)

tld_category = dict(zip(df_domain["tld"].to_list(), df_domain["Category"].to_list()))
tld_company = dict(zip(df_domain["tld"].to_list(), df_domain["Company"].to_list()))


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


def get_trackers_count(frame):
    """
    获得每个frame中trackers的字典
        :param frame:
    """
    frame["trackers_list"] = frame["3p-domain"].str.split(",")
    t_count = trackers_count(trackers, frame["trackers_list"])
    c_count = company_count(company, frame["trackers_list"])

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
df_company_rate.to_csv("dataset_archive/df_company_rate_compare_all.csv", index=None)


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
    "dataset_archive/tracker_edu_non_edu_compare_all.csv", index=None
)
