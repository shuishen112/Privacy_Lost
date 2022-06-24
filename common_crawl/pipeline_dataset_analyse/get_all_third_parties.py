# this file is to get all the third-parties from html file

import glob
import pandas as pd
import tldextract
from collections import Counter

task_type = "edu"
files = glob.glob(f"dataset_archive/{task_type}_archive_ali_all*.csv")

trackers = []

analytics = []


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


def get_trackers(row):
    domain = row["3p-domain"]
    url = row["url"]

    if domain != domain:
        return []
    else:
        trackers_list = domain.split(",")
        if "" in trackers_list:
            trackers_list.remove("")
        extract_domain = tldextract.extract(url).domain
        if extract_domain in trackers_list:
            trackers_list.remove(extract_domain)
        trackers.extend(trackers_list)
        return trackers_list


# df = pd.read_csv("dataset_archive/control_archive_ali_all_2021.csv")
# df["trackers"] = df.apply(get_trackers, axis=1)

for file in files:
    df = pd.read_csv(file)
    df["trackers"] = df.apply(get_trackers, axis=1)
tracker_count = Counter(trackers)
print(tracker_count.most_common(100))

keys, values = zip(*tracker_count.most_common(100))
df_tracker = pd.DataFrame({"trackers": keys, "frequency": values})

df_tracker.to_csv(f"resource/all_trackers{task_type}.csv", index=None)
# print(tracker_count.keys())
