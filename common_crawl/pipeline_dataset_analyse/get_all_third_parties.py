# this file is to get all the third-parties from html file

import glob
import pandas as pd
import tldextract
from collections import Counter

import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from config import args

task_type = args["task_type"]
element_type = args["element_type"]


edu_trackers = []
control_trackers = []
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


def get_trackers(row, trackers):
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
def generate_all_3p(files, task_type, element_type):
    for file in files:
        df = pd.read_csv(file)
        if task_type == "edu":
            df["trackers"] = df.apply(get_trackers, trackers=edu_trackers, axis=1)
        else:
            df["trackers"] = df.apply(get_trackers, trackers=control_trackers, axis=1)
    if task_type == "edu":
        tracker_count = Counter(edu_trackers)
    else:
        tracker_count = Counter(control_trackers)

    print(tracker_count.most_common(100))

    keys, values = zip(*tracker_count.most_common(100))
    df_tracker = pd.DataFrame({"trackers": keys, "frequency": values})

    df_tracker.to_csv(f"resource/all_trackers{task_type}{element_type}.csv", index=None)
    # print(tracker_count.keys())


if element_type != "exclude":
    edu_files = glob.glob(f"dataset_archive/edu_archive_ali_all*.csv")
    generate_all_3p(edu_files, "edu", element_type)
    control_files = glob.glob(f"dataset_archive/control_archive_ali_all*.csv")
    generate_all_3p(control_files, "control", element_type)
else:
    edu_files = glob.glob(f"dataset_archive/edu_archive_ali_exclude_all*.csv")
    generate_all_3p(edu_files, "edu", element_type)
    control_files = glob.glob(f"dataset_archive/control_archive_ali_exclude_all*.csv")
    generate_all_3p(control_files, "control", element_type)
