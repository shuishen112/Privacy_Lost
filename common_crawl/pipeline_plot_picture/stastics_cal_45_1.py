import pandas as pd
import tldextract
import glob
import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from config import args

tracker_type = args["tracker_type"]
element_type = args["element_type"]


def get_register_domain(row):
    url = row["url"].split("//")[-1]
    if url == '':
        url = row["url"].split("//")[-2]
    domain = tldextract.extract(url).domain
    suffix = tldextract.extract(url).suffix
    subdomain = tldextract.extract(url).subdomain
    if subdomain == '':
        name = ".".join([domain, suffix])
    else:
        name = ".".join([subdomain,domain, suffix])
    name = name.replace("www.",'')
    return name


# noted that this domain is different from the domain in df_final.csv file. So we should
# handle this later
df_control = pd.read_csv(f"dataset_archive/control_archive_ali_{tracker_type}_2021.csv")
df_control["control_domain"] = df_control.apply(get_register_domain, axis=1)

df_edu = pd.read_csv(f"dataset_archive/edu_archive_ali_{tracker_type}_2021.csv")
df_edu["edu_domain"] = df_edu.apply(get_register_domain, axis=1)


edu = pd.DataFrame(df_edu["edu_domain"])
edu_tracker_count = pd.DataFrame(df_edu["edu_domain"])

control = pd.DataFrame(df_control["control_domain"])
control_tracker_count = pd.DataFrame(df_control["control_domain"])


def count_trackers(row):
    # if row is nan, return 0
    if row != row:
        return 0
    return len(row.split(","))


def cal_experiment_45(files, type):
    # to compute the number in experiment 4 and 5

    for file in files:
        year = file.split("_")[-1][:4]
        df = pd.read_csv(file)
        if type == "edu":
            edu[f"tracker_{year}"] = df["3p-domain"]
            edu_tracker_count[f"tracker_{year}"] = df["3p-domain"].apply(count_trackers)
        else:
            control[f"tracker_{year}"] = df["3p-domain"]
            control_tracker_count[f"tracker_{year}"] = df["3p-domain"].apply(
                count_trackers
            )

    if type == "edu":

        edu.to_csv(
            f"dataset_archive/frame_edu_{tracker_type}{element_type}.csv",
            index=None,
            sep="\t",
        )
        edu_tracker_count.to_csv(
            f"dataset_archive/frame_edu_count_{tracker_type}{element_type}.csv",
            index=None,
            sep="\t",
        )
    else:
        control.to_csv(
            f"dataset_archive/frame_control_{tracker_type}{element_type}.csv",
            index=None,
            sep="\t",
        )
        control_tracker_count.to_csv(
            f"dataset_archive/frame_control_count_{tracker_type}{element_type}.csv",
            index=None,
            sep="\t",
        )


if element_type == "exclude":
    edu_files = sorted(
        glob.glob(
            "dataset_archive/{}_archive_ali_exclude_{}*.csv".format("edu", tracker_type)
        )
    )
    control_files = sorted(
        glob.glob(
            "dataset_archive/{}_archive_ali_exclude_{}*.csv".format(
                "control", tracker_type
            )
        )
    )
else:
    edu_files = sorted(
        glob.glob("dataset_archive/{}_archive_ali_{}*.csv".format("edu", tracker_type))
    )
    control_files = sorted(
        glob.glob(
            "dataset_archive/{}_archive_ali_{}*.csv".format("control", tracker_type)
        )
    )
cal_experiment_45(edu_files, "edu")
cal_experiment_45(control_files, "control")
