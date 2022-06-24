import pandas as pd
import tldextract
import glob

type = "edu"
track_type = "all"
files = sorted(
    glob.glob("dataset_archive/{}_archive_ali_{}*.csv".format(type, track_type))
)
print(files)


def get_register_domain(row):
    url = row["url"]
    url = url.split("//")[-1]
    domain = tldextract.extract(url).domain
    suffix = tldextract.extract(url).suffix
    return ".".join([domain, suffix])


# noted that this domain is different from the domain in df_final.csv file. So we should
# handle this later
df_control = pd.read_csv(f"dataset_archive/control_archive_ali_{track_type}_2021.csv")
df_control["control_domain"] = df_control.apply(get_register_domain, axis=1)

df_edu = pd.read_csv(f"dataset_archive/edu_archive_ali_{track_type}_2021.csv")
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


for file in files:
    year = file.split("_")[-1][:4]
    df = pd.read_csv(file)
    if type == "edu":
        edu[f"tracker_{year}"] = df["3p-domain"]
        edu_tracker_count[f"tracker_{year}"] = df["3p-domain"].apply(count_trackers)
    else:
        control[f"tracker_{year}"] = df["3p-domain"]
        control_tracker_count[f"tracker_{year}"] = df["3p-domain"].apply(count_trackers)

if type == "edu":

    edu.to_csv("dataset_archive/frame_edu.csv", index=None, sep="\t")
    edu_tracker_count.to_csv(
        "dataset_archive/frame_edu_count.csv", index=None, sep="\t"
    )
else:
    control.to_csv("dataset_archive/frame_control.csv", index=None, sep="\t")
    control_tracker_count.to_csv(
        "dataset_archive/frame_control_count.csv", index=None, sep="\t"
    )
# frame_201301_edu_origin = frame_construct("201301")
# print(frame_201301_edu_origin.head())
