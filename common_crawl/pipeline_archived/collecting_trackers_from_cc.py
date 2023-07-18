import sys
from os import path


from functools import partial

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
import glob

import pandas as pd
from tqdm import tqdm

from warc_module.warc_utils import get_text_selectolax, process_warc_froms3

tqdm.pandas()
import multiprocessing as mp

print("Number of processors: ", mp.cpu_count())

# pandarallel.initialize(progress_bar=True)


def collect_trackers_from_cc(row):
    warc_record_offset = row["offset"]
    warc_record_length = row["length"]
    warc_filename = row["cc_path"]
    # year = row["year"]
    # url_host_name = row['url_host_name']
    offset = warc_record_offset
    length = warc_record_length
    url, trackers = process_warc_froms3(
        warc_filename, offset=offset, length=length, parser=get_text_selectolax
    )
    # line = offset + '\t' + warc_filename + '\t'
    # print(line)
    # fout.write(offset + '\t' + warc_filename + '\t')
    return trackers


fout = open(f"Journal/trackers_allurls_2018_left.txt", "a", encoding="utf-8")


def collect_trackers_from_map_cc(row):
    try:
        url_host_name = row[0]
        warc_filename = row[2]

        offset = row[3]
        length = row[4]
        url, trackers = process_warc_froms3(
            warc_filename,
            offset=offset,
            length=length,
            parser=get_text_selectolax,
        )
        line = url_host_name + "\t" + "\t" + trackers + "\n"
        # print(line)
        fout.write(line)
        fout.flush()
        return trackers
    except Exception as e:
        print(e)


collect_trackers_from_map_cc_parallel = partial(collect_trackers_from_map_cc, y=10)

# df_all = pd.read_csv("Journal/all_urls_2018.csv", chunksize=1000000, skiprows=3992111)
df = pd.read_csv("Journal/all_urls_2018.csv", skiprows=3992111)

# for e, df in enumerate(df_all):

#     print(f"chunk {e} begin ")

v = df.values

# for vv in v:
#     collect_trackers_from_map_cc(vv)

pool = mp.Pool(24)

# pool.map(collect_trackers_from_map_cc, list(v))
for _ in tqdm(
    pool.imap_unordered(collect_trackers_from_map_cc, list(v)), total=len(df)
):
    pass
pool.close()
pool.join()


# fout = open(f"dataset_cc/cc_dataset_england.txt", "w", encoding="utf-8")
# df = pd.read_csv("resource/england.csv")[
#     [
#         "url_host_name",
#         "warc_filename",
#         "year",
#         "warc_record_offset",
#         "warc_record_length",
#     ]
# ]
# v = df.values

# # for vv in v:
# #     collect_trackers_from_map_cc(vv)

# pool = mp.Pool(mp.cpu_count())

# # pool.map(collect_trackers_from_map_cc, list(v))
# for _ in tqdm(
#     pool.imap_unordered(collect_trackers_from_map_cc, list(v)), total=len(df)
# ):
#     pass
# pool.close()
# pool.join()
