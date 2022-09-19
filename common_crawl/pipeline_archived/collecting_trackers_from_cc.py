import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
import pandas as pd
from warc_module.warc_utils import process_warc_froms3, get_text_selectolax
from tqdm import tqdm
import glob

tqdm.pandas()
from pandarallel import pandarallel
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


def collect_trackers_from_map_cc(row):
    try:
        url_host_name = row[0]
        warc_filename = row[1]
        year = row[2]

        offset = row[3]
        length = row[4]
        url, trackers = process_warc_froms3(
            warc_filename, offset=offset, length=length, parser=get_text_selectolax
        )
        line = url_host_name + "\t" + str(year) + "\t" + trackers + "\n"
        # print(line)
        fout.write(line)
        fout.flush()
        return trackers
    except Exception as e:
        print(e)


# df["trackers"] = df.parallel_apply(
#     collect_trackers_from_cc, axis=1, result_type="expand"
# )
# df[["year", "url", "trackers"]].to_csv("dataset_cc/cc_dataset.csv")


files = glob.glob("dataset_cc/*.txt")
for e, f in enumerate(files):

    df = pd.read_csv(
        f,
        names=["url", "cc_path", "year", "offset", "length"],
    )

    file_name = f.split("_")[-1]

    fout = open(f"dataset_cc/cc_dataset_{file_name}", "w", encoding="utf-8")

    v = df.values

    # for vv in v:
    #     collect_trackers_from_map_cc(vv)

    pool = mp.Pool(mp.cpu_count())

    # pool.map(collect_trackers_from_map_cc, list(v))
    for _ in tqdm(
        pool.imap_unordered(collect_trackers_from_map_cc, list(v)), total=len(df)
    ):
        pass
    pool.close()
    pool.join()
