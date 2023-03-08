# 20230101 add collecting the dataset from common crawl and store the zip file in local machine.

import pandas as pd
import sys
from os import path
import time
import os

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from pipeline_archived.utils import collect_dataset_from_url
from warc_module.warc_utils import (
    process_warc_froms3,
    get_text_selectolax,
    download_warc_froms3,
)
from tqdm import tqdm 
from warcio import WARCWriter


def downzip_file_from_CC():
    for year in range(2016, 2017):
        time_s = time.time()
        year = str(year)
        store_path = f"RQX/CC{year}"
        if not os.path.exists(store_path):
            os.makedirs(store_path)
        df = pd.read_csv(f"RQX/{year}_500_scanning_result.csv")
        list_dir = os.listdir(store_path)
        list_dir = [d[:-5] for d in list_dir]
        # try:

        for e, item in df.iterrows():

            url_host_name = item["url_host_name"]
            if url_host_name in list_dir:
                # it has been archived
                continue
            warc_filename = item["warc_filename"]
            offset = item["warc_record_offset"]
            length = item["warc_record_length"]
            text = download_warc_froms3(warc_filename, offset=offset, length=length)
            if text is not None:
                output = f"{store_path}/{url_host_name}.warc"
                fout = open(output, "w")

                fout.write(text)
        time_e = time.time()
        print("time consuming", time_e - time_s)
        # except Exception as e:
        #     print(e)


def extracting_trackers_from_common_crawl():
    df = pd.read_csv("RQX/2016_500_scanning_result.csv")

    fout = open("RQX/2016_500_extract_trackers_cc.csv", "w")
    for _, item in df.iterrows():

        url_host_name = item["url_host_name"]
        warc_filename = item["warc_filename"]
        offset = item["warc_record_offset"]
        length = item["warc_record_length"]
        url, trackers = process_warc_froms3(
            warc_filename, offset=offset, length=length, parser=get_text_selectolax
        )
        # fout.write(url + "\t" + trackers + "\n")
        # fout.flush()
        print(url, trackers)


############################################ collecting trackers from local machine in CC #######################
fout = open("RQX/10000sample_analysis/CC_2014_2019_trackers.csv", "w")
for year in range(2014, 2020):
    year = str(year)
    file_list = os.listdir(f"RQX/10000sample_analysis/CC_archive/{year}")

    for file in tqdm(file_list):
        file_name = file[:-5]
        text = open(
            os.path.join(f"RQX/10000sample_analysis/CC_archive/{year}", file)
        ).read()
        trackers = get_text_selectolax(text, source="cc")
        trackers = list(set(trackers))
        if trackers is not None:
            fout.write(year + "\t" + file_name + "\t" + ",".join(trackers) + "\n")
        else:
            fout.write(year + "\t" + file_name + "\n")
        fout.flush()


# downzip_file_from_CC()
# df = pd.read_csv("RQX/2015_500_scanning_result.csv")


# df_special = df[df.url_host_name == "www.ucoz.ro"]
# print(df_special)

# url, trackers = process_warc_froms3(
#     "crawl-data/CC-MAIN-2015-14/segments/1427131298015.2/warc/CC-MAIN-20150323172138-00061-ip-10-168-14-71.ec2.internal.warc.gz",
#     offset=882944732,
#     length=10489,
#     parser=get_text_selectolax,
# )

# print(trackers)
