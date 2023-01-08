# 20230101 add collecting the dataset from common crawl and store the zip file in local machine.

import pandas as pd
import sys
from os import path
import time

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from pipeline_archived.utils import collect_dataset_from_url
from warc_module.warc_utils import (
    process_warc_froms3,
    get_text_selectolax,
    download_warc_froms3,
)
from warcio import WARCWriter


def downzip_file_from_CC():
    df = pd.read_csv("RQX/2015_500_scanning_result.csv")

    # try:

    for e, item in df.iterrows():
        url_host_name = item["url_host_name"]
        warc_filename = item["warc_filename"]
        offset = item["warc_record_offset"]
        length = item["warc_record_length"]
        text = download_warc_froms3(warc_filename, offset=offset, length=length)
        print(e)
        if text is not None:
            output = f"RQX/CC2015/{url_host_name}.warc"
            fout = open(output, "w")

            fout.write(text)
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


downzip_file_from_CC()
