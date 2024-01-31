import sys
from functools import partial
import json
import pandas as pd
from tqdm import tqdm
import argparse
from warc_module.warc_utils import get_text_selectolax, process_warc_froms3
import time

tqdm.pandas()
import multiprocessing as mp

print("Number of processors: ", mp.cpu_count())

# pandarallel.initialize(progress_bar=True)

argparser = argparse.ArgumentParser()
argparser.add_argument(
    "--year",
    type=int,
    default=2016,
    help="year of the dataset",
)
argparser.add_argument(
    "--skiprows",
    type=int,
    default=0,
    help="skiprows of the dataset",
)
argparser.add_argument(
    "--input_path",
    type=str,
    default="resource/england.csv",
    help="input path of the dataset",
)
argparser.add_argument(
    "--output_dir",
    type=str,
    default="test.txt",
    help="output path of the dataset",
)

argparser.add_argument(
    "--num_process",
    type=int,
    default=1,
    help="number of processes",
)
argparser.add_argument(
    "--multi_process",
    action="store_true",
    help="multi process collecting",
)
argparser.add_argument(
    "--unit_test",
    action="store_true",
    help="unit test",
)
argparser.add_argument(
    "--single_process",
    type=bool,
    default=False,
    help="single process collecting",
)


args = argparser.parse_args()


def collect_trackers_from_cc(row):
    warc_record_offset = row["offset"]
    warc_record_length = row["length"]
    warc_filename = row["cc_path"]
    offset = warc_record_offset
    length = warc_record_length
    url, trackers = process_warc_froms3(
        warc_filename, offset=offset, length=length, parser=get_text_selectolax
    )

    return trackers


def unit_test(args):
    url, trackers = process_warc_froms3(
        "crawl-data/CC-MAIN-2015-14/segments/1427131298015.2/warc/CC-MAIN-20150323172138-00061-ip-10-168-14-71.ec2.internal.warc.gz",
        offset=882944732,
        length=10489,
        parser=get_text_selectolax,
    )
    print(trackers)


def single_process(args):
    fout = open(f"{args.output_dir}", "w", encoding="utf-8")
    df = pd.read_csv("resource/england.csv")[
        [
            "url_host_name",
            "warc_filename",
            "year",
            "warc_record_offset",
            "warc_record_length",
        ]
    ]

    def collect_trackers_from_map_cc(row, fout):
        try:
            url_host_name = row["url_host_name"]
            warc_filename = row["warc_filename"]
            offset = row["warc_record_offset"]
            length = row["warc_record_length"]
            url, trackers = process_warc_froms3(
                warc_filename,
                offset=offset,
                length=length,
                parser=get_text_selectolax,
            )
            line = url_host_name + "\t" + trackers + "\n"
            fout.write(line)
            fout.flush()
            return trackers
        except Exception as e:
            print(e)

    df.progress_apply(collect_trackers_from_map_cc, axis=1, args=(fout,))


if __name__ == "__main__":
    time_start = time.time()

    if args.single_process:
        single_process(args)
    elif args.unit_test:
        unit_test(args)
    elif args.multi_process:
        fout = open(f"{args.output_dir}", "a", encoding="utf-8")

        def collect_trackers_from_map_cc(row):
            try:
                url_host_name = row["url_host_name"]
                warc_filename = row["warc_filename"]
                offset = row["warc_record_offset"]
                length = row["warc_record_length"]
                url, trackers = process_warc_froms3(
                    warc_filename,
                    offset=offset,
                    length=length,
                    parser=get_text_selectolax,
                )
                write_json = json.dumps(
                    {"url_host_name": url_host_name, "trackers": trackers}
                )
                # line = url_host_name + "\t" + trackers + "\n"
                # print(line)
                fout.write(write_json + "\n")
                fout.flush()
            except Exception as e:
                print(e)

        df = pd.read_csv(f"{args.input_path}", skiprows=args.skiprows)

        v = json.loads(df.to_json(orient="records"))
        pool = mp.Pool(args.num_process)

        # pool.map(collect_trackers_from_map_cc, list(v))
        for _ in tqdm(
            pool.imap_unordered(collect_trackers_from_map_cc, v), total=len(df)
        ):
            pass
        pool.close()
        pool.join()
        print(f"total time: {time.time()-time_start}")
