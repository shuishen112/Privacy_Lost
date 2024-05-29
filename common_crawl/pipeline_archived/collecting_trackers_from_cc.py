import sys
from functools import partial
import json
import pandas as pd
from tqdm import tqdm
import argparse
from warc_module.warc_utils import get_text_selectolax, process_warc_froms3
import time
import wandb

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
    action="store_true",
    help="single process collecting",
)

argparser.add_argument(
    "--wandb",
    action="store_true",
    help="wandb usage",
)

argparser.add_argument(
    "--get_description",
    action="store_true",
    help="get description",
)

argparser.add_argument(
    "--outgoing_link",
    action="store_true",
    help="get outgoing link",
)

argparser.add_argument(
    "--group",
    type=str,
    default="CC",
    help="group name",
)

argparser.add_argument(
    "--time_stamp",
    action="store_true",
    help="time stamp",
)

args = argparser.parse_args()

if args.wandb:
    run = wandb.init(
        project="communication_conference",
        group=args.group,
        job_type=f"collect_historical_trackers{args.year}",
        config={
            "year": args.year,
        },
    )


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
    example = process_warc_froms3(
        "crawl-data/CC-MAIN-2013-20/segments/1368711005985/warc/CC-MAIN-20130516133005-00010-ip-10-60-113-184.ec2.internal.warc.gz",
        offset=634421693,
        length=26133,
        parser=get_text_selectolax,
        get_description=True,
        outgoing_link=True,
    )

    with open("unit_test.json", "w", encoding="utf-8") as fout:
        fout.write(
            json.dumps(
                {
                    "hostname": "test",
                    "trackers": example.trackers,
                    "outgoing_links": example.outgoing_links,
                    "descriptions": example.descriptions,
                },
                ensure_ascii=False,
            )
        )
    print(example.trackers)
    print("description:", example.descriptions)


def single_process(args):
    fout = open(f"{args.output_dir}", "w", encoding="utf-8")
    df = pd.read_csv(f"{args.input_path}", skiprows=args.skiprows)

    def collect_trackers_from_map_cc(row, fout):
        try:
            url_host_name = row["url_host_name"]
            warc_filename = row["warc_filename"]
            offset = row["warc_record_offset"]
            length = row["warc_record_length"]
            example = process_warc_froms3(
                warc_filename,
                offset=offset,
                length=length,
                parser=get_text_selectolax,
                get_description=args.get_description,
            )

            fout.write(
                json.dumps(
                    {
                        "hostname": url_host_name,
                        "trackers": example.trackers,
                        "descriptions": example.descriptions,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
            fout.flush()
        except Exception as e:
            print(e)

    df.progress_apply(collect_trackers_from_map_cc, axis=1, args=(fout,))


def collect_trackers_from_map_cc(row):
    try:
        url_host_name = row["url_host_name"]
        warc_filename = row["warc_filename"]
        offset = row["warc_record_offset"]
        length = row["warc_record_length"]
        time_stamp = int(row["warc_filename"].split("/")[5].split("-")[2][2:10])
        example = process_warc_froms3(
            warc_filename,
            offset=offset,
            length=length,
            parser=get_text_selectolax,
            get_description=args.get_description,
            outgoing_link=args.outgoing_link,
        )

        return (
            json.dumps(
                {
                    "hostname": url_host_name,
                    "trackers": example.trackers,
                    "time_stamp": time_stamp,
                    "outgoing_links": example.outgoing_links,
                    "descriptions": example.descriptions,
                },
                ensure_ascii=False,
            )
            + "\n"
        )

    except Exception as e:
        print("error is in collect_trackers_from_map_cc", e)


if __name__ == "__main__":
    time_start = time.time()

    if args.single_process:
        single_process(args)
    elif args.unit_test:
        unit_test(args)
    elif args.multi_process:
        fout = open(f"{args.output_dir}", "w", encoding="utf-8")

        df = pd.read_csv(f"{args.input_path}", skiprows=args.skiprows)

        v = json.loads(df.to_json(orient="records"))
        pool = mp.Pool(args.num_process)

        # pool.map(collect_trackers_from_map_cc, list(v))
        for i, result in enumerate(
            tqdm(pool.imap_unordered(collect_trackers_from_map_cc, v), total=len(df))
        ):
            if args.wandb:
                wandb.log({"progress": i + 1})
            fout.write(result)
            fout.flush()
        pool.close()
        pool.join()
        if args.wandb:
            run.finish()
        print(f"total time: {time.time()-time_start}")
