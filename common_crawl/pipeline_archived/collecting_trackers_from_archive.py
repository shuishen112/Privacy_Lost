################################## get the tracker information ##########
import time
import glob
from tqdm import tqdm
import pandas as pd
import sys
from os import path
import logging
import os
import wandb
import multiprocessing as mp
import json
import argparse

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from warc_module.warc_utils import (
    get_text_selectolax,
    get_outer_link,
)
from utils import extract_trackers_from_internet_archive

parser = argparse.ArgumentParser()
parser.add_argument(
    "--output_path",
    type=str,
    default="debug/IA/GOV/",
    help="output path",
)
parser.add_argument(
    "--input_path",
    type=str,
    default="websci/scanning_websites/government_websites_page_rank_2023_top_500_historical_url.csv",
    help="input path",
)
parser.add_argument(
    "--year",
    type=str,
    default="2014",
    help="year",
)

parser.add_argument(
    "--skiprows",
    type=int,
    default=0,
    help=0,
)
parser.add_argument(
    "--unit_test",
    action="store_true",
    help="unit test",
)

parser.add_argument(
    "--wandb",
    action="store_true",
    help="wandb",
)

parser.add_argument(
    "--sleep_second",
    type=int,
    default=5,
    help="sleep_second",
)

parser.add_argument(
    "--multi_process",
    action="store_true",
    help="multi process",
)

parser.add_argument(
    "--num_process",
    type=int,
    default=1,
    help="number of processes",
)

parser.add_argument(
    "--zyte",
    action="store_true",
    help="zyte",
)

parser.add_argument(
    "--outgoing_link",
    action="store_true",
    help="outgoing_link",
)

args = parser.parse_args()


def collect_trackers_from_map_ia(row):
    try:
        hostname = row["hostname"]
        history_url = row["url"]
        # get historical url

        if history_url in ["NAN", "DEAD"]:
            return json.dumps({"hostname": hostname, "trackers": "EMPTY_URL"}) + "\n"
        fields = history_url.split("/")
        fields[4] = fields[4] + "id_"
        history_url = "/".join(fields)
        time.sleep(args.sleep_second)
        trackers, outgoing_links = extract_trackers_from_internet_archive(
            history_url,
            get_text_selectolax,
            if_wandb=args.wandb,
            using_zyte=args.zyte,
            outgoing_link=args.outgoing_link,
        )
        if args.outgoing_link and outgoing_links is not None:
            return (
                json.dumps(
                    {
                        "hostname": hostname,
                        "trackers": trackers,
                        "outgoing_links": outgoing_links,
                    }
                )
                + "\n"
            )
        elif trackers == []:
            return json.dumps({"hostname": hostname, "trackers": "NO_TRACKERS"}) + "\n"
        else:
            return json.dumps({"hostname": hostname, "trackers": trackers}) + "\n"
    except Exception as e:
        # print(history_url)
        print(e)


###### add log ######
logging.basicConfig(
    filename=f"all_tracker_{args.year}.log",
    level=logging.WARNING,
    format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
)
logger = logging.getLogger("webtracking")
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter("%(asctime)s|%(levelname)s|%(name)s|%(message)s")
)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

t = time.time()


################################### store the zip file from historical trackers from Internet Archive ###############################

# for year in range(2017, 2023):
#     year = str(year)
#     store_path = f"RQX/10000sample_analysis/IA_archive/{year}"
#     if not os.path.exists(store_path):
#         os.makedirs(store_path)
#     start_time = time.time()
#     # whether it has been archived
#     dir_list = os.listdir(store_path)
#     dir_list = [name[:-5] for name in dir_list]
#     df_sample_500 = pd.read_csv(
#         f"RQX/10000sample_analysis/IA/domain_historical_year_{year}.csv",
#         sep="\t",
#         names=["domain", "historical_url"],
#     ).dropna()
#     for e, item in df_sample_500.iterrows():

#         domain = item["domain"]
#         if domain in dir_list:
#             # it has been archived
#             continue
#         history_url = item["historical_url"]
#         text = download_dataset_from_url(history_url)
#         if text is not None:
#             output = f"{store_path}/{domain}.warc"
#             fout = open(output, "w")
#             fout.write(text)
#     end_time = time.time()
#     print("time consuming:{}".format(end_time - start_time))

#################################### collecting trackers from stored local machines #################################################

# fout = open("RQX/2015_500_extract_trackers_archive_internet.csv", "w")
# file_list = os.listdir("RQX/IA2015/")

# for file in tqdm(file_list):
#     file_name = file[:-5]
#     text = open(os.path.join("RQX/IA2015/", file)).read()
#     trackers = get_text_selectolax(text, source="ia")
#     trackers = list(set(trackers))
#     if trackers is not None:
#         fout.write(file_name + "\t" + ",".join(trackers) + "\n")
#     else:
#         fout.write(file_name + "\n")
#     fout.flush()

# fout = open("RQX/10000sample_analysis/IA_2014_2019_trackers.csv", "w")
# for year in range(2014, 2020):
#     year = str(year)
#     file_list = os.listdir(f"RQX/10000sample_analysis/IA_archive/{year}")

#     for file in tqdm(file_list):
#         file_name = file[:-5]
#         text = open(
#             os.path.join(f"RQX/10000sample_analysis/IA_archive/{year}", file)
#         ).read()
#         trackers = get_text_selectolax(text, source="ia")
#         trackers = list(set(trackers))
#         if trackers is not None:
#             fout.write(year + "\t" + file_name + "\t" + ",".join(trackers) + "\n")
#         else:
#             fout.write(year + "\t" + file_name + "\n")
#         fout.flush()


#################################### collecting from historical trackers from Internet Archive ###############################


# only test one historical snapshot
def test_archive():
    history_url = "https://web.archive.org/web/20230127201823/https://e-tender.ua/"
    fields = history_url.split("/")
    fields[4] = fields[4] + "id_"
    history_url = "/".join(fields)
    trackers, outgoing_links = extract_trackers_from_internet_archive(
        history_url,
        get_text_selectolax,
        using_zyte=True,
        outgoing_link=True,
    )
    print(f"len:{len(trackers)}", trackers)


def collecting_single_thread():
    df = pd.read_json(
        args.input_path,
        lines=True,
    )

    fout = open(args.output_path + f"/archived_websites_{args.year}.json", "w")
    for e, item in df.iterrows():
        if args.wandb:
            wandb.log({"progress": e, "total": len(df)})
        hostname = item["hostname"]
        history_url = item["url"]

        if history_url in ["NAN", "DEAD"]:
            fout.write(hostname + "\t" + "EMPTY_URL" + "\n")
            continue
        fields = history_url.split("/")
        fields[4] = fields[4] + "id_"
        history_url = "/".join(fields)
        time.sleep(args.sleep_second)
        logger.info(f"collecting number:{e}:{hostname}")
        trackers, outgoing_links = extract_trackers_from_internet_archive(
            history_url,
            get_text_selectolax,
            if_wandb=args.wandb,
            using_zyte=args.zyte,
            outgoing_link=args.outgoing_link,
        )
        if args.outgoing_link and outgoing_links is not None:
            fout.write(
                json.dumps(
                    {
                        "hostname": hostname,
                        "trackers": trackers,
                        "outgoing_links": outgoing_links,
                    }
                )
                + "\n"
            )
        else:
            fout.write(json.dumps({"hostname": hostname, "trackers": trackers}) + "\n")
            # fout.write(hostname + "\t" + ",".join(trackers) + "\n")
        fout.flush()


if __name__ == "__main__":

    if args.wandb:
        run = wandb.init(
            project="websci",
            group="IA",
            job_type=f"collect_historical_trackers{args.year}",
            config={
                "year": args.year,
            },
        )
    if args.unit_test:
        test_archive()
    elif args.multi_process:
        import json

        df = pd.read_json(
            args.input_path,
            lines=True,
        )
        fout = open(args.output_path + f"/archived_websites_{args.year}.json", "w")
        pool = mp.Pool(args.num_process)
        v = json.loads(df.to_json(orient="records"))
        # pool.map(collect_trackers_from_map_cc, list(v))
        for i, result in enumerate(
            tqdm(pool.imap_unordered(collect_trackers_from_map_ia, v), total=len(df))
        ):
            fout.write(result)
            fout.flush()
            if args.wandb:
                wandb.log({"progress": i + 1})
        pool.close()
        pool.join()

    else:
        collecting_single_thread()
    if args.wandb:
        run.finish()

#################################### colllecting outlinks from Internet Archive ###############################

# fout = open("RQX/tj_outlinks_of_old_outlinks.csv", "w")
# df_tj = pd.read_csv("RQX/tj_outlinkes_old_new.csv")
# for _, item in df_tj.iterrows():
#     try:
#         domain = item["outlinks"]

#         history_url = item["old_url"]
#         print(history_url)
#         outer_links = extract_trackers_from_internet_archive(history_url, get_outer_link)
#         fout.write(",".join(outer_links) + "\n")
#         # print(outer_links)å
#         fout.flush()

#     except Exception as e:
#         print(e)

########################### collecting from archive from ali


# years = list(range(2012, 2022))
# for year in years:
#     collect_trackers(f"{args['task_type']}_archive_ali", year)
# logger.info(time.time() - t)
