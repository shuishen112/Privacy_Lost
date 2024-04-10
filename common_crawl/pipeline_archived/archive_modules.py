import time
import argparse
from utils import extract_trackers_from_internet_archive
from warc_module.warc_utils import (
    get_text_selectolax,
    get_outer_link,
)
import json

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
        trackers = extract_trackers_from_internet_archive(
            history_url, get_text_selectolax, if_wandb=args.wandb
        )
        if trackers == []:
            return json.dumps({"hostname": hostname, "trackers": "NO_TRACKERS"}) + "\n"
        elif trackers == "REFUSED":
            return json.dumps({"hostname": hostname, "trackers": "REFUSED"}) + "\n"
        elif trackers == "DEAD":
            return json.dumps({"hostname": hostname, "trackers": "DEAD"}) + "\n"
        else:
            return json.dumps({"hostname": hostname, "trackers": trackers}) + "\n"
    except Exception as e:
        # print(history_url)
        print(e)
