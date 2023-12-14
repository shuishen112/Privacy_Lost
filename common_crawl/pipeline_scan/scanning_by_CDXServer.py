from urllib.request import urlopen
import json, requests
import time
import pandas as pd
import multiprocessing as mp
from tqdm import tqdm
import argparse
import os
import wandb

wandb.login()


parser = argparse.ArgumentParser()
parser.add_argument(
    "--input_data_path",
    type=str,
    default="websci/government_websites.csv",
    help="sampe path",
)

parser.add_argument(
    "--output_path",
    type=str,
    default="websci/IA/GOV/",
    help="output path",
)

parser.add_argument(
    "--year_begin",
    type=int,
    default=2023,
    help="year",
)

parser.add_argument(
    "--year_end",
    type=int,
    default=2024,
    help="year",
)

parser.add_argument(
    "--year",
    type=int,
    default=None,
    help="year",
)
args = parser.parse_args()

run = wandb.init(
    project="websci",
    group="IA",
    job_type=f"collect_historical_url{args.year}",
    config={
        "year": args.year,
    },
)


def get_specific_time_url(url, year_from, year_to):
    link = (
        "http://web.archive.org/cdx/search/cdx?url="
        + url
        + "&fl=timestamp,original&mimetype=text/html&output=json&from="
        + year_from
        + "&to="
        + year_to
        + "&collapse=timestamp:4&showSkipCount=true&lastSkipTimestamp=true&limit=1"
    )
    try:
        f = urlopen(link)
        text = f.read()
        list_archive = json.loads(text)
        if len(list_archive) == 2:
            timestamp = list_archive[1][0]
            origin = list_archive[1][1]
            historical_url = f"https://web.archive.org/web/{timestamp}/{origin}"
            return historical_url
        else:
            return None
    except Exception as e:
        print(url, e)
        # if the exception is connection refused
        # send a message to my email
        if ("429" in str(e)) or ("111" in str(e)):
            print("Connection refused by the server..")
            print("Let me sleep for 5 min ")
            print("ZZzzzz...")
            wandb.alert(
                title=f"{year_from}--------{url} Connection refused by the server..",
                text="Connection refused by the server.. Let me sleep for 5 min ZZzzzz...",
            )
            time.sleep(300)
            print("Was a nice sleep, now let me continue...")
            return "REFUSED"
        else:
            return "DEAD"


def get_timespan(domain):
    try:
        old_f = urlopen(
            f"https://web.archive.org/cdx/search/cdx?url={domain}&limit=1&output=json"
        )
        myfile = old_f.read()
        old = json.loads(myfile)

        new_f = urlopen(
            f"https://web.archive.org/cdx/search/cdx?url={domain}&limit=1&fastLatest=True&output=json"
        )
        myfile = new_f.read()
        new = json.loads(myfile)
        print(domain + "\t" + old[1][1] + "\t" + new[1][1] + "\n")
        # fout.write(domain + "\t" + old[1][1] + "\t" + new[1][1] + "\n")
        # fout.flush()
        return old[1][1], new[1][1]
    except Exception as e:
        print(e)


def get_timescan_fromCC(domain):
    link = (
        f"https://index.commoncrawl.org/CC-MAIN-2022-43-index?url={domain}&output=json"
    )
    try:
        requests.get(link).text
        f = urlopen(link)

        myfile = f.read()

        archive_url = json.loads(myfile)
        print(archive_url)
    except Exception as e:
        print(e)


def multi_threading():
    pool = mp.Pool(5)

    for _ in tqdm(
        pool.imap_unordered(get_timespan, list(list_host_name)),
        total=len(list_host_name),
    ):
        pass
    pool.close()
    pool.join()


def unit_test():
    url = "ku.dk"
    year_from = "2012"
    year_to = "2012"
    print(get_specific_time_url(url, year_from, year_to))
    # get_timespan("archive.org")


def collect_historical_url(year, list_host_name):
    # first we judge if the fout exists
    if os.path.exists(f"{args.output_path}domain_historical_year_{str(year)}.csv"):
        print("file exists")
        #   read the file
        df = pd.read_csv(
            f"{args.output_path}domain_historical_year_{str(year)}.csv",
            sep="\t",
            names=["hostname", "historical_url"],
        )
        # filter all the NAN
        df = df[df["historical_url"] == "NAN"]
        # get the list of the hostnames
        list_host_name = df["hostname"].unique()

    fout = open(f"{args.output_path}domain_historical_year_{str(year)}.csv", "a+")
    print(year)
    i = 0
    for item in tqdm(list_host_name):
        # logging the process using wandb
        i += 1
        wandb.log({"progress": i, "total": len(list_host_name)})
        time.sleep(1)
        # we should check if the url has been archived in the year
        historical_url = get_specific_time_url(item, str(year), str(year))
        if historical_url:
            fout.write(item + "\t" + historical_url + "\n")
        else:
            fout.write(item + "\t" + "NAN" + "\n")
        fout.flush()
    fout.close()


def collect_historical_url_from_several_years(year_begin, year_end, list_host_name):
    for year in range(year_begin, year_end):
        fout = open(f"{args.output_path}domain_historical_year_{str(year)}.csv", "w")
        print(year)
        for item in tqdm(list_host_name):
            time.sleep(1)
            historical_url = get_timescan_fromCC(item)
            if historical_url:
                fout.write(item + "\t" + historical_url + "\n")
            else:
                fout.write(item + "\t" + "NAN" + "\n")
            fout.flush()
        fout.close()


if __name__ == "__main__":
    if args.input_data_path.endswith("csv"):
        # read the dataset sample between x to y rows
        df = pd.read_csv(args.input_data_path)
    elif args.input_data_path.endswith("jsonl"):
        df = pd.read_json(args.input_data_path, lines=True)
    list_host_name = df["hostnames"].to_list()
    if not os.path.exists(args.output_path):
        os.makedirs(args.output_path, exist_ok=True)
    if args.year:
        collect_historical_url(args.year, list_host_name)
    else:
        collect_historical_url_from_several_years(
            args.year_begin, args.year_end, list_host_name
        )

    run.finish()
