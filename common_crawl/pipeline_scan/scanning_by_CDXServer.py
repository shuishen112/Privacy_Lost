from urllib.request import urlopen
import json, requests
import time
import pandas as pd
import multiprocessing as mp
from tqdm import tqdm
import argparse
import os
import wandb


parser = argparse.ArgumentParser()
parser.add_argument(
    "--input_data_path",
    type=str,
    default="websci/government_websites.csv",
    help="sampe path",
)

parser.add_argument(
    "--output_dir",
    type=str,
    default="websci/IA/GOV/",
    help="output path",
)

parser.add_argument(
    "--year",
    type=int,
    default=None,
    help="year",
)

parser.add_argument(
    "--project",
    type=str,
    default="websci",
    help="project",
)

parser.add_argument(
    "--group",
    type=str,
    default="IA",
    help="group",
)

parser.add_argument(
    "--list_begin",
    type=int,
    default=0,
    help="list_begin",
)

parser.add_argument(
    "--list_end",
    type=int,
    default=30000,
    help="list_end",
)

parser.add_argument(
    "--unit_test",
    action="store_true",
    help="unit_test",
)

parser.add_argument(
    "--wandb",
    action="store_true",
    help="wandb",
)

parser.add_argument(
    "--sleep_second",
    type=int,
    default=0,
    help="sleep_second",
)


args = parser.parse_args()

if args.wandb:
    wandb.login()
    run = wandb.init(
        project=args.project,
        group=args.group,
        job_type=f"collect_historical_url{args.year}",
        config={
            "year": args.year,
        },
    )


def get_specific_time_url(url, time_start, time_end):
    # link = (
    #     "http://web.archive.org/cdx/search/cdx?url="
    #     + url
    #     + "&fl=timestamp,original&mimetype=text/html&output=json&from="
    #     + time_start
    #     + "&to="
    #     + time_end
    #     + "&collapse=timestamp:4&showSkipCount=true&lastSkipTimestamp=true&limit=1"
    # )

    link = f"https://web.archive.org/cdx/search/cdx?url={url}&from={time_start}&to={time_end}&limit=1&output=json&fl=timestamp,original"
    try:
        text = requests.get(
            link,
            proxies={
                "http": "http://17c6f0e36605472d8b3d8bdb034a1bd3:@api.zyte.com:8011/",
                "https": "http://17c6f0e36605472d8b3d8bdb034a1bd3:@api.zyte.com:8011/",
            },
            verify="zyte/zyte-ca.crt",
        ).text
        # f = urlopen(link)
        # text = f.read()
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
            if args.wandb:
                wandb.alert(
                    title=f"{time_start}-{url} Connection refused",
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
    time_start = "2012"
    time_end = "2012"
    print(get_specific_time_url(url, time_start, time_end))

    # get the specific month
    time_start = "201205"
    time_end = "201205"
    print(get_specific_time_url(url, time_start, time_end))
    # get_timespan("archive.org")


def collect_historical_url(year, list_host_name):
    fout = open(
        f"{args.output_dir}/hostname_historical_year_{str(year)}_{args.list_begin}_{args.list_end}.json",
        "a+",
    )
    print(year)
    i = 0
    for item in tqdm(list_host_name):
        # logging the process using wandb
        hostname = item.strip()
        i += 1
        if args.wandb:
            wandb.log({"progress": i, "total": len(list_host_name)})
        time.sleep(args.sleep_second)
        # we should check if the url has been archived in the year
        historical_url = get_specific_time_url(hostname, str(year), str(year))
        if historical_url:
            jsonwrite = json.dumps({"hostname": hostname, "url": historical_url})
            fout.write(jsonwrite + "\n")
        else:
            # if the url has not been archived in the year
            jsonwrite = json.dumps({"hostname": hostname, "url": "NAN"})
            fout.write(jsonwrite + "\n")
        fout.flush()
    fout.close()


if __name__ == "__main__":
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir, exist_ok=True)
    if args.unit_test:
        unit_test()
    else:
        assert args.year is not None
        list_host_name = open(args.input_data_path, "r").readlines()
        collect_historical_url(
            args.year, list_host_name[args.list_begin : args.list_end]
        )
        if args.wandb:
            run.finish()
