import whois
import pandas as pd
import argparse
import json
import tqdm
import multiprocessing as mp

parser = argparse.ArgumentParser(description="Get country from domain")

parser.add_argument("--input_path", type=str, help="domain name")
parser.add_argument("--output_path", type=str, help="output path")
parser.add_argument(
    "--multi_process",
    action="store_true",
    help="multi process collecting",
)
parser.add_argument(
    "--num_process",
    type=int,
    default=10,
    help="number of process",
)

args = parser.parse_args()


def single_process():
    df = pd.read_csv(args.input_path, names=["domain", "count"], sep="\t")
    fout = open(args.output_path, "w")
    for i, row in tqdm.tqdm(df.iterrows()):
        domain = row["domain"].strip()
        try:
            json_format = whois.whois(domain)
            fout.write(
                json.dumps(
                    {"domain": domain, "info": json_format},
                    indent=4,
                    sort_keys=True,
                    default=str,
                )
                + "\n"
            )
            fout.flush()
        except Exception as e:
            print(e)
            print("error", domain)


df = pd.read_csv(args.input_path, names=["domain", "count"], sep="\t")

if __name__ == "__main__":

    def get_country_from_whois(row):
        try:
            domain = row["domain"].strip()
            json_format = whois.whois(domain)
            return (
                json.dumps(
                    {"domain": domain, "info": json_format},
                    indent=4,
                    sort_keys=True,
                    default=str,
                )
                + "\n"
            )
        except Exception as e:
            print(e)
            print("error", domain)
            return None

    if args.multi_process:
        fout = open(args.output_path, "w")
        pool = mp.Pool(args.num_process)
        v = json.loads(df.to_json(orient="records"))
        for i, result in enumerate(
            tqdm(pool.imap_unordered(get_country_from_whois, v), total=len(df))
        ):
            fout.write(result)
            fout.flush()
    else:
        single_process()
