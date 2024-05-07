import whois
import pandas as pd
import argparse
import json

parser = argparse.ArgumentParser(description="Get country from domain")

parser.add_argument("--input_path", type=str, help="domain name")

args = parser.parse_args()

df = pd.read_csv(args.input_path, names=["domain", "count"], sep="\t")

if __name__ == "__main__":
    fout = open("pipeline_scan/domain_country.json", "w")
    for i, row in df.iterrows():
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
