# pip install homepage2vec

import logging
from homepage2vec.model import WebsiteClassifier
from datetime import date
import argparse
from tqdm import tqdm
import json

parser = argparse.ArgumentParser()
parser.add_argument(
    "--start",
    default=0,
    help="start",
)

parser.add_argument(
    "--end",
    default=20000,
    help="end",
)

parser.add_argument(
    "--input_file",
    default="websci/government_websites_reversed.csv",
    help="input_file",
)

parser.add_argument(
    "--output_file",
    default="websci/government_websites_reversed_classification.jsonl",
    help="output_file",
)

args = parser.parse_args()
print(args)


logging.getLogger().setLevel(logging.DEBUG)

model = WebsiteClassifier()
# Using readlines()
file1 = open(args.input_file, "r")
Lines = file1.readlines()

today = date.today()
print("Starting date and time:", today)
count = 0
# Strips the newline character
with open(args.output_file, "w") as f:
    for line in tqdm(Lines[int(args.start) : int(args.end)]):
        try:
            website = model.fetch_website(line.strip())
            scores, embeddings = model.predict(website)
            # print(max(scores, key=scores.get))
            write_dict = {
                "host_name": line.strip(),
                "class": max(scores, key=scores.get),
                "class_probabilities": scores,
                "embedding": embeddings,
            }
            f.write(json.dumps(write_dict) + "\n")
            f.flush()
        except Exception as e:
            print(e)
            pass
today = date.today()
print("Ending date and time:", today)
