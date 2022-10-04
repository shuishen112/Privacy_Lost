# pip install homepage2vec

import logging
from homepage2vec.model import WebsiteClassifier
from datetime import date
import argparse
from tqdm import tqdm

parser = argparse.ArgumentParser()
parser.add_argument(
    "--start",
    default=0,
    help="start",
)

parser.add_argument(
    "--end",
    default=5000,
    help="end",
)

args = parser.parse_args()
print(args)


logging.getLogger().setLevel(logging.DEBUG)

model = WebsiteClassifier()
# Using readlines()
file1 = open("eu_urls_4th_sample_list.txt", "r")
Lines = file1.readlines()

today = date.today()
print("Starting date and time:", today)
count = 0
# Strips the newline character

for line in tqdm(Lines[int(args.start) : int(args.end)]):

    try:
        website = model.fetch_website(line.strip())

        scores, embeddings = model.predict(website)
        # print(max(scores, key=scores.get))
        with open("RQx_URLs_Classification_Output.txt", "a") as f:
            f.write(line.strip() + "\t" + max(scores, key=scores.get) + "\n")
        # print("Classes probabilities:", scores)
        # print("Embedding:", embeddings)
    except:
        pass
today = date.today()
print("Ending date and time:", today)
