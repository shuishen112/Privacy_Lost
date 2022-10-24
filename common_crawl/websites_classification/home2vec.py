# pip install homepage2vec

import logging
from homepage2vec.model import WebsiteClassifier
import operator
from datetime import date

logging.getLogger().setLevel(logging.DEBUG)

model = WebsiteClassifier()
# Using readlines()
file1 = open("eu_urls_1st_smaple_list.txt", "r")
Lines = file1.readlines()

today = date.today()
print("Starting date and time:", today)
count = 0
# Strips the newline character
start = input("Enter the starting url index: ")
end = input("Enter the ending url index: ")
for line in Lines[int(start) : int(end)]:
    count += 1
    print("Line{}: {}".format(count, line.strip()))
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
