import pandas as pd
from collections import Counter

df = pd.read_csv("resource/edu_repair/edu_controlset_dmoz.csv")

set_edu = df["edu_domain"].to_list()
set_sample = df["sample_domain"].to_list()

print(len(set(set_edu).intersection(set(set_sample))))

word_count = Counter(set_sample)
print(len(set(set_edu)))
print(len(set(set_sample)))
