"""
Author: your name
Date: 2021-08-30 10:54:01
LastEditTime: 2022-04-04 13:28:34
LastEditors: Please set LastEditors
Description: In User Settings Edit
FilePath: /common_crawl/sample.py
"""
# 对教育网站进行sample
import pandas as pd
import tldextract
from tqdm import tqdm
import time

tqdm.pandas()

edu_web_list_df = pd.read_csv(
    "resource/dmoz_educational_websites_clean.csv", names=["domain", "category"]
)


def get_domain(row):
    domain = row["domain"]
    if "www" in domain:
        domain = domain.replace("www.", "")
    return domain
    # items = tldextract.extract(row['domain'])
    # domain_tld = '.'.join([items.domain,items.suffix])
    # register_domain = items.domain

    # return domain_tld,register_domain


edu_web_list_df["domain"] = edu_web_list_df.apply(get_domain, axis=1)
print(edu_web_list_df[:10])


######## 阅读top 的websites
# sample_list_df = pd.read_csv("resource/top-1m.csv",names = ['index','domain'])
# sample_list_df['tld'] = sample_list_df.progress_apply(get_domain,axis = 1)
# sample_list = sample_list_df['tld'].to_list()

# sample_list_df = pd.read_csv("resource/top10milliondomains.csv")
# sample_list_df.columns = ['rank','domain','open_page_rank']
# sample_list_df[['domain_tld','register_domain']] = sample_list_df.progress_apply(get_domain,axis = 1,result_type='expand')
# sample_list_df.to_csv("resource/top10milliondomains_expand.csv",index = None)

##########

key_compare = "domain"
sample_list_df = pd.read_csv("resource/top10milliondomains_expand.csv")

sample_list = sample_list_df[key_compare].to_list()
rank = sample_list_df["rank"].to_list()

rank_to_domain = dict(zip(rank, sample_list))
domain_to_rank = dict(zip(sample_list, rank))

print(len(edu_web_list_df[edu_web_list_df[key_compare].isin(sample_list)]))

###################  sample previous ############

sample = []
edu = []
edu_rank = []
sample_rank = []
edu_list = edu_web_list_df["domain"].unique()


def judge(sample_list, index, edu_list, sample):
    if sample_list[index] not in edu_list and sample_list[index] not in sample:
        return True
    else:
        return False


windows = 100

start = time.time()
for e, d in tqdm(enumerate(edu_list)):

    # i = sample_list.index(d)
    i = domain_to_rank.get(d)
    if i is not None:
        # judge above is non-edu?
        for scan in range(1, windows):
            above = i + scan
            below = i - scan
            if (
                rank_to_domain[above] not in edu_list
                and rank_to_domain[above] not in sample
            ):
                sample.append(rank_to_domain[above])
                sample_rank.append(above)
                # print("above", scan)
                break
            elif (
                rank_to_domain[below] not in edu_list
                and rank_to_domain[below] not in sample
            ):
                sample.append(rank_to_domain[below])
                sample_rank.append(below)
                # print("below", scan)
                break
        edu.append(edu_list[e])
        edu_rank.append(i)
df_sample = pd.DataFrame(
    {
        "edu_domain": edu,
        "sample_domain": sample,
        "edu_rank": edu_rank,
        "sample_rank": sample_rank,
    }
)

print(time.time() - start)
print(len(df_sample))
df_sample.to_csv("resource/edu_repair/edu_controlset_dmoz.csv", index=None)

#########
