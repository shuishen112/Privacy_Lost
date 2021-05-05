'''
Author: Zhan
Date: 2021-04-27 17:20:32
LastEditTime: 2021-05-02 11:34:48
LastEditors: Please set LastEditors
Description: In User Settings Edit
FilePath: /project_code/feature_extraction/feature_extraction.py
'''

import pandas as pd 

df = pd.read_csv('dk_cookie',sep = ',',error_bad_lines=False)
# sample
print(df.head()['name'])
# basic analysis
print(len(df))

# the number of cookies 
print(len(df['name'].unique()))

# the number of domain

print(len(df['domain'].unique()))

print(len(df['domain_id'].unique()))
# domain_id >

def cookie_feature(group):
    l = group['name'].unique()
    return l
df_count = df.groupby('domain_id').apply(cookie_feature).reset_index()
df_count.columns = ['domain_id','cookie_list']
print(df_count.head())

df_tiny = df[['domain_id','name']]
y = pd.get_dummies(df.name)

print(len(y.columns))


# print(df_tiny.head())

# one hot 特征转换

# from sklearn import preprocessing

# enc = preprocessing.OneHotEncoder()

# enc.fit(df['name'])

# data_transform = enc.transform(df_count['cookie_list'])
# print(data_transform)








