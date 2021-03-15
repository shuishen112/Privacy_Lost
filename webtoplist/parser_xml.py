'''
Author: Zhan Su
Date: 2021-03-01 22:09:47
LastEditTime: 2021-03-09 10:42:27
LastEditors: Please set LastEditors
Description: In User Settings Edit
'''

import xml.etree.ElementTree as ET

import os

dirs = os.listdir('countryXml')

ns = {"domain":"http://ats.amazonaws.com/doc/2005-10-05"}

if not os.path.exists('country_rank'):
    os.makedirs('country_rank')
        
for file in dirs:
    print(file)
    tree = ET.parse('countryXml/{}'.format(file))
    root = tree.getroot()
# print(root.tag)
# print(root.attrib)
   
    with open("country_rank/rank_{}.txt".format(file.split('.')[0]),'w') as fout:

        for site in root.iter("{http://ats.amazonaws.com/doc/2005-07-11}DataUrl"):
            # print("http://" + site.text)
            fout.write("http://" + site.text + '\n')


# import pycountry

# # print(len(pycountry.countries))

# for country in pycountry.countries:
#     print(country.alpha_2)
