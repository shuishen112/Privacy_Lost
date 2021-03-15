
###
 # @Author: Zhan Su
 # @Date: 2021-03-03 11:20:16
 # @LastEditTime: 2021-03-03 11:42:09
 # @LastEditors: Please set LastEditors
 # @Description: In User Settings Edit
### 
for line in $(cat countryCode)
do
    printPer $line
    java TopSites AKIATNXZERZXGKH76H4F BtWf/+CCEboCjZL3AipSRykG4XS1YJVa/FxtZVdw $line
done