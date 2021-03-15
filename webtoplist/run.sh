
###
 # @Author: Zhan Su
 # @Date: 2021-03-03 11:20:16
 # @LastEditTime: 2021-03-15 20:14:38
 # @LastEditors: Please set LastEditors
 # @Description: In User Settings Edit
### 
for line in $(cat countryCode)
do
    printPer $line
    java TopSites * * $line
done