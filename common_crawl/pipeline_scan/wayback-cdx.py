from waybackpy import WaybackMachineCDXServerAPI
from urllib.request import urlopen
import json, requests

def get_specific_time_url(url,year_from,year_to):
	link = "http://web.archive.org/cdx/search/cdx?url=" + url +"&fl=timestamp,&mimetype=text/html&output=json&from=" +year_from + "&to="+year_to+ "&collapse=timestamp:4&showSkipCount=true&lastSkipTimestamp=true"
	try:
		f = urlopen(link)
	except:
		time.sleep(1)
		f = urlopen(link)
	myfile = f.read()
	archive_url = json.loads(myfile)
	snap_list = {}
	for line in archive_url[1:]:
		snap_list[line[0][0:4]] = line[0]
	return snap_list

#loop here 
url = 'ku.dk'
year_from = "1985"
year_to = "2022"
print(get_specific_time_url(url,year_from,year_to))