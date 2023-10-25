# Importing libraries
import requests
from bs4 import BeautifulSoup
  
# setting up the URL
URL = "https://whotracks.me/trackers.html"
  
# perform get request to the url
reqs = requests.get(URL)
  
# extract all the text that you received from
# the GET request
content = reqs.text
  
# convert the text to a beautiful soup object
soup = BeautifulSoup(content, 'html.parser')
  
# Empty list to store the output
urls = []
  
# For loop that iterates over all the <li> tags

for h in soup.findAll('li'):
    
    # looking for anchor tag inside the <li>tag
    a = h.find('a')
    try:
        
        # looking for href inside anchor tag
        if 'href' in a.attrs:
            
            # storing the value of href in a separate variable
            url = a.get('href')
              
            # appending the url to the output list
            urls.append(url)
              
    # if the list does not has a anchor tag or an anchor tag
    # does not has a href params we pass
    except:
        pass
  
# print all the urls stored in the urls list
trackerList = []
for url in urls:
	tracker = "https://whotracks.me" +url.replace('./','/')
	trackerList.append(tracker)
	print(tracker)
    
print(str(len(urls)))   

count = 0 
f1 = open('WhoTracksMeList_New_0.txt', 'a', encoding="utf-8")
for t in trackerList[:10]:
	count = count + 1
	print(str(count))
	try:
		
		reqs = requests.get(t, timeout=5)
		content = reqs.text
		soup = BeautifulSoup(content, 'html.parser')
		paragraphs = soup.find_all("a", {"class": "profile-label"})
		title = ""
	
		title = soup.h1
		owner = ''
		article = soup.find("div", {"class":"col-md-6 col-sm-6"}).findAll('p')
		#spans = soup.findAll('span', attrs = {'class' : 'cat-item'})
		#cat =  spans[len(spans)-1].text.strip()
		cat = soup.find("span", {"class":"cat-item"}).text.strip()
		for element in article:
			owner += '\t' + ''.join(element.findAll(text = True)).strip()
		review_text = []
		#title =  soup.find_all(["h1"])
		paragraph = [p.text for p in paragraphs]
		review_text.append(paragraph)
		for t1 in review_text:
			for t2 in t1:
				#print( t2.strip())
				f1.write(t2.strip()+ '\t' + str(title) + '\t'+ str(cat) + '\t' +str(owner)  + '\n')
	except:
		print(t + "\t" + str(count))
		pass
f1.close()