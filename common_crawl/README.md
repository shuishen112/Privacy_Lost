<!--
 * @Author: your name
 * @Date: 2021-03-24 17:27:32
 * @LastEditTime: 2021-03-25 15:50:41
 * @LastEditors: Please set LastEditors
 * @Description: In User Settings Edit
 * @FilePath: /project_code/common_crawl/README.md
-->

# Common Crawl For Privacy Lost

We will utilize the Common Crawl datasets to help us analyse the development of web tracking all over the world. For example, we need to analyse the datasets harvested from China. Before analysing, we need to clarify a few issues.

- how to determine which country the data comes from

- how can we know the tracking method from the HTML web page.


## Determine the countries

In the Common Crawl project, the webpage datasets are stored in WARC format. We can know the target URL from the head of the WARC file.

```
WARC/1.0
WARC-Type: response
WARC-Date: 2014-08-02T09:52:13Z
WARC-Record-ID: 
Content-Length: 43428
Content-Type: application/http; msgtype=response
WARC-Warcinfo-ID: 
WARC-Concurrent-To: 
WARC-IP-Address: 212.58.244.61
WARC-Target-URI: http://news.bbc.co.uk/2/hi/africa/3414345.stm
WARC-Payload-Digest: sha1:M63W6MNGFDWXDSLTHF7GWUPCJUH4JK3J
WARC-Block-Digest: sha1:YHKQUSBOS4CLYFEKQDVGJ457OAPD6IJO
WARC-Truncated: length
```

We can parse the Target-URL to obtain the domain of a website. In this example, "news.bbc.co.uk" belongs to the UK.

## Finding the tracking methods

Since we have knownn the country of website, we need to find the tracking methods in the webpages.

### Third-Parties and Trackers

Online tracking involves users browsing the Web and the website that they intentionally visit. In addition to these two actors, one or more services may be present that record users’ browsing to the website; we call such services **third-parties**. We refer to third-parties as **trackers** if their main purpose or the business model of their owning company depends on collecting browsing data of users.

Third-parties are either embedded dynamically such as via **JavaScript** or the **iframe** element, or statically via **link** or **image** tags. In the latter case, they lose some of their tracking abilities such as reading the referrer and browser properties such as the screen resolution.

In our project, we need to parse the webpage to obtain the third-parites. As for the trackers, I think we need to label it mannually. Some previous research work may give us a lot of help.

[https://ssc.io/trackingthetrackers/](https://ssc.io/trackingthetrackers/)

They labeld some third-parties and trackers and release their dataset on their website.

## Code

We will use the Map Reduce framework to help us process the massive datasets from the Common Crawl project. In this example, we just use a tiny sample of the dataset and we run our code at Amazon platform.

```
class DomainCount(CCJob):
    def process_record(self, record):
        # we're only interested in the response
        if record['WARC-Type'] != 'response':
            return
        try:
            url = urlparse(record['WARC-Target-URI'])[1]
            domain = url.split('.')[-1]
            if domain.upper() in country_dict:
                yield domain.upper()+"_{}".format(country_dict[domain.upper()]),1
           # yield domain, 1
            self.increment_counter('commoncrawl',"processed_server_domain", 1)
        except KeyError:
            pass

```

The result is as follows:

the data format is: countrycode_country  count

```
"AE_United Arab Emirates"       1
"AL_Albania"    1
"AM_Armenia"    2
"AR_Argentina"  43
"AS_American Samoa"     1
"AT_Austria"    26
"AU_Australia"  217
"AZ_Azerbaijan" 1
"BA_Bosnia and Herzegovina"     1
"BD_Bangladesh" 1
"BE_Belgium"    26
"BG_Bulgaria"   5
"BO_Bolivia, Plurinational State of"    2
"BR_Brazil"     113
"BY_Belarus"    1
"BZ_Belize"     1
"CA_Canada"     202
"CC_Cocos (Keeling) Islands"    18
"CH_Switzerland"        44
```

We can find that there are 105 countries in sample data.

```

wc -l part-0000*

54 part-00000
51 part-00001

```