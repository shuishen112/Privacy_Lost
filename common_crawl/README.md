# Privacy Lost in Online Education

Offical code for the paper:
Privacy Lost in Online Education: Analysis of Web Tracking Evolution



[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Wayback Machine

We use the archive as the wayback machine.

https://archive.org/web/

## Data processing 

We get the historical snapshots for educational urls and control set urls based on *WaybackMachineCDXServerAPI*

1) resource/available-control-urls.txt
2) resource/available-edu-urls.txt


# ARCHIVE 
## Crawling

1) crawling the html file to the local machine

pipeline_archived/collecting.py

2) extracting the dataset from the zip file.

pipeline_archived/collecting_trackers.py

Then the final dataset is just like this:

https://web.archive.org/web/20120519095510/http://11makelaars.nl:80/,"addthis,google-analytics"

## Analyse

Figure 1: notebook/track_evolution.ipynb

Figure 2: notebook/Wilcoxon_Sign-Ranked.ipynb

