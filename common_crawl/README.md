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

# Archive New Version

1. scanning process:

In this case, we scan the snapshot from 2003. 

> python pipeline_scan/scanning_by_CDXServer.py --year=2003 --input_data_path=communication_conference/complete_list.csv --output_dir=debug --list_begin=30000 --list_end=60000 --wandb

2. collecting trackers:

unit test 

> python pipeline_archived/collecting_trackers_from_archive.py --unit_test

collecting trackers from 2023, you should change the default input path

> python pipeline_archived/collecting_trackers_from_archive.py --year=2023 --output_path=debug



# COMMON CRAWL
## unit test

cd common_crawl

export PYTHONPATH=./

python pipeline_archived/collecting_trackers_from_cc.py --unit_test


## Crawling from common crawl


> python pipeline_archived/collecting_trackers_from_cc.py --input_path=communication_conference/CC/historical_scan/6.5_snapshot_2021_host_name.csv --num_process=96 --multi_process --output_dir=communication_conference/CC/historical_trackers/6.5_2021_with_description.json --get_description=True --group=CC_description --wandb
