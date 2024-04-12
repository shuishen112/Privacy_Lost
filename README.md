<!-- Improved compatibility of back to top link: See: https://github.com/othneildrew/Best-README-Template/pull/73 -->
<a name="readme-top"></a>
<!--
*** Thanks for checking out the Best-README-Template. If you have a suggestion
*** that would make this better, please fork the repo and create a pull request
*** or simply open an issue with the tag "enhancement".
*** Don't forget to give the project a star!
*** Thanks again! Now go create something AMAZING! :D
-->



<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]
[![LinkedIn][linkedin-shield]][linkedin-url]



<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://github.com/github_username/repo_name">
    <img src="images/Privacy_Lost_cover_Large.jpg" alt="Logo" width="80" height="80">
  </a>

<h3 align="center">Privacy Lost</h3>

  <p align="center">
    The project aims to uncover how commercial surveillance of web users has developed all over the world, and how political, economic and demographic factors have shaped it.
    <br />
    <a href="https://github.com/github_username/repo_name"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="https://github.com/github_username/repo_name">View Demo</a>
    ·
    <a href="https://github.com/github_username/repo_name/issues">Report Bug</a>
    ·
    <a href="https://github.com/github_username/repo_name/issues">Request Feature</a>
  </p>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project

[![Product Name Screen Shot][product-screenshot]](nytimes.com/2019/04/21/opinion/letters/privacy-project-responses.html)

The privacy List project aims to study how regional differences in power and politics have shaped worldwide digital surveillance. We want to use web tracking (based on cookies and scripts embedded in websites) as an exemplar of digital surveillance and use this to make core elements and dynamics of digital surveillance visible. The engineers of some of the early web tracking companies were quick to realize how tracking could combine with machine learning and be monetized. The project we propose turns this on its head and uses the same tools to understand web tracking in a way that is at once find-grained and large-scale. 

<p align="right">(<a href="#readme-top">back to top</a>)</p>


### Built With

<!-- * [![Next][Next.js]][Next-url]
* [![React][React.js]][React-url]
* [![Vue][Vue.js]][Vue-url]
* [![Angular][Angular.io]][Angular-url]
* [![Svelte][Svelte.dev]][Svelte-url]
* [![Laravel][Laravel.com]][Laravel-url]
* [![Bootstrap][Bootstrap.com]][Bootstrap-url]
* [![JQuery][JQuery.com]][JQuery-url] -->

- [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

- [![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
- [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
- [![python:3.8](https://img.shields.io/badge/python-3.8-green)]()
<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- GETTING STARTED -->
## Getting Started


### Installation


   ```sh
   git clone https://github.com/shuishen112/Privacy_Lost.git

   cd Privacy_Lost/commoncrawl
   pip install -r requirements.txt
   ```


<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Register Zyte

you also need to registered the https://www.zyte.com/ to use this library efficiently (with multi-threads)

- download the zyte-ca.crt file from https://app.zyte.com/o/659646/zyte-api/run-request

- save the file in the path
common_crawl/zyte/zyte-ca.crt

- set the API environment

```
export API=your api
```



<!-- USAGE EXAMPLES -->
## Usage

The project include the paper for ADMA2023:

[Privacy Lost in Online Education: Analysis of
Web Tracking Evolution](common_crawl/README.md)


# Internet Archive 

When you want to use the multi-thread, it is better to use zyte with --zyte, you can also use it when you use single-thread. 

## Scanning process:

### Single-thread
In this case, we scan the snapshot from 2003. 

 ```
 python pipeline_scan/scanning_by_CDXServer.py --year=2003 --input_data_path=communication_conference/complete_list.csv --output_dir=debug --list_begin=30000 --list_end=60000
 ```

### Multi-thread



```
python pipeline_scan/scanning_by_CDXServer.py --year=2003 --input_data_path=communication_conference/complete_list.csv --output_dir=debug --list_begin=30000 --list_end=60000 --sleep_second=0 --multi_process --num_process=48 --zyte
 ```
## Collecting trackers:

### Unit test 

```
python pipeline_archived/collecting_trackers_from_archive.py --unit_test
```
collecting trackers from 2023, you should change the default input path

### Single-thread

```
python pipeline_archived/collecting_trackers_from_archive.py --year=2023 --input_path=your_scanning_file --output_path=debug
```

### Multi-thread

```
python pipeline_archived/collecting_trackers_from_archive.py --year=2023 --input_path=debug/hostname_historical_year_2023_30000_60000.json  --output_path=debug --sleep_second=0 --multi_process --num_process=48 --zyte
```


# COMMON CRAWL
## Unit test

```
cd common_crawl

export PYTHONPATH=./

python pipeline_archived/collecting_trackers_from_cc.py --unit_test
```


## Crawling from common crawl

```
python pipeline_archived/collecting_trackers_from_cc.py --input_path=communication_conference/CC/historical_scan/6.5_snapshot_2021_host_name.csv --num_process=96 --multi_process --output_dir=communication_conference/CC/historical_trackers/6.5_2021_with_description.json --get_description=True --group=CC_description --wandb

```

<!-- Use this space to show useful examples of how a project can be used. Additional screenshots, code examples and demos work well in this space. You may also link to more resources.

_For more examples, please refer to the [Documentation](https://example.com)_ -->

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- ROADMAP -->
## Roadmap

There are several steps to analyze the historical web tracking. 

- Sample: we create the samples. (Usually, samples are specific websites)

- Scanning: we need to get access to the historical snapshot using wayback machine. 

- Collecting: We collect the historical trackers from HTML files. 


<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- CONTRIBUTING -->
## Contributing

This project aims to collect the trackers from waybackmachine: Archive or Common Crawl. 



<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- CONTACT -->
## Contact

Your Name - [@twitter_handle](https://twitter.com/twitter_handle) - zhan.su@di.ku.dk

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- ACKNOWLEDGMENTS -->
## Acknowledgments


<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/github_username/repo_name.svg?style=for-the-badge
[contributors-url]: https://github.com/github_username/repo_name/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/github_username/repo_name.svg?style=for-the-badge
[forks-url]: https://github.com/github_username/repo_name/network/members
[stars-shield]: https://img.shields.io/github/stars/github_username/repo_name.svg?style=for-the-badge
[stars-url]: https://github.com/github_username/repo_name/stargazers
[issues-shield]: https://img.shields.io/github/issues/github_username/repo_name.svg?style=for-the-badge
[issues-url]: https://github.com/github_username/repo_name/issues
[license-shield]: https://img.shields.io/github/license/github_username/repo_name.svg?style=for-the-badge
[license-url]: https://github.com/github_username/repo_name/blob/master/LICENSE.txt
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://linkedin.com/in/linkedin_username
[product-screenshot]: images/privacy_lost.png
[Next.js]: https://img.shields.io/badge/next.js-000000?style=for-the-badge&logo=nextdotjs&logoColor=white
[Next-url]: https://nextjs.org/
[React.js]: https://img.shields.io/badge/React-20232A?style=for-the-badge&logo=react&logoColor=61DAFB
[React-url]: https://reactjs.org/
[Vue.js]: https://img.shields.io/badge/Vue.js-35495E?style=for-the-badge&logo=vuedotjs&logoColor=4FC08D
[Vue-url]: https://vuejs.org/
[Angular.io]: https://img.shields.io/badge/Angular-DD0031?style=for-the-badge&logo=angular&logoColor=white
[Angular-url]: https://angular.io/
[Svelte.dev]: https://img.shields.io/badge/Svelte-4A4A55?style=for-the-badge&logo=svelte&logoColor=FF3E00
[Svelte-url]: https://svelte.dev/
[Laravel.com]: https://img.shields.io/badge/Laravel-FF2D20?style=for-the-badge&logo=laravel&logoColor=white
[Laravel-url]: https://laravel.com
[Bootstrap.com]: https://img.shields.io/badge/Bootstrap-563D7C?style=for-the-badge&logo=bootstrap&logoColor=white
[Bootstrap-url]: https://getbootstrap.com
[JQuery.com]: https://img.shields.io/badge/jQuery-0769AD?style=for-the-badge&logo=jquery&logoColor=white
[JQuery-url]: https://jquery.com 