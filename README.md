<div id="top"></div>

[Contributors][contributors-url]
[Forks][forks-url]
[Stargazers][stars-url]
[Issues][issues-url]
[MIT License][license-url]
[LinkedIn][linkedin-url]

<!-- PROJECT LOGO -->

<br />
<div align="center">
    <h1>Open Data Crawler</h1>
  <p align="center">
    A tool to crawl data to your projects from open data portals
    <br />
    <a href="https://github.com/aberenguerpas/opendatacrawler/issues">Report Bug</a>
    ·
    <a href="https://github.com/aberenguerpas/opendatacrawler/issues">Request Feature</a>
  </p>
</div>

<!-- TABLE OF CONTENTS -->

<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#requirements">requirements</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
  </ol>
</details>

<!-- ABOUT THE PROJECT -->

## About The Project

Open Data Crawler is a tool to extract data from open data portals and statistics portals. The community can contribute adding support to other data portals or adding new features.

Features:

* Download datasets from open data portals
* Download metadata from resources
* Filter by data type

<p align="right">(<a href="#top">back to top</a>)</p>

<!-- GETTING STARTED -->

## Getting Started

This is an example of how you may give instructions on setting up your project locally.
To get a local copy up and running follow these simple example steps.

### Requirements

* You need at least python 3.9 installed
* Clone the repo

  ```sh
  git clone https://github.com/aberenguerpas/opendatacrawler.git
  ```
* Move to root directory

  ```sh
  cd opendatacrawler
  ```
* Install the requirements from requirements.txt

  ```sh
  pip install -r requirements.txt
  ```
* Socrata portals requiere an app token to avoid throttling limits, you can obtain an api key [here](https://support.socrata.com/hc/en-us/articles/210138558-Generating-an-App-Token)
  and set on ``config.ini``

### Installation

1. Run from the project root
   ```sh
   python3 setup.py install 
   ```

<p align="right">(<a href="#top">back to top</a>)</p>

<!-- USAGE EXAMPLES -->

## Usage

Use this tool is very simple, you only need to specify the data source and the tool automatically detect the portal type and starts to dowload the data.

### Examples

#### Dowload all data and metadata from a portal:

```
python opendatacrawler -d data.europa.eu
```

#### Download only metadata:

```
python opendatacrawler -d data.europa.eu -m
```

#### Dowload specific fromat data. For example xls and csv:

```
python opendatacrawler -d data.europa.eu -f xls csv
```

_For more examples, please refer to the [Documentation](https://example.com)_

<p align="right">(<a href="#top">back to top</a>)</p>

## Currently supported portals and sites

- [X] data.europa.eu

See the [open issues](https://github.com/aberenguerpas/opendatacrawler/issues) for a full list of proposed features (and known issues).

<p align="right">(<a href="#top">back to top</a>)</p>

<!-- LICENSE -->

## License

Distributed under the MIT License. See `LICENSE` for more information.

<p align="right">(<a href="#top">back to top</a>)</p>

## Colaborators

🙋‍♂️ Javier Ríos Cerdán

🙋‍♂️ Alberto Berenguer Pastor

<!-- CONTACT -->

## Contact

🙋‍♂️Alberto Berenguer Pastor 
✉️ alberto.berenguer@ua.es

<p align="right">(<a href="#top">back to top</a>)</p>

<!-- MARKDOWN LINKS & IMAGES -->

<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->

[contributors-shield]: https://img.shields.io/github/contributors/aberenguerpas/opendatacrawler?style=for-the-badge
[contributors-url]: https://github.com/aberenguerpas/opendatacrawler/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/aberenguerpas/opendatacrawler.svg?style=for-the-badge
[forks-url]: https://github.com/aberenguerpas/opendatacrawler/network/members
[stars-shield]: https://img.shields.io/github/stars/aberenguerpas/opendatacrawler.svg?style=for-the-badge
[stars-url]: https://github.com/aberenguerpas/opendatacrawler/stargazers
[issues-shield]: https://img.shields.io/github/issues/aberenguerpas/opendatacrawler.svg?style=for-the-badge
[issues-url]: https://github.com/aberenguerpas/opendatacrawler/issues
[license-shield]: https://img.shields.io/github/license/aberenguerpas/opendatacrawler?style=for-the-badge
[license-url]: https://github.com/aberenguerpas/opendatacrawler/blob/main/LICENSE
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://www.linkedin.com/in/alberto-berenguer-pastor-220274154/
[product-screenshot]: images/screenshot.png
