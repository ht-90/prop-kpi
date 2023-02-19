"""Generate URLs and download data files."""

import os
import requests


class DataScraper:
    """Parent class for scraping data."""

    def __init__(self):
        """Initialise a data scraper class."""
        self.parent_path = "./data"


class BuildingApprovalsScraper(DataScraper):
    """Download building approvals data."""

    def __init__(self):
        """Initialise a building approvals scraper class."""
        super().__init__()
        self.dest_path = os.path.join(
            self.parent_path,
            "building_approval"
        )
        self.base_url = "https://www.abs.gov.au/statistics/industry/building-and-construction/building-approvals-australia/"
        self.base_name = "87310do0"
        self.year = [2020, 2021, 2022]
        self.map_month = {
            "jan": "01",
            "feb": "02",
            "mar": "03",
            "apr": "04",
            "may": "05",
            "jun": "06",
            "jul": "07",
            "aug": "08",
            "sep": "09",
            "oct": "10",
            "nov": "11",
            "dec": "12"
        }
        self.map_state = {
            "nsw": "04",
            "vic": "08",
            "qld": "12",
            "sa": "16",
            "wa": "20",
            "tas": "24",
            "nt": "28",
            "act": "32",
        }
        self.formats = ["xls", "xlsx"]
        self.file_urls = self.build_urls()

    def build_urls(self):
        """Build URLs to download data files.

        :returns: A list of URLs
        :type str: list
        """
        file_urls = []
        for y in self.year:
            for m in self.map_month.keys():
                for s in self.map_state.keys():
                    file_urls.append(
                        f"{self.base_url}{m}-{y}/{self.base_name}{self.map_state[s]}_{y}{self.map_month[m]}"
                    )
        return file_urls

    def download_files(self):
        """Download data files."""
        for f in self.file_urls:
            with requests.get(f"{f}.{self.formats[0]}") as req:
                if req.status_code == 200:
                    self.download_single_file(f, 0)
                else:
                    self.download_single_file(f, 1)

    def download_single_file(self, url, ind):
        """Download a file from URL.

        :param url: URL
        :type: str
        :param ind: An index for file formats
        :type: int
        """
        with requests.get(f"{url}.{self.formats[ind]}") as f_req:
            f_name = f"{url.split('/')[-1]}.{self.formats[ind]}"
            file_path = os.path.join(self.dest_path, f_name)
            open(file_path, 'wb').write(f_req.content)
            print(f"Downloaded: {url.split('/')[-1]}.{self.formats[ind]}")

    def execute(self):
        """Execute generating a directory and downloading data files."""
        make_dir(self.dest_path)
        self.download_files()
        print("Building Approvals data downloaded.")


def make_dir(dir_path):
    """Create a directory to store downloaded data files.

    :param: A path for a directory to be created
    :type path: str

    """
    if os.path.exists(dir_path):
        print(f"Directory {dir_path} already exists.")

    else:
        try:
            os.mkdir(dir_path)

        except OSError as e:
            print(e)
            print(f"Directory '{dir_path}' could not be created.")
