import os
import csv
import requests
import json
import logging
from urllib.parse import urlencode, urlparse
from bs4 import BeautifulSoup
import concurrent.futures
from dataclasses import dataclass, field, fields, asdict

API_KEY = ""

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    API_KEY = config["api_key"]


def get_scrapeops_url(url, location="us", wait=None):
    payload = {
        "api_key": API_KEY,
        "url": url,
        "country": location,
        }
    if wait:
        payload["wait"] = wait
    proxy_url = "https://proxy.scrapeops.io/v1/?" + urlencode(payload)
    return proxy_url


## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



@dataclass
class SearchData:
    name: str = ""
    base_url: str = ""
    url: str = ""
    result_number: int = 0

    def __post_init__(self):
        self.check_string_fields()
        
    def check_string_fields(self):
        for field in fields(self):
            if isinstance(getattr(self, field.name), str):
                if getattr(self, field.name) == "":
                    setattr(self, field.name, f"No {field.name}")
                    continue
                value = getattr(self, field.name)
                setattr(self, field.name, value.strip())

@dataclass
class MetaData:
    name: str = ""
    url: str = ""
    description: str = ""


    def __post_init__(self):
        self.check_string_fields()
        
    def check_string_fields(self):
        for field in fields(self):
            if isinstance(getattr(self, field.name), str):
                if getattr(self, field.name) == "":
                    setattr(self, field.name, f"No {field.name}")
                    continue
                value = getattr(self, field.name)
                setattr(self, field.name, value.strip())


class DataPipeline:
    
    def __init__(self, filename="", storage_queue_limit=50, output_format="csv"):
        self.names_seen = []
        self.storage_queue = []
        self.storage_queue_limit = storage_queue_limit
        self.filename = filename
        self.file_open = False
        self.output_format = output_format.lower()
    
    def save_to_csv(self):
        self.file_open = True
        data_to_save = self.storage_queue[:]
        self.storage_queue.clear()
        if not data_to_save:
            return

        keys = [field.name for field in fields(data_to_save[0])]
        file_exists = os.path.isfile(self.filename) and os.path.getsize(self.filename) > 0

        with open(self.filename, mode="a", newline="", encoding="utf-8") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=keys)
            if not file_exists:
                writer.writeheader()
            for item in data_to_save:
                writer.writerow(asdict(item))
        
        self.file_open = False
    
    def save_to_json(self):
        self.file_open = True
        data_to_save = self.storage_queue[:]
        self.storage_queue.clear()
        if not data_to_save:
            return

        file_exists = os.path.isfile(self.filename)

        existing_data = []
        if file_exists:
            with open(self.filename, "r", encoding="utf-8") as existing_file:
                try:
                    existing_data = json.load(existing_file)
                except json.JSONDecodeError:
                    logger.error(f"Corrupted JSON file: {self.filename}")
                    raise

            existing_data.extend(asdict(item) for item in data_to_save)

        with open(self.filename, "w", encoding="utf-8") as output_file:
            json.dump(existing_data, output_file, indent=4)

        self.file_open = False


    def is_duplicate(self, input_data):
        if input_data.name in self.names_seen:
            logger.warning(f"Duplicate item found: {input_data.name}. Item dropped.")
            return True
        self.names_seen.append(input_data.name)
        return False
    
    def add_data(self, scraped_data):
        if not self.is_duplicate(scraped_data):
            self.storage_queue.append(scraped_data)
            if len(self.storage_queue) >= self.storage_queue_limit and not self.file_open:
                self.save()
    
    def save(self):
        if self.output_format == "csv":
            self.save_to_csv()
        elif self.output_format == "json":
            self.save_to_json()
        else:
            raise ValueError(f"Unsupported output format: {self.output_format}")
    
    def close_pipeline(self):
        if self.file_open:
            time.sleep(3)
        if self.storage_queue:
            self.save()


def scrape_search_results(keyword, location, data_pipeline=None, retries=3):
    formatted_keyword = keyword.replace(" ", "+")
    result_number = 0
    url = f"https://duckduckgo.com/?q={formatted_keyword}&t=h_&ia=web"
    tries = 0
    success = False
    
    while tries <= retries and not success:
        try:
            scrapeops_proxy_url = get_scrapeops_url(url, location=location, wait=5)
            response = requests.get(scrapeops_proxy_url)
            logger.info(f"Recieved [{response.status_code}] from: {url}")
            if response.status_code == 200:
                success = True
            
            else:
                raise Exception(f"Failed request, Status Code {response.status_code}")
                
                ## Extract Data
            
            soup = BeautifulSoup(response.text, "html.parser")            
            headers = soup.find_all("h2")
            
            for header in headers:
                link = header.find("a")
                h2 = header.text
                if not link:
                    continue
                href = link.get("href")
                
                rank = result_number

                parsed_url = urlparse(href)
                base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

                search_data = SearchData(
                    name=h2,
                    base_url=base_url,
                    url=href,
                    result_number=rank
                )
                data_pipeline.add_data(search_data)
                result_number += 1                
                
            logger.info(f"Successfully parsed data from: {url}")
            success = True
        
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")
    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")


def process_result(row, location, retries=3, data_pipeline=None):
    url = row["url"]
    tries = 0
    success = False

    while tries <= retries and not success:
        response = requests.get(get_scrapeops_url(url, location=location))
        try:
            if response.status_code == 200:
                logger.info(f"Status: {response.status_code}")

                soup = BeautifulSoup(response.text, "html.parser")
                head = soup.find("head")

                title = head.find("title").text
                meta_tags = head.find_all("meta")

                description = "n/a"
                description_holder = head.select_one("meta[name='description']")
                if description_holder:
                    description = description_holder.get("content")
                    
                meta_data = MetaData(
                    name=title,
                    url=row["url"],
                    description=description
                )
                data_pipeline.add_data(meta_data)
                success = True

                data_pipeline.close_pipeline()

            else:
                logger.warning(f"Failed Response: {response.status_code}")
                raise Exception(f"Failed Request, status code: {response.status_code}")
        except Exception as e:
            logger.error(f"Exception thrown: {e}")
            logger.warning(f"Failed to process page: {row['url']}")
            logger.warning(f"Retries left: {retries-tries}")
            tries += 1
    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")
    else:
        logger.info(f"Successfully parsed: {row['url']}")


def process_results(csv_file, location, max_threads=5, retries=3, data_pipeline=None):
    logger.info(f"processing {csv_file}")
    with open(csv_file, newline="") as file:
        reader = list(csv.DictReader(file))

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            executor.map(
                process_result,
                reader,
                [location] * len(reader),
                [retries] * len(reader),
                [data_pipeline] * len(reader)
            )

if __name__ == "__main__":

    MAX_RETRIES = 3
    MAX_THREADS = 5
    LOCATION = "us"

    logger.info(f"Crawl starting...")

    ## INPUT ---> List of keywords to scrape
    keyword_list = ["learn rust"]
    aggregate_files = []

    ## Job Processes
    for keyword in keyword_list:
        filename = keyword.replace(" ", "-")

        crawl_pipeline = DataPipeline(filename=f"{filename}.csv")
        scrape_search_results(keyword, LOCATION, data_pipeline=crawl_pipeline, retries=MAX_RETRIES)
        crawl_pipeline.close_pipeline()
        aggregate_files.append(f"{filename}.csv")
    logger.info(f"Crawl complete.")

    metadata_pipeline = DataPipeline("metadata-report.json", output_format="json")
    for file in aggregate_files:
        process_results(file, LOCATION, max_threads=MAX_THREADS, retries=MAX_RETRIES, data_pipeline=metadata_pipeline)