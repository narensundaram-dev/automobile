import os
import re
import sys
import time
import json
import logging
import argparse
import traceback
from datetime import datetime as dt, timedelta as td
from concurrent.futures import as_completed, ThreadPoolExecutor

import pandas as pd
from bs4 import BeautifulSoup, NavigableString

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC


GOOGLE, JUSTDIAL = "google", "justdial"


def get_logger():
    filename = os.path.split(__file__)[-1]
    log = logging.getLogger(filename)
    log_level = logging.INFO
    log.setLevel(log_level)
    log_handler = logging.StreamHandler()
    log_formatter = logging.Formatter('%(levelname)s: %(asctime)s - %(name)s:%(lineno)d - %(message)s')
    log_handler.setFormatter(log_formatter)
    log.addHandler(log_handler)
    return log

log = get_logger()


class Scraper:

    def __init__(self, source, args, settings):
        self.source = source
        self.args = args
        self.settings = settings

        self.url_google = "https://google.com/search?tbm=lcl&q={}"  # https://google.com/search?tbm=lcl&q=JAIN+TYRES
        self.url_justdial = ""

        self.data = []
        self.output_xlsx = f"output_{self.source}.xlsx"

        self.chrome_log_path = '/dev/null' if sys.platform == "linux" else "NUL"
        self.chrome_options = webdriver.ChromeOptions()
        # self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument("--disable-extensions")
        self.chrome_options.add_argument("--disable-gpu")
        self.chrome_options.add_argument("--no-sandbox")

    def get(self):
        df = pd.read_excel("input.xlsx")

        retailers = list(df[df.columns[0]])
        cities = list(df[df.columns[2]])
        count = 1
        workers = self.settings["workers"]["value"]
        with ThreadPoolExecutor(max_workers=workers) as executor:        
            for info in executor.map(self.get_from_google, retailers, cities):
                if info:  # {"retailer": "xxxx", "mobile1": "xxxx", "mobile2": "xxxx"}
                    self.data.append(info)
                if count % 20 == 0:
                    log.info("So far {} has been fetched ...".format(count))
                count += 1

    def get_from_google(self, retailer, city):
        data = {"retailer": retailer, "city": city}
        retailer = re.sub(r"\s*\(.*\)", "", retailer).replace(" ", "+").strip()
        query = f"{retailer}+{city}"

        def get_data_from_card(card):
            span_mobile = card.find("span", class_="rllt__details").find_all("div")[2].get_text().strip()
            match_mobile = re.search(r"\d{5,6}\s\d{5}", span_mobile)
            match_landline = re.search(r"\d{2,3}\s\d{4}\s\d{4}", span_mobile)

            contact_nos = []
            contact_nos.append(match_mobile.group(0)) if match_mobile else ""
            contact_nos.append(match_landline.group(0)) if match_landline else ""
            return contact_nos

        chrome = webdriver.Chrome(self.settings["driver_path"]["value"], chrome_options=self.chrome_options, service_log_path=self.chrome_log_path)
        url = self.url_google.format(query)
        data["url"] = url

        chrome.get(url)
        try:
            wait = self.settings["page_load_timeout"]["value"]
            WebDriverWait(chrome, wait).until(EC.presence_of_element_located((By.CLASS_NAME, "rl_full-list")))
            soup = BeautifulSoup(chrome.page_source, "html.parser")

            cards = soup.select("div.uMdZh.rl-qs-crs-t.mnr-c")
            if len(cards) == 1:
                contact_nos = get_data_from_card(cards[0])
                if contact_nos:
                    data[f"mobile1"] = ",".join(contact_nos)
            else:
                count = 1
                for card in cards:
                    dom_title = card.find("div", role="heading")
                    if dom_title:
                        title = dom_title.get_text().strip()

                        if title.split(" ")[0].lower() == data["retailer"].split("+")[0].lower():
                            contact_nos = get_data_from_card(card)
                            if contact_nos:
                                data[f"mobile{count}"] = ",".join(contact_nos)
                                count += 1

                            if count > 10:
                                break
        except (TimeoutException, Exception) as err:
            pass
        finally:
            chrome.close()
            return data

    def get_from_justdial(self):
        pass

    def save(self):
        df = pd.DataFrame(self.data)
        df.to_excel(self.output_xlsx, index=False)
        log.info("Fetched data has been stored in {} file".format(self.output_xlsx))


def get_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-s', "--source", type=str, choices=(GOOGLE, JUSTDIAL), required=True)
    return arg_parser.parse_args()


def get_settings():
    with open("settings.json", "r") as f:
        return json.load(f)


def main():
    start = dt.now()
    log.info("Script starts at: {}".format(start.strftime("%d-%m-%Y %H:%M:%S %p")))

    args, settings = get_args(), get_settings()

    scraper = Scraper(args.source, args, settings)
    try:
        scraper.get()
    except Exception as e:
        print("Error: ", e)
    finally:
        scraper.save()

    end = dt.now()
    log.info("Script ends at: {}".format(end.strftime("%d-%m-%Y %H:%M:%S %p")))
    elapsed = round(((end - start).seconds / 60), 4)
    log.info("Time Elapsed: {} minutes".format(elapsed))


if __name__ == "__main__":
    main()
