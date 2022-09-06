"""
Scrape the json site. This is a stopgap.
"""
import os

import bs4
#  import scrapy
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options

import cog.util

DRIVER_PATH = "/home/starcraftman/Downloads/chromedriver"
# Must be actual google chrome project
#   wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb


def powerplay_for(driver, name):
    # Fetch the page for named leader
    top_box = driver.find_element_by_class_name("blazored-typeahead__input")
    # Clear input first?
    top_box.send_keys(name)
    top_box.send_keys(Keys.RETURN)

    # select all boxes and then fetch
    for box in driver.find_elements_by_class_name("checkmark"):
        box.click()

    print(bs4.BeautifulSoup(driver.page_source, 'html.parser').prettify())


def main():
    try:
        url = os.path.join(cog.util.CONF.scrape, 'powerplay')
        options = Options()
        #  options.headless = True
        #  options.add_argument("--windo-size=960x960")
        driver = webdriver.Chrome(executable_path=DRIVER_PATH, options=options)
        driver.get(url)

        leader = "Zachary Hudson"
        powerplay_for(driver, leader)

        # Fetch the page for hudson
        driver.get(url)
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
