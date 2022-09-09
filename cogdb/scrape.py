"""
Scrape the json site. This is a stopgap.

    0) Remove all "chromium" packages, driver needs chrome
    1) Install google chrome
        wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
    2) Fetch corresponding chromedriver
        https://chromedriver.chromium.org/downloads
    3) Configure 'scrape.url' and 'scrape.driver' in the config.
    4) Ensure you have required pip packages, run: python setup.py deps
"""
import contextlib
import datetime
import json
import logging
import os
import re
import time
import urllib.request

import bs4
import selenium
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

import cog.util
import cogdb
import cogdb.eddb
from cogdb.eddb import Power


# All gaps in seconds
SHORT_GAP = 0.1
LONG_GAP = 2
LONGEST_GAP = 15
# Regex below
# Match a line for held merits where it isn't unknown
MAT_MERITS = re.compile(r".*Fortification: (\d+) / (\d+).*Undermining: (\d+) / (\d+).*Held Merits: (\d+) stolen and (\d+) held \(\+ (\d+) (.*)")
# Match a line for held merits where it is unknown
MAT_UKNOWN = re.compile(r".*Fortification: (\d+) / (\d+).*Undermining: (\d+) / (\d+).*Held Merits: unknown")
# Matchers to parse time out of the held merits line
MAT_TIME = {
    'seconds': re.compile(r'(\d+) seconds?'),
    'minutes': re.compile(r'(\d+) minutes?'),
    'hours': re.compile(r'(\d+) hours?'),
    'days': re.compile(r'(\d+) days?'),
}
HELD_MERITS_RECENT = 60 * 60  # Within last hour


@contextlib.contextmanager
def get_chrome_driver(dev=True):  # pragma: no cover | Just a context wrapper around library startup
    """Initialize the chrome webdriver.

    This is a context object, use with with.

    Args:
        dev: When False, will run in headless mode. Otherwise GUI is run.
    """
    driver = None
    try:
        options = Options()
        options.add_argument("--window-size=1920x1080")
        if not dev:
            options.headless = True

        service = Service(ChromeDriverManager().install())
        driver = selenium.webdriver.Chrome(service=service, options=options)
        yield driver
    finally:
        if driver:
            driver.quit()


def powerplay_names():
    """The names of all powerplay leaders.

    Returns: A list of all powerplay leaders sorted alphabetically.
    """
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        powers = eddb_session.query(Power.text).\
            filter(Power.text != "None").\
            order_by(Power.text).\
            all()
        return [x[0] for x in powers]


def parse_date(text, *, start):
    """Parse date from powerplay page source block.

    The date is relatively in past (i.e. of format x days, y minutes ago).

    Args:
        text: The text block to extract date info from.
        start: The starting datetime object (should be utc).

    Returns: A datetime offset from start that is time this info was updated, 0 if no info present.
    """
    if ": unknown" in text or "as of" not in text:
        return 0

    text = text[text.rindex("as of"):]
    kwargs = {}

    for name, mat in MAT_TIME.items():
        found = mat.findall(text)
        kwargs[name] = int(found[0]) if found else 0

    return start - datetime.timedelta(**kwargs)


def powerplay_leader(driver, leader_index, *, updated_at=0, held_merits=False):
    """Scrape the powerplay page for the spy site.

    Expectation: Driver is already loaded and pointed at powerplay page.

    Args:
        driver: A webdriver, should be chrome.
        leader_index: The index of the leader you want to parse, 1 index
        held_merits: If True, fetch held merits for all systems on page.
                     Estimate, 30s a system. Will block and return once all fetched.
    """
    # Fetch the page for named leader
    top_box = driver.find_element(By.CLASS_NAME, "blazored-typeahead__input-icon")
    top_box.click()
    time.sleep(LONG_GAP)

    # Select leader from drop down by index
    selects = driver.find_elements(By.CLASS_NAME, "blazored-typeahead__result ")
    selects[leader_index].click()
    now = datetime.datetime.utcnow()  # Time is relative when you click leader in list
    time.sleep(LONG_GAP)

    if held_merits:
        for check in driver.find_elements(By.CLASS_NAME, "checkmark"):
            check.click()
            time.sleep(SHORT_GAP)

        # Get held merits fro all selected systems of power
        buttons = driver.find_elements(By.CSS_SELECTOR, "button.btn-primary")
        buttons[0].click()

        print("Wait start:", str(now))
        while not check_held_recent(driver.page_source, now):
            time.sleep(LONGEST_GAP)
        end = datetime.datetime.utcnow()
        print("Wait end:", str(now))
        print("Wait diff:", str(end - now))

    # Parse all information on the page
    info = parse_powerplay_page(driver.page_source, start=now, updated_at=updated_at)

    # Clear the selection for next time.
    clear = driver.find_element(By.CLASS_NAME, "blazored-typeahead__clear")
    clear.click()
    time.sleep(LONG_GAP)

    return info


def scrape_all_powerplay(driver, held_merits=False):  # pragma: no cover | Depends on driver running.
    """Scrape all powerplay factions.

    Args:
        driver: The webdriver, probably chrome.
        held_merits: If True, fetch held merits for all powerplay leaders.
                     WARNING: VERY SLOW
    """
    url = os.path.join(cog.util.CONF.scrape.url, 'powerplay')
    driver.get(url)
    time.sleep(LONG_GAP)

    # Push refine button to get latest fort and um data
    buttons = driver.find_elements(By.CSS_SELECTOR, "button.btn-primary")
    ref_button = buttons[-1]
    ref_button.click()
    time.sleep(LONGEST_GAP)
    while ref_button.text != "Refine":
        time.sleep(5)

    # Determine the updated_at time for general fort/um data
    now = datetime.datetime.utcnow()
    spans = driver.find_elements(By.TAG_NAME, "span")
    updated_at = int(parse_date("as of" + spans[4].text, start=now).timestamp())

    # Expand all blocks for information
    switch = driver.find_element(By.CLASS_NAME, "input-switch")
    switch.click()

    return {
        leader: powerplay_leader(driver, ind, held_merits=held_merits, updated_at=updated_at)
        for ind, leader in enumerate(powerplay_names())
    }


def check_held_recent(page_source, *, start):
    """Check if any of the outstanding requests are still unknown or too old.

    A recent held merits is present and less than 1 hour old.

    Args:
        page_source: The page source, a string.
        start: The start timestamp of page load.

    Returns: Boolean True iff all parts of page are showing recent held.
    """
    soup = bs4.BeautifulSoup(page_source, 'html.parser')

    for body in soup.find_all("div", {"class": "accordion-body"}):
        text = str(body)
        if MAT_UKNOWN.match(text):
            return False

        updated_at = parse_date(text, start=start)
        if (start - updated_at).total_seconds() > HELD_MERITS_RECENT:
            return False

    return True


def parse_powerplay_page(page_source, *, start, updated_at):
    """Parse the systems and information from the current powerplay page.

    Args:
        page_source: The page source, a string.
        start: The start timestamp of page load.

    Return: A dictionary of form: system_name -> info dict.
    """
    soup = bs4.BeautifulSoup(page_source, 'html.parser')

    # Find all names of the systems
    headers = soup.find_all("h2", {"class": "accordion-header"})
    systems = [{"system_name": header.text.strip()} for header in headers]

    # Extract info by regex from all expanded info blocks
    bodies = soup.find_all("div", {"class": "accordion-body"})
    for ind, body in enumerate(bodies):
        info = {
            'held_merits': 0,
            'held_updated_at': 0,
        }

        print('start', int(start.timestamp()))
        held_updated_at = parse_date(str(body), start=start).timestamp()
        print('held at', held_updated_at)

        # If the held merits available parse them out
        mat = MAT_MERITS.match(str(body))
        if mat:
            info = {
                'held_merits': int(mat.group(5)) + int(mat.group(6)),
                'held_updated_at': held_updated_at,
            }
        else:  # Held merits were not available.
            logging.getLogger(__name__).error("Held Merits missing: %s", systems[ind]['system_name'])
            mat = MAT_UKNOWN.match(str(body))

        info.update({
            "fort": int(mat.group(1)),
            "fort_trigger": int(mat.group(2)),
            "um": int(mat.group(3)),
            "um_trigger": int(mat.group(4)),
            "updated_at": updated_at,
        })
        systems[ind].update(info)

    return {x['system_name']: x for x in systems}


def main():
    """
    Demonstrate a complete parsing of all powerplay information and dump to json file.
    """
    out_file = "/tmp/data.json"

    try:
        # confirm page is up and working BEFORE asking for complete scrape
        urllib.request.urlopen(cog.util.CONF.scrape.url)

        # Run a sanity test, parse entire powerplay page for all leaders.
        with get_chrome_driver(dev=True) as driver:
            data = scrape_all_powerplay(driver, held_merits=False)
            with open(out_file, "w") as fout:
                fout.write(json.dumps(data, sort_keys=True, indent=2))

        print("Parsing all powerplay data is COMPLETE!")
        print(f"JSON info can be found in: {out_file}")

    except urllib.error.URLError:
        logging.getLogger(__name__).error("Site down for now, try again later!")


if __name__ == "__main__":
    main()
