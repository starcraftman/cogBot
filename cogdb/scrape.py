"""
IMPORTANT: Module now deprecated.
    Module is being archived "as is" in case it needs to be reused for another site or replacement goes down.

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
import warnings

import bs4
import selenium
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.webdriver.common.by import By
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
MAT_MERITS = re.compile(r".*Fortification: (\d+) / (\d+).*Undermining: (\d+) / (\d+).*Held Merits:"
                        r"(\d+) stolen and (\d+) held \(\+ (\d+) (.*)")
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

warnings.warn("This module is deprecated in favour of cogdb.spy_squirrel. Archived for now.", stacklevel=3)


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


def click_with_retry(element, *, delay=5, retries=5):
    """Click an element in the page.
    If the click raises an exception retry again after a delay. Retry until all retries used.

    Args:
        element: The element found in the page with the webdriver.
        delay: The delay in seconds if the click fails.
        retries: The number of times to retry on fail.

    Returns: True IFF the element was successfully clicked.
    """
    # Sane minimums on flags.
    max(retries, 1)
    if delay < 0:
        delay = 5

    been_clicked = False
    while retries and not been_clicked:
        try:
            element.click()
            been_clicked = True
        except ElementClickInterceptedException:
            time.sleep(delay)
        finally:
            retries = retries - 1

    return been_clicked


def powerplay_leader(driver, leader_index, *,
                     updated_at=0, held_merits=False):  # pragma: no cover | Depends on river.
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
    click_with_retry(top_box)
    time.sleep(LONG_GAP)

    # Select leader from drop down by index, sometimes delayed
    selects = driver.find_elements(By.CLASS_NAME, "blazored-typeahead__result ")
    click_with_retry(selects[leader_index])
    now = datetime.datetime.utcnow()  # Time is relative when you click leader in list
    time.sleep(LONG_GAP)

    if held_merits:
        for check in driver.find_elements(By.CLASS_NAME, "checkmark"):
            click_with_retry(check)
            time.sleep(SHORT_GAP)

        # Get held merits fro all selected systems of power
        buttons = driver.find_elements(By.CSS_SELECTOR, "button.btn-primary")
        click_with_retry(buttons[0])

        print("Wait start:", str(now))
        while not check_held_recent(driver.page_source, start=now):
            time.sleep(LONGEST_GAP)
        end = datetime.datetime.utcnow()
        print("Wait end:", str(now))
        print("Wait diff:", str(end - now))

    # Parse all information on the page
    info = parse_powerplay_page(driver.page_source, start=now, updated_at=updated_at)

    # Clear the selection for next time.
    clear = driver.find_element(By.CLASS_NAME, "blazored-typeahead__clear")
    click_with_retry(clear)
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
    click_with_retry(ref_button)
    time.sleep(LONGEST_GAP)
    while ref_button.text != "Refine":
        time.sleep(5)

    # Determine the updated_at time for general fort/um data
    now = datetime.datetime.utcnow()
    spans = driver.find_elements(By.TAG_NAME, "span")
    updated_at = int(parse_date("as of" + spans[4].text, start=now).replace(tzinfo=datetime.timezone.utc).timestamp())

    # Expand all blocks for information
    switch = driver.find_element(By.CLASS_NAME, "input-switch")
    click_with_retry(switch)

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

        # If the held merits available parse them out
        mat = MAT_MERITS.match(str(body))
        if mat:
            # N.B. Going from native UTC BACK to UTC timestamp requires becoming aware UTC.
            held_updated_at = int(parse_date(str(body), start=start).replace(tzinfo=datetime.timezone.utc).timestamp())
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


def scrape_all_bgs(driver, systems):  # pragma: no cover | Depends on driver running.
    """Scrape all bgs information for given systems.

    Args:
        driver: The webdriver, probably chrome.
        systems: The name of the systems to push.

    Returns: A large dict of form {system_name: {info: value, info2: value}, ...}
    """
    results = {}
    url = os.path.join(cog.util.CONF.scrape.url, 'bgs')
    driver.get(url)
    time.sleep(LONG_GAP)

    # Push refine button to get latest fort and um data
    for system_name in systems:
        input_box = driver.find_element(By.CLASS_NAME, "blazored-typeahead__input ")
        input_box.send_keys(system_name)
        time.sleep(LONG_GAP)

        # Wait until choices populate
        done = False
        while not done:
            time.sleep(LONG_GAP)
            drop_items = driver.find_elements(By.CLASS_NAME, "blazored-typeahead__result")
            if drop_items:
                done = True
        drop_items[0].click()
        time.sleep(LONG_GAP)

        # Click Fetch button, wait until fetched
        buttons = [x for x in driver.find_elements(By.TAG_NAME, "button") if x.text == "Fetch"]
        buttons[0].click()
        done = False
        while not done:
            time.sleep(LONG_GAP)
            buttons = [x for x in driver.find_elements(By.TAG_NAME, "button") if x.text == "Fetch"]
            if buttons:
                done = True

        # Parse the information
        eles = [system_name] + [x.text for x in driver.find_elements(By.TAG_NAME, "p")]
        results.update(parse_bgs_page(*eles))
        time.sleep(LONG_GAP * 3)

    return results


def parse_bgs_page(system_name, date_p, info_p):
    """Parse the bgs information based on separated out elements.

    Args
        page_source: The text of the bgs page to parse.

    Returns: Parsed information in a dictionary.
    """
    retrieved = re.search(r'Last fetched: (.*?) \(.*\).', date_p).group(1)
    retrieved = datetime.datetime.strptime(retrieved, '%m/%d/%Y %H:%M:%S %Z').replace(tzinfo=datetime.timezone.utc)
    updated_at = re.search(r'Data updated: (.*) \(.*\).', date_p).group(1)
    updated_at = datetime.datetime.strptime(updated_at, '%m/%d/%Y %H:%M:%S %Z').replace(tzinfo=datetime.timezone.utc)

    return {
        system_name: {
            'retrieved': retrieved.timestamp(),
            'updated_at': updated_at.timestamp(),
            'factions': {name: float(inf) for name, inf in re.findall(r'(.*?): ([.0-9]+)%', info_p)},
        }
    }


def main():  # pragma: no cover | Main test code to sanity check with real driver
    """
    Demonstrate a complete parsing of all powerplay information and dump to json file.
    """
    out_file = "/tmp/data.json"

    try:
        # confirm page is up and working BEFORE asking for complete scrape
        urllib.request.urlopen(cog.util.CONF.scrape.url)  # pylint: disable=consider-using-with

        # Run a sanity test, parse entire powerplay page for all leaders.
        with get_chrome_driver(dev=True) as driver:
            data = scrape_all_bgs(driver, ["Sol", "Rana", "Abi"])
            #  data = scrape_all_powerplay(driver, held_merits=False)
            with open(out_file, "w", encoding='utf-8') as fout:
                fout.write(json.dumps(data, sort_keys=True, indent=2))

        print("Parsing all powerplay data is COMPLETE!")
        print(f"JSON info can be found in: {out_file}")

    except urllib.error.URLError:
        logging.getLogger(__name__).error("Site down for now, try again later!")


if __name__ == "__main__":
    main()
