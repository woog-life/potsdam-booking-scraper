import dataclasses
import inspect
import json
import logging
import os
import socket
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Tuple, Optional, Callable, Union, NewType, List, Dict

import pytz
import requests
import urllib3
from bs4 import BeautifulSoup, Tag
from requests import Response
from telegram import Bot

BOOKING_URL = "https://www.blp-shop.de/de/eticket_applications/select_timeslot_list/10/{}/"
# noinspection HttpUrlsUsage
# cluster internal communication
BACKEND_URL = os.getenv("BACKEND_URL") or "http://api:80"
BACKEND_PATH = os.getenv("BACKEND_PATH") or "lake/{}/booking"
UUID = os.getenv("POTSDAM_UUID")
API_KEY = os.getenv("API_KEY")

WATER_INFORMATION = NewType("WaterInformation", Tuple[str, float])


@dataclass
class EventDetails:
    booking_link: str
    begin_time: datetime
    end_time: datetime
    sale_start: datetime
    is_available: bool

    def __repr__(self):
        return f"is_available={self.is_available} ({self.booking_link})"

    def json(self) -> Dict[str, Union[bool, str, int]]:
        result: Dict[str, Union[bool, str, int]] = {}
        for key, value in dataclasses.asdict(self).items():
            if isinstance(value, datetime):
                result[key] = f"{value.isoformat()}Z"
            else:
                result[key] = value
        return result


def _utc(input_time: datetime) -> datetime:
    naive_time = input_time.replace(tzinfo=None)
    input_tz = pytz.timezone("Europe/Berlin")
    local_time = input_tz.localize(naive_time)
    utc_time = local_time.astimezone(pytz.utc)
    return utc_time.replace(tzinfo=None)


def create_logger(name: str, level: int = logging.DEBUG) -> logging.Logger:
    logger = logging.Logger(name)
    ch = logging.StreamHandler(sys.stdout)

    formatting = "[{}] %(asctime)s\t%(levelname)s\t%(module)s.%(funcName)s#%(lineno)d | %(message)s".format(name)
    formatter = logging.Formatter(formatting)
    ch.setFormatter(formatter)

    logger.addHandler(ch)
    logger.setLevel(level)

    return logger


def send_telegram_alert(message: str, token: str, chatlist: List[str]):
    logger = create_logger(inspect.currentframe().f_code.co_name)
    if not token:
        logger.error("TOKEN not defined in environment, skip sending telegram message")
        return

    if not chatlist:
        logger.error("chatlist is empty (env var: TELEGRAM_CHATLIST)")

    for user in chatlist:
        Bot(token=token).send_message(chat_id=user, text=f"Error while executing: {message}")


def get_website(date: str) -> Tuple[str, bool]:
    logger = create_logger(inspect.currentframe().f_code.co_name)
    url = BOOKING_URL.format(date)

    logger.debug(f"Requesting {url}")
    response = requests.get(url)

    content = response.content.decode("ISO-8859-1")
    logger.debug(content)

    return content, response.status_code == 200


def parse_website_xml(xml: str) -> BeautifulSoup:
    return BeautifulSoup(xml, "html.parser")


def extract_table_row(html: BeautifulSoup):
    logger = create_logger(inspect.currentframe().f_code.co_name)

    table = html.find("table")
    if not table:
        logger.error(f"table not found in html {html}")
        return None

    rows = table.find_all("tr")
    if not rows or len(rows) < 2:
        logger.error(f"tr not found or len(rows) < 2 in {table}")
        return None

    try:
        for idx, row in enumerate(rows):
            columns = row.find_all("td")
            if columns:
                return row
    except IndexError:
        pass

    logger.error("Couldn't find a row for bookings")
    return None


def get_tag_text_from_xml(xml: Union[BeautifulSoup, Tag], name: str, conversion: Callable) -> Optional:
    tag = xml.find(name)

    if not tag:
        return None

    return conversion(tag.text)


def get_booking_information(soup: BeautifulSoup, date: str) -> Optional[Tuple[datetime, datetime, bool, str]]:
    logger = create_logger(inspect.currentframe().f_code.co_name)
    start_slot_col = soup.find("td", attrs={"data-title": "Von"})
    end_slot_col = soup.find("td", attrs={"data-title": "Bis"})
    available_slots_col = soup.find("td", attrs={"data-title": "Freie E-Tickets"})
    childs = list(soup.children)
    booking_link_a = [td.find("a", attrs={"title": "Zur Tarifauswahl"}) for td in soup.find_all("td") if
                      td.find("a")]

    if not (start_slot_col and end_slot_col and available_slots_col):
        logger.error(f"{start_slot_col}, {end_slot_col}, {available_slots_col}, {booking_link_a})")
        return None

    time = datetime.strptime(f"{date} {start_slot_col.text.strip()}", "%d.%m.%Y %H:%M Uhr")
    start_slot = _utc(time)
    time = datetime.strptime(f"{date} {end_slot_col.text.strip()}", "%d.%m.%Y %H:%M Uhr")
    end_slot = _utc(time)

    is_available = booking_link_a and not "ausverkauft" in available_slots_col.text.lower()
    if not is_available:
        is_available = False
    booking_link = booking_link_a[0].get("href") if is_available else "https://not.available"

    # noinspection PyTypeChecker
    # at this point pycharm doesn't think that the return type can be optional despite the many empty returns beforehand
    return start_slot, end_slot, is_available, booking_link


def send_data_to_backend(variation: str, details: List[EventDetails]) -> Tuple[
    Optional[Response], str]:
    logger = create_logger(inspect.currentframe().f_code.co_name)
    path = BACKEND_PATH.format(UUID)
    url = "/".join([BACKEND_URL, path])

    try:
        body = {
            "variation": "Stadtbad Babelsberg",
            "events": [event.json() for event in details],
        }
        response = requests.put(
            url,
            json=body,
            headers={"Authorization": f"Bearer {API_KEY}"}
        )
        logger.debug(f"success: {response.ok} | content: {response.content}")
    except (requests.exceptions.ConnectionError, socket.gaierror, urllib3.exceptions.MaxRetryError):
        logger.exception(f"Error while connecting to backend ({url})", exc_info=True)
        return None, url

    return response, url


def main() -> Tuple[bool, str]:
    if not UUID:
        root_logger.error("POTSDAM_UUID not defined in environment")
        return False, "POTSDAM_UUID not defined"
    elif not API_KEY:
        root_logger.error("API_KEY not defined in environment")
        return False, "API_KEY not defined"

    logger = create_logger(inspect.currentframe().f_code.co_name)
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    sale_start_time = _utc(today)
    details = []

    for i in range(0):
        date = today + timedelta(days=i)
        content, success = get_website(date.strftime("%Y-%m-%d"))
        if not success:
            message = f"Couldn't retrieve website: {content}"
            logger.error(message)
            return False, message

        soup = parse_website_xml(content)
        booking_row = extract_table_row(soup)
        if not booking_row:
            logger.error("Couldn't find correct row")
            return False, "Couldn't find correct row"

        booking_information = get_booking_information(booking_row, date.strftime("%d.%m.%Y"))

        if not booking_information:
            message = f"Couldn't retrieve water information from {soup}"
            logger.error(message)
            return False, message

        start_time, end_time, is_available, booking_link = booking_information
        detail = EventDetails(booking_link=booking_link,
                              begin_time=start_time,
                              end_time=end_time,
                              sale_start=sale_start_time,
                              is_available=is_available)
        details.append(detail)

    response, generated_backend_url = send_data_to_backend("Stadtbad Babelsberg", details)

    if not response or not response.ok:
        message = f"Failed to put data 'variation': 'Stadtbad Badelsberg', 'events': {details}) to backend: {generated_backend_url}\n{response.content}"
        logger.error(message)
        return False, message

    return True, ""


root_logger = create_logger("__main__")

success, message = main()
if not success:
    root_logger.error(f"Something went wrong ({message})")
    token = os.getenv("TOKEN")
    chatlist = os.getenv("TELEGRAM_CHATLIST") or "139656428"
    send_telegram_alert(message, token=token, chatlist=chatlist.split(","))
    sys.exit(1)
