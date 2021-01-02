from bs4 import BeautifulSoup as bs
from time import sleep
from urllib.request import Request, urlopen
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
# from pyvirtualdisplay import Display
from datetime import timedelta

import pandas as pd
import numpy as np
import os
import urllib.request
import string
import re
import ssl
import os
import shutil
import tempfile
import boto3
import datetime as dt
import sys
import argparse
import logging

ssl._create_default_https_context = ssl._create_unverified_context
driver_path = 'c:/Users/email//tinker/brian_dwh/drivers/chromedriver_linux_87'


chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')

def parse_text(unparsed):
    try:
        parsed = unparsed.get_text().lstrip('\n\r\n').lstrip('\n\r\n ').rstrip('\n\r\n').rstrip('\n\r\n ').strip(' ')
    except AttributeError:
        parsed = 0
    return parsed
def parse_tag(unparsed, lvl):
    try:
        parsed = str(unparsed).split('="')[lvl].split('">')[0].split('"')[0]
    except AttributeError:
        parsed = 0
    return parsed
def get_game_links_soup(gamer_id):
    gamer = gamer_id.replace('#', '%23')
    game_links_url = f'https://cod.tracker.gg/warzone/profile/battlenet/{gamer}/matches'
    driver = webdriver.Chrome(executable_path=driver_path, chrome_options=chrome_options)
    driver.get(game_links_url)
    # need to sleep for 5 sec to let page finish loading before pulling dynamic content
    sleep(5)
    game_links_html = driver.page_source
    game_links_soup = bs(game_links_html, 'lxml')
    driver.close()
    return game_links_soup

gamer_id = 'gs25#11901'
game_links_soup = get_game_links_soup(gamer_id)
games_by_day = game_links_soup.findAll('div', attrs={'class': 'trn-gamereport-list__group'})
games_details_all = pd.DataFrame()
games_most_recent = games_by_day[0].findAll('div', attrs={'class':'match__row'})
games_prior = games_by_day[1].findAll('div', attrs={'class':'match__row'})
