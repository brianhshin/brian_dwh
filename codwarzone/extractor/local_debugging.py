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

driver_path = '/home/ubuntu/brian_dwh/drivers/chromedriver_linux_87'

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

# game_date = games_by_day[0].findAll('h3', attrs={'class': 'trn-gamereport-list__title'})[0]

print(games_by_day[0].findAll('h3', attrs={'class': 'data-v-43c59a99'}))
# for x in range(len(games_most_recent)):
#     game_details_df = get_game_details_for_day(games_most_recent, x)
#     game_date = games_by_day[0].findAll('h3', attrs={'class': 'trn-gamereport-list__title'})[0]
#     game_date_parsed = parse_text(game_date)
#     game_details_df.insert(loc=1, column='game_date', value=game_date_parsed)
#     games_details_all = pd.concat([games_details_all, game_details_df])

# for x in range(len(games_prior)):
#     game_details_df = get_game_details_for_day(games_prior, x)
#     game_date = games_by_day[1].findAll('h3', attrs={'class': 'trn-gamereport-list__title'})[0]
#     game_date_parsed = parse_text(game_date)
#     game_details_df.insert(loc=1, column='game_date', value=game_date_parsed)
#     games_details_all = pd.concat([games_details_all, game_details_df])

# # date_col = games_details_all.pop('game_date')
# # games_details_all.insert(loc=1, column='game_date', value=date_col)
# games_details_all.insert(loc=2, column='gamer_id', value=gamer_id)
# games_details_all['game_type'] = np.where(
#     games_details_all['game_type'] == '',
#     'Other',
#     games_details_all['game_type'])
# games_details_all.reset_index(inplace=True, drop=True)


# print(games_most_recent)
# print(games_prior)