"""
author: Brian Shin
email: brianhesungshin@gmail.com
"""

"""
  usage: python warzone_scraper.py [gamer_id (default is gs25#11901)]

  The purpose of this assignment is to scrape a warzone tracker profile with games.
  This is a local version, so this will write to local csv's instead of my S3 bucket.
  The dataframes get written to a local file warzone/dataframe/dataframe_date.csv.
  (ex: warzone/profile/profile_20200803.csv).

"""

from bs4 import BeautifulSoup as bs
from time import sleep
from urllib.request import Request, urlopen
from selenium import webdriver
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

pd.set_option('display.float_format', lambda x: '%.3f' % x)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)

# display = Display(visible=0, size=(800, 800))
# display.start()

today = dt.datetime.now().strftime("%Y%m%d")
today_id = dt.datetime.now().strftime("%Y-%m-%d")
yesterday_id = (dt.datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# log = logging.getLogger(__name__)
s3_bucket = 'codwarzone'
# session = boto3.Session(
#             aws_access_key_id='',
#             aws_secret_access_key='')


s3 = boto3.client(
    's3',
    aws_access_key_id=os.environ['aws_access_key_id'],
    aws_secret_access_key=os.environ['aws_secret_access_key'])

# local_driver_path = '/Users/brianshin/brian/tinker/drivers/chromedriver_mac'
# os.chdir('/home/ubuntu')

driver_path = '/home/ubuntu/brian_dwh/drivers/chromedriver_linux'

################################################################################
# for loading dataframes to s3 bucket
def s3_upload_file(data, bucket, filepath):
    # logging.info(f'Exporting contents to file: {filepath}')
    # s3_resource = session.resource('s3')
    # obj = s3_resource.Object(bucket, filepath).put(Body=data)
    s3.upload_file(data, bucket, filepath)
    print(f'uploaded file as {bucket}/{filepath}')

    return

# gets text values from whatever you parse with beautifulsoup
def parse_text(unparsed):

    try:
        parsed = unparsed.get_text().lstrip('\n\r\n').lstrip('\n\r\n ').rstrip('\n\r\n').rstrip('\n\r\n ').strip(' ')
    except AttributeError:
        parsed = 0

    return parsed


# parses values from whatever tags you parse and how many levels down
def parse_tag(unparsed, lvl):

    try:
        parsed = str(unparsed).split('="')[lvl].split('">')[0].split('"')[0]
    except AttributeError:
        parsed = 0

    return parsed


# takes in the gamer_id and requests/pulls the soup using urllib with mozilla driver
def get_profile_soup(gamer_id):

    gamer = gamer_id.replace('#', '%23')
    profile_url = f'https://cod.tracker.gg/warzone/profile/battlenet/{gamer}/overview'
    hdr = {'User-Agent': 'Mozilla/5.0'}
    profile_req = Request(profile_url,headers=hdr)
    profile_html = urlopen(profile_req)
    profile_soup = bs(profile_html, 'lxml')

    return profile_soup


# takes in the gamer_id and requests/pulls the soup using selenium with chrome driver
def get_game_links_soup(gamer_id):

    gamer = gamer_id.replace('#', '%23')
    game_links_url = f'https://cod.tracker.gg/warzone/profile/battlenet/{gamer}/matches'
    driver = webdriver.Chrome(executable_path=driver_path)
    driver.get(game_links_url)
    # need to sleep for 5 sec to let page finish loading before pulling dynamic content
    sleep(5)
    game_links_html = driver.page_source
    game_links_soup = bs(game_links_html, 'lxml')
    driver.close()

    return game_links_soup


# takes in a game url and requests/pulls the soup using selenium with chrome driver
def get_game_soup(game_url):

    driver = webdriver.Chrome(executable_path=driver_path)
    driver.get(game_url)
    # need to sleep for 5 sec to let page finish loading before pulling dynamic content
    sleep(5)
    game_html = driver.page_source
    game_soup = bs(game_html, 'lxml')
    driver.close()

    return game_soup
################################################################################

# takes in the profile soup and parses the data as a snapshot dataframe for the day
def get_profile_data(profile_soup, gamer_id):

    playtime = profile_soup.find('span', attrs={'class':'playtime'})
    playtime_parsed = parse_text(playtime)

    games = profile_soup.find('span', attrs={'class':'matches'})
    games_parsed = parse_text(games)

    level = profile_soup.find('div', attrs={'class':'highlight-text'})
    level_parsed = parse_text(level).split('\n')[0]

    level_rank = profile_soup.find('div', attrs={'class':'highlight-text'})
    level_rank_parsed = parse_text(level).split('\n')[1].strip(' ')

    wins = profile_soup.findAll('div', attrs={'class':'numbers'})[0].find('span', attrs={'class':'value'})
    wins_parsed = parse_text(wins)

    wins_rank = profile_soup.findAll('div', attrs={'class':'numbers'})[0].find('span', attrs={'class':'rank'})
    wins_rank_parsed = parse_text(wins_rank)

    top5 = profile_soup.findAll('div', attrs={'class':'numbers'})[1].find('span', attrs={'class':'value'})
    top5_parsed = parse_text(top5)

    top5_rank = profile_soup.findAll('div', attrs={'class':'numbers'})[1].find('span', attrs={'class':'rank'})
    top5_rank_parsed = parse_text(top5_rank)

    kd = profile_soup.findAll('div', attrs={'class':'numbers'})[2].find('span', attrs={'class':'value'})
    kd_parsed = parse_text(kd)

    kd_rank = profile_soup.findAll('div', attrs={'class':'numbers'})[2].find('span', attrs={'class':'rank'})
    kd_rank_parsed = parse_text(kd_rank)

    dmg_per_game = profile_soup.findAll('div', attrs={'class':'numbers'})[3].find('span', attrs={'class':'value'})
    dmg_per_game_parsed = parse_text(dmg_per_game)

    top10 = profile_soup.findAll('div', attrs={'class':'numbers'})[4].find('span', attrs={'class':'value'})
    top10_parsed = parse_text(top10)

    top10_rank = profile_soup.findAll('div', attrs={'class':'numbers'})[4].find('span', attrs={'class':'rank'})
    top10_rank_parsed = parse_text(top10_rank)

    top25 = profile_soup.findAll('div', attrs={'class':'numbers'})[5].find('span', attrs={'class':'value'})
    top25_parsed = parse_text(top25)

    top25_rank = profile_soup.findAll('div', attrs={'class':'numbers'})[5].find('span', attrs={'class':'rank'})
    top25_rank_parsed = parse_text(top25_rank)

    kills = profile_soup.findAll('div', attrs={'class':'numbers'})[6].find('span', attrs={'class':'value'})
    kills_parsed = parse_text(kills)

    deaths = profile_soup.findAll('div', attrs={'class':'numbers'})[7].find('span', attrs={'class':'value'})
    deaths_parsed = parse_text(deaths)

    downs = profile_soup.findAll('div', attrs={'class':'numbers'})[8].find('span', attrs={'class':'value'})
    downs_parsed = parse_text(downs)

    avg_life = profile_soup.findAll('div', attrs={'class':'numbers'})[9].find('span', attrs={'class':'value'})
    avg_life_parsed = parse_text(avg_life)

    avg_life_rank = profile_soup.findAll('div', attrs={'class':'numbers'})[9].find('span', attrs={'class':'rank'})
    avg_life_rank_parsed = parse_text(avg_life_rank)

    score = profile_soup.findAll('div', attrs={'class':'numbers'})[10].find('span', attrs={'class':'value'})
    score_parsed = parse_text(score)

    score_per_min = profile_soup.findAll('div', attrs={'class':'numbers'})[11].find('span', attrs={'class':'value'})
    score_per_min_parsed = parse_text(score_per_min)

    score_per_min_rank = profile_soup.findAll('div', attrs={'class':'numbers'})[11].find('span', attrs={'class':'rank'})
    score_per_min_rank_parsed = parse_text(score_per_min_rank)

    score_per_game = profile_soup.findAll('div', attrs={'class':'numbers'})[12].find('span', attrs={'class':'value'})
    score_per_game_parsed = parse_text(score_per_game)

    cash = profile_soup.findAll('div', attrs={'class':'numbers'})[13].find('span', attrs={'class':'value'})
    cash_parsed = parse_text(cash)

    contracts = profile_soup.findAll('div', attrs={'class':'numbers'})[14].find('span', attrs={'class':'value'})
    contracts_parsed = parse_text(contracts)

    contracts_rank = profile_soup.findAll('div', attrs={'class':'numbers'})[14].find('span', attrs={'class':'rank'})
    contracts_rank_parsed = parse_text(contracts_rank)

    win_perc = profile_soup.findAll('div', attrs={'class':'numbers'})[15].find('span', attrs={'class':'value'})
    win_perc_parsed = parse_text(win_perc)

    win_perc_rank = profile_soup.findAll('div', attrs={'class':'numbers'})[15].find('span', attrs={'class':'rank'})
    win_perc_rank_parsed = parse_text(win_perc_rank)

    profile_id = today_id.replace('-', '')+'_'+gamer_id.replace('#', '_')

    profile_dict = {'profile_id': [profile_id],
                    'profile_date': [today_id],
                    'gamer_id': [gamer_id],
                    'playtime': [playtime_parsed],
                    'games': [games_parsed],
                    'level': [level_parsed],
                    'level_rank': [level_rank_parsed],
                    'wins': [wins_parsed],
                    'wins_rank': [wins_rank_parsed],
                    'kd': [kd_parsed],
                    'kd_rank': [kd_rank_parsed],
                    'dmg_per_game': [dmg_per_game_parsed],
                    'top5': [top5_parsed],
                    'top5_rank': [top5_rank_parsed],
                    'top10': [top10_parsed],
                    'top10_rank': [top10_rank_parsed],
                    'top25': [top25_parsed],
                    'top25_rank': [top25_rank_parsed],
                    'kills': [kills_parsed],
                    'deaths': [deaths_parsed],
                    'downs': [downs_parsed],
                    'avg_life': [avg_life_parsed],
                    'avg_life_rank': [avg_life_rank_parsed],
                    'avg_life_rank': 0,
                    'score': [score_parsed],
                    'score_per_min': [score_per_min_parsed],
                    'score_per_min_rank': [score_per_min_rank_parsed],
                    'score_per_game': [score_per_game_parsed],
                    'cash': [cash_parsed],
                    'contracts': [contracts_parsed],
                    'contracts_rank': [contracts_rank_parsed],
                    'win_perc': [win_perc_parsed],
                    'win_perc_rank': [win_perc_rank_parsed]}

    profile_df = pd.DataFrame(profile_dict)
    profile_df.reset_index(inplace=True, drop=True)

    return profile_df


# takes in the games list page soup and parses the game url's into a list
def get_game_links(game_links_soup):


    games = game_links_soup.findAll('div', attrs={'class': 'trn-gamereport-list__group'})
    games_most_recent = games[0].findAll('div', attrs={'class':'match__row'})
    games_prior = games[1].findAll('div', attrs={'class':'match__row'})

    games_most_recent_len = len(games_most_recent)
    games_prior_len = len(games_prior)
    games_list = []

    for i in range(games_most_recent_len):
        game = games_most_recent[i].find('a')['href']
        games_list.append(game)

    for i in range(games_prior_len):
        game = games_prior[i].find('a')['href']
        games_list.append(game)

    return games_list


def get_game_details_for_day(day_soup, day_index):

    game_id = day_soup[day_index].find('a')['href'].split('match/')[1].split('?')[0]
    game_placement = day_soup[day_index].find('div', attrs={'class': 'match__placement'})
    game_placement_parsed = parse_text(game_placement)
    game_type = day_soup[day_index].find('span', attrs={'class':'match__name'})
    game_type_parsed = parse_text(game_type)
    game_time = day_soup[day_index].find('span', attrs={'class':'match__time'})
    game_time_parsed = parse_text(game_time)
    game_details = day_soup[day_index].find('div', attrs={'class':'match__row-stats'}).findAll('span', attrs={'class': 'value'})
    game_kills = parse_text(game_details[0])
    if len(game_details) < 4:
        game_cache_open = np.nan
    else:
        game_cache_open = parse_text(game_details[1])
    game_dmg = parse_text(game_details[-2])
    game_dmg_per_min = parse_text(game_details[-1])

    game_details_dict = {'game_type': [game_type_parsed],
                    'game_time': [game_time_parsed],
                    'placement': [game_placement_parsed],
                    'kills': [game_kills],
                    'cache_open': [game_cache_open],
                    'damage': [game_dmg],
                    'damage_per_min': [game_dmg_per_min]}

    game_details_df = pd.DataFrame(game_details_dict)
    game_details_df.insert(loc=0, column='game_id', value=game_id)

    return game_details_df


# to get games from today and yesterday to cover all bases since i usually play around midnight
def get_game_details(game_links_soup, gamer_id):

    games_by_day = game_links_soup.findAll('div', attrs={'class': 'trn-gamereport-list__group'})

    games_details_all = pd.DataFrame()
    # all games from most recent day
    games_most_recent = games_by_day[0].findAll('div', attrs={'class':'match__row'})
    # all games for prior day
    games_prior = games_by_day[1].findAll('div', attrs={'class':'match__row'})

    for x in range(len(games_most_recent)):
        game_details_df = get_game_details_for_day(games_most_recent, x)
        game_date = games_by_day[0].findAll('h3', attrs={'class': 'trn-gamereport-list__title'})[0]
        game_date_parsed = parse_text(game_date)
        game_details_df.insert(loc=1, column='game_date', value=game_date_parsed)
        games_details_all = pd.concat([games_details_all, game_details_df])

    for x in range(len(games_prior)):
        game_details_df = get_game_details_for_day(games_prior, x)
        game_date = games_by_day[1].findAll('h3', attrs={'class': 'trn-gamereport-list__title'})[0]
        game_date_parsed = parse_text(game_date)
        game_details_df.insert(loc=1, column='game_date', value=game_date_parsed)
        games_details_all = pd.concat([games_details_all, game_details_df])

    # date_col = games_details_all.pop('game_date')
    # games_details_all.insert(loc=1, column='game_date', value=date_col)
    games_details_all.insert(loc=2, column='gamer_id', value=gamer_id)
    games_details_all['game_type'] = np.where(
        games_details_all['game_type'] == '',
        'Other',
        games_details_all['game_type'])
    games_details_all.reset_index(inplace=True, drop=True)

    return games_details_all


# used in get_game_data and is iterated on the len of how many game metrics there are to be kd_parsed
# takes in a metric and parses the data into a tuple like ex: ('kills': '5')
def get_game_stats_data(game_stats, i):

    stat = parse_tag(game_stats[i], 5)
    value = parse_text(game_stats[i].find('span', attrs={'class': 'value'}))

    return stat, value

################################################################################
# takes in a game url and pulls the soup with get_game_soup and parses the data into a dataframe
def get_game_data(game):

    game_url = f'https://cod.tracker.gg{game}'
    game_id = game.split('match/')[1].split('?')[0]
    # takes in a game url and requests/pulls the soup using selenium with chrome driver
    game_soup = get_game_soup(game_url)

    # parse timestamp of the game and split into date and time
    # game_timestamp = game_soup.find('span', {'class': 'time'})
    # game_time = parse_text(game_timestamp).split(', ')[1]
    # game_date = parse_text(game_timestamp).split(',')[0]
    # game_date_parsed = dt.datetime.strptime(game_date, "%m/%d/%Y").strftime("%Y-%m-%d")
    game_stats_soup = game_soup.find('div', {'class':'player__stats'})

    # try statement to parse the data from a given game bc sometimes the page request fails
    try:
        game_stats = game_stats_soup.findAll('div', attrs={'class': 'numbers'})
        # create empty stats list to be populated by the specific parsed game stats
        stats_list = []
        print('stats to be parsed len:', len(game_stats))
        # iterate on each metric to pull the data as a tuple
        for i in range(0, len(game_stats)):
            stat, val = get_game_stats_data(game_stats, i)
            stats_list.append((stat, val))

        # create empty stats dict to add each game stat to it (diff games have diff # of stats. thanks activision)
        game_stats_dict = {
            # 'game_date': game_date_parsed,
            # 'game_time': game_time,
            'game_id': game_id}
        for stat in stats_list:
            game_stats_dict.update({stat[0]: [stat[1]]})

        game_stats_df = pd.DataFrame(game_stats_dict)

    except AttributeError:

        print('bad game page. thanks activision ¯\_(ツ)_/¯')

        game_stats_dict = {
            # 'game_date': game_date_parsed,
            # 'game_time': game_time,
            'game_id': game_id}

        game_stats_df = pd.DataFrame(game_stats_dict)

    return game_stats_df


################################################################################
# zhu li, do the thing
def parse_warzone_tracker(gamer_id):

    start_time = dt.datetime.now()
    print('script started at:', start_time)

    # gamer_id = 'gs25#11901'

    # pull profile soup
    profile_soup = get_profile_soup(gamer_id)
    # parse profile data
    profile_df = get_profile_data(profile_soup, gamer_id)
    # parse games page soup
    game_links_soup = get_game_links_soup(gamer_id)
    # parse urls of games page
    games_list = get_game_links(game_links_soup)
    # parse details of games page
    games_details_all = get_game_details(game_links_soup, gamer_id)
    # create empty dataframe to populate with game stats
    games_stats_all = pd.DataFrame(
        columns = [
            # 'game_date',
            # 'game_time',
            'game_id',
            'Kills',
            'Medal Xp',
            'Team Wiped',
            'Last Stand Kill',
            'Match Xp',
            'Score Xp',
            'Wall Bangs',
            'Score',
            'Total Xp',
            'Headshots',
            'Assists',
            'Challenge Xp',
            'Score/min',
            'Distance Traveled',
            'Team Survival',
            'Deaths',
            'K/D Ratio',
            'Mission Pickup Tablet',
            'Bonus Xp',
            'Gulag Deaths',
            'Time Played',
            'Executions',
            'Gulag Kills',
            '% Time Moving',
            'Misc Xp',
            'Longest Streak',
            'Team Placement',
            'Damage',
            'Damage Taken',
            'Reviver',
            'Nearmisses',
            'Kiosk Buy',
            'Damage/min',
            'Down Enemy Circle 1',
            'Down Enemy Circle 2',
            'Down Enemy Circle 3',
            'Down Enemy Circle 4',
            'Down Enemy Circle 5',
            'Cache Open'])

    # iterate on game urls and parse data to append to empty df
    for game in games_list:
        print('parsing game:', game)
        # try statement bc sometimes activision pulls bad data for game stats
        try:
            game_stats = get_game_data(game)
            games_stats_all = pd.concat([games_stats_all, game_stats])
        # except IndexError:
        except Exception as ex:
            game_stats = get_game_data(game)
            games_stats_all = pd.concat([games_stats_all, game_stats])

    games_stats_all.rename(
        columns={'Kills': 'kills',
            'Medal Xp': 'medal_xp',
            'Team Wiped': 'team_wiped',
            'Last Stand Kill': 'last_stand_kill',
            'Match Xp': 'match_xp',
            'Score Xp': 'score_xp',
            'Wall Bangs': 'wall_bangs',
            'Score': 'score',
            'Total Xp': 'total_xp',
            'Headshots': 'headshots',
            'Assists': 'assists',
            'Challenge Xp': 'challenge_xp',
            'Score/min': 'score_per_min',
            'Distance Traveled': 'distance_traveled',
            'Team Survival': 'team_survival',
            'Deaths': 'deaths',
            'K/D Ratio': 'kd',
            'Mission Pickup Tablet': 'mission_pickup_tablet',
            'Bonus Xp': 'bonus_xp',
            'Gulag Deaths': 'gulag_deaths',
            'Time Played': 'time_played',
            'Executions': 'executions',
            'Gulag Kills': 'gulag_kills',
            '% Time Moving': 'percent_time_moving',
            'Misc Xp': 'misc_xp',
            'Longest Streak': 'longest_streak',
            'Team Placement': 'team_placement',
            'Damage': 'damage',
            'Damage Taken': 'damage_taken',
            'Reviver': 'reviver',
            'Nearmisses': 'nearmisses',
            'Kiosk Buy': 'kiosk_buy',
            'Damage/min': 'damager_per_min',
            'Down Enemy Circle 1': 'down_enemy_circle1',
            'Down Enemy Circle 2': 'down_enemy_circle2',
            'Down Enemy Circle 3': 'down_enemy_circle3',
            'Down Enemy Circle 4': 'down_enemy_circle4',
            'Down Enemy Circle 5': 'down_enemy_circle5',
            'Cache Open': 'cache_open'},
        inplace=True)

    # reorder the columns
    games_stats_all = games_stats_all[[
        'game_id',
        # 'game_date',
        # 'game_time',
        'kills',
        'deaths',
        'assists',
        'kd',
        'damage',
        'score',
        'score_per_min',
        'wall_bangs',
        'headshots',
        'reviver',
        'team_placement',
        'time_played',
        'total_xp',
        'score_xp',
        'match_xp',
        'challenge_xp',
        'medal_xp',
        'bonus_xp',
        'misc_xp',
        'team_wiped',
        'gulag_kills',
        'gulag_deaths',
        'distance_traveled',
        'percent_time_moving',
        'team_survival',
        'executions',
        'nearmisses',
        'kiosk_buy',
        'damage_taken',
        'mission_pickup_tablet',
        'last_stand_kill',
        # 'down_enemy_circle1',
        # 'down_enemy_circle2',
        # 'down_enemy_circle3',
        # 'down_enemy_circle4',
        # 'down_enemy_circle5',
        'longest_streak',
        'cache_open']]
    games_stats_all.insert(loc=1, column='gamer_id', value=gamer_id)
    games_stats_all.reset_index(inplace=True, drop=True)

    print(profile_df.shape)
    print(games_details_all.shape)
    print(games_stats_all.shape)

    final_dfs = [('profile', profile_df),
                 ('game_details', games_details_all),
                 ('game_stats', games_stats_all)]

    s3_bucket = 'codwarzone'

    for final_df in final_dfs:
        # # for local
        # local_filename_archive = f'/Users/brianshin/brian/tinker/brian_dwh/codwarzone/output/{final_df[0]}/{final_df[0]}_{gamer_id.replace('#', '_')}_'+today+'.csv'
        # filename = f'/Users/brianshin/brian/tinker/brian_dwh/codwarzone/output/{final_df[0]}.csv'
        # file = final_df[1]
        # output_filename = f'{final_df[0]}/{final_df[0]}_{today}.csv'
        # file.to_csv(filename_historic, index=False, encoding='utf-8')
        # file.to_csv(filename, index=False, encoding='utf-8')
        # # for s3
        # # load_s3(s3_bucket=s3_bucket, input_filename=filename, output_filename=output_filename)


        archive_filename = f'{final_df[0]}_{gamer_id}_{today}.csv'.replace('#', '_')
        current_filename = f'{final_df[0]}_{gamer_id}.csv'.replace('#', '_')
        archive_local_filepath = f'/Users/brianshin/brian/tinker/brian_dwh/codwarzone/output/archive/{final_df[0]}/{archive_filename}'
        current_local_filepath = f'/Users/brianshin/brian/tinker/brian_dwh/codwarzone/output/current/{current_filename}'

        final_df[1].to_csv(archive_local_filepath, encoding='utf-8', index=False)
        final_df[1].to_csv(current_local_filepath, encoding='utf-8', index=False)

        s3_upload_file(data = archive_local_filepath, bucket = s3_bucket, filepath = f'archive/{final_df[0]}/{archive_filename}')
        s3_upload_file(data = current_local_filepath, bucket = s3_bucket, filepath = f'current/{current_filename}')


    time = (dt.datetime.now() - start_time)
    print("--- {} ---".format(dt.timedelta(seconds=time.seconds)))


# to get games from all days available on load screen, not just today
def get_historic_game_details(backfill_path, gamer_id):

    print('historic_soup_path: ', historic_soup_path)

    with open(historic_soup_path) as f:
        soup = bs(f, "html.parser")

    games_by_day = soup.findAll('div', attrs={'class': 'trn-gamereport-list__group'})

    games_details_all = pd.DataFrame()

    for day in range(len(games_by_day)):

        games_for_day = games_by_day[day].findAll('div', attrs={'class':'match__row'})

        for game in range(len(games_for_day)):
            game_id = games_for_day[game].find('a')['href'].split('match/')[1].split('?')[0]
            game_placement = games_for_day[game].find('div', attrs={'class': 'match__placement'})
            game_placement_parsed = parse_text(game_placement)
            game_type = games_for_day[game].find('span', attrs={'class':'match__name'})
            game_type_parsed = parse_text(game_type)
            game_time = games_for_day[game].find('span', attrs={'class':'match__time'})
            game_time_parsed = parse_text(game_time)
            game_details = games_for_day[game].find('div', attrs={'class':'match__row-stats'})
            game_stats = game_details.findAll('div', attrs={'class':'stat align-right'})

            game_details_stats = []

            for game_stat in game_stats:
                stat = parse_text(game_stat.find('span', {'class': 'name'}))
                val = parse_text(game_stat.find('span', {'class': 'value'}))
                game_details_stats.append((stat, val))


            game_details_stats_dict = {
                'game_id': game_id,
                'game_type': game_type_parsed,
                'game_time': game_time_parsed,
                'placement': game_placement_parsed
                }


            for game_detail_stat in game_details_stats:
                game_details_stats_dict.update({game_detail_stat[0]: [game_detail_stat[1]]})

            game_detail_stats_df = pd.DataFrame(game_details_stats_dict)
            game_detail_stats_df.rename(
                columns={
                    'Kills':'kills',
                    'Cache Open': 'cache_open',
                    'Damage': 'damage',
                    'Damage/min': 'damage_per_min'},
                inplace=True)

            if day != 0:
                game_month_date = parse_text(games_by_day[day].find('h3', attrs={'class':'trn-gamereport-list__title'}))
                game_month_date = game_month_date+' 2020'
                game_date = dt.datetime.strptime(game_month_date,'%b %d %Y').strftime('%Y-%m-%d')
                game_detail_stats_df.insert(loc=1, column='game_date', value=game_date)
            else:
                game_detail_stats_df.insert(loc=1, column='game_date', value=today_id)

            games_details_all = pd.concat([game_detail_stats_df, games_details_all])
    games_details_all.insert(loc=2, column='gamer_id', value=gamer_id)
    games_details_all.reset_index(inplace=True, drop=True)

    filename = f'game_details_{gamer_id}_{today}.csv'
    local_filepath = f'/Users/brianshin/brian/tinker/brian_dwh/codwarzone/output/game_details/{filename}'

    games_details_all.to_csv(
        local_filepath,
        encoding='utf-8',
        index=False)

    s3_upload_file(data = local_filepath, bucket = s3_bucket, filepath = f'game_details/{filename}')

    return games_details_all


def get_historic_game_stats(games_details_all, gamer_id):

    # create empty dataframe to populate with game stats
    game_stats_historic = pd.DataFrame(
        columns = [
            'game_date',
            'game_time',
            'game_id',
            'Kills',
            'Medal Xp',
            'Team Wiped',
            'Last Stand Kill',
            'Match Xp',
            'Score Xp',
            'Wall Bangs',
            'Score',
            'Total Xp',
            'Headshots',
            'Assists',
            'Challenge Xp',
            'Score/min',
            'Distance Traveled',
            'Team Survival',
            'Deaths',
            'K/D Ratio',
            'Mission Pickup Tablet',
            'Bonus Xp',
            'Gulag Deaths',
            'Time Played',
            'Executions',
            'Gulag Kills',
            '% Time Moving',
            'Misc Xp',
            'Longest Streak',
            'Team Placement',
            'Damage',
            'Damage Taken',
            'Reviver',
            'Nearmisses',
            'Kiosk Buy',
            'Damage/min',
            'Down Enemy Circle 1',
            'Down Enemy Circle 2',
            'Down Enemy Circle 3',
            'Down Enemy Circle 4',
            'Down Enemy Circle 5',
            'Cache Open'])
    counter = 0

    for game_id in games_details_all['game_id']:

        counter = counter + 1
        print(counter)

        game = f'/warzone/match/{game_id}?handle={gamer_id}'.split('#')[0].replace('zeroserker', 'Zeroserker').replace('yummkpop', 'Yummkpop')

        # try statement bc sometimes activision pulls bad data for game stats
        try:
            print('parsing game:', game)
            game_stats = get_game_data(game)
            game_stats_historic = pd.concat([game_stats_historic, game_stats])
        # except IndexError:
        except:
            print('bad match data. thanks activision. ¯\_(ツ)_/¯')

    game_stats_historic.rename(
            columns={'Kills': 'kills',
                'Medal Xp': 'medal_xp',
                'Team Wiped': 'team_wiped',
                'Last Stand Kill': 'last_stand_kill',
                'Match Xp': 'match_xp',
                'Score Xp': 'score_xp',
                'Wall Bangs': 'wall_bangs',
                'Score': 'score',
                'Total Xp': 'total_xp',
                'Headshots': 'headshots',
                'Assists': 'assists',
                'Challenge Xp': 'challenge_xp',
                'Score/min': 'score_per_min',
                'Distance Traveled': 'distance_traveled',
                'Team Survival': 'team_survival',
                'Deaths': 'deaths',
                'K/D Ratio': 'kd',
                'Mission Pickup Tablet': 'mission_pickup_tablet',
                'Bonus Xp': 'bonus_xp',
                'Gulag Deaths': 'gulag_deaths',
                'Time Played': 'time_played',
                'Executions': 'executions',
                'Gulag Kills': 'gulag_kills',
                '% Time Moving': 'percent_time_moving',
                'Misc Xp': 'misc_xp',
                'Longest Streak': 'longest_streak',
                'Team Placement': 'team_placement',
                'Damage': 'damage',
                'Damage Taken': 'damage_taken',
                'Reviver': 'reviver',
                'Nearmisses': 'nearmisses',
                'Kiosk Buy': 'kiosk_buy',
                'Damage/min': 'damager_per_min',
                'Down Enemy Circle 1': 'down_enemy_circle1',
                'Down Enemy Circle 2': 'down_enemy_circle2',
                'Down Enemy Circle 3': 'down_enemy_circle3',
                'Down Enemy Circle 4': 'down_enemy_circle4',
                'Down Enemy Circle 5': 'down_enemy_circle5',
                'Cache Open': 'cache_open'},
            inplace=True)

    # reorder the columns
    game_stats_historic = game_stats_historic[[
        'game_id',
        'game_date',
        'game_time',
        'kills',
        'deaths',
        'assists',
        'kd',
        'damage',
        'score',
        'score_per_min',
        'wall_bangs',
        'headshots',
        'reviver',
        'team_placement',
        'time_played',
        'total_xp',
        'score_xp',
        'match_xp',
        'challenge_xp',
        'medal_xp',
        'bonus_xp',
        'misc_xp',
        'team_wiped',
        'gulag_kills',
        'gulag_deaths',
        'distance_traveled',
        'percent_time_moving',
        'team_survival',
        'executions',
        'nearmisses',
        'kiosk_buy',
        'damage_taken',
        'mission_pickup_tablet',
        'last_stand_kill',
        # 'down_enemy_circle1',
        # 'down_enemy_circle2',
        # 'down_enemy_circle3',
        # 'down_enemy_circle4',
        # 'down_enemy_circle5',
        'longest_streak',
        'cache_open']]
    game_stats_historic.insert(loc=1, column='gamer_id', value=gamer_id)
    game_stats_historic.reset_index(inplace=True, drop=True)

    filename = f'game_stats_{gamer_id}_{today}.csv'
    local_filepath = f'/Users/brianshin/brian/tinker/brian_dwh/codwarzone/output/game_stats/{filename}'

    game_stats_historic.to_csv(
        local_filepath,
        encoding='utf-8',
        index=False)


    s3_upload_file(data = local_filepath, bucket = s3_bucket, filepath = f'game_stats/{filename}')

    return game_stats_historic


# for parsing the parameters when running the script (defaults gamer_id to me)
def get_args():

    parser = argparse.ArgumentParser(description='warzone_scraper_local.py scrapes warzone stats from cod.tracker.')
    parser.add_argument('--gamer_id', default='gs25#11901', help='enter your warzone username. the default is me: gs25#11901.')
    parser.add_argument('--is_backfill', default='N', help='do you want to pull historic data? Y for yes, N for no. default is N.')

    args = parser.parse_args()

    # are league usernames case sensitive?
    gamer_id = args.gamer_id
    is_backfill = args.is_backfill.upper()

    return gamer_id, is_backfill
################################################################################
# u know what it do
if __name__ == '__main__':
    # parse gamer_id from script parameters
    gamer_id, is_backfill = get_args()
    # gamer_id = 'gs25#11901'
    # is_historic = 'Y'
    print('gamer_id:', gamer_id)
    print('is_historic:', is_backfill)

    if is_backfill == 'Y':

        # backfill_username='gs25#11901'
        # backfill_date='20201201'
        backfill_username = input('enter username (ex: gs25#11901):')
        backfill_date = input ('enter backfill date as yyyymmdd (ex: 20201201):')
        backfill_filename = f'backfill_{backfill_username}_{backfill_date}.csv'.replace('#', '_')
        backfill_path = f'/Users/brianshin/brian/tinker/brian_dwh/codwarzone/output/archive/backfills/{backfill_filename}'

        games_details_all = get_historic_game_details(backfill_path, gamer_id)
        get_historic_game_stats(games_details_all, gamer_id)

    else:
        parse_warzone_tracker(gamer_id)
