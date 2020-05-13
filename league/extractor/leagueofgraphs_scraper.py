"""
author: Brian Shin
date: 05.12.2020
email: brianhesungshin@gmail.com
"""

"""
  usage: python leagueofgraphs_scraper.py [username (default is rickyyytan)]

  The purpose of this assignment is to scrape a league of graphs profile with games.
  The dataframes get written to s3 bucket leagueofgraphs/dataframe/dataframe_date.csv.
  (ex: leagueofgraphs/profile/profile_20200513.csv)

"""

from bs4 import BeautifulSoup as bs
from time import sleep
from urllib.request import Request, urlopen

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

ssl._create_default_https_context = ssl._create_unverified_context

today = dt.datetime.now().strftime("%Y%m%d")

################################################################################

def parse_text(unparsed):

    parsed = unparsed.get_text().lstrip('\n\r\n').lstrip('\n\r\n ').rstrip('\n\r\n').rstrip('\n\r\n ').strip(' ')

    return parsed


def parse_tag(unparsed, lvl):

    parsed = str(unparsed).split('="')[lvl].split('">')[0].split('"')[0]

    return parsed


def get_profile_soup(username):

    profile_user = username
    profile_url = 'https://www.leagueofgraphs.com/summoner/na/'+profile_user
    hdr = {'User-Agent': 'Mozilla/5.0'}
    profile_req = Request(profile_url,headers=hdr)
    profile_html = urlopen(profile_req)
    profile_soup = bs(profile_html, "lxml")


    return profile_soup

################################################################################

def get_favchamp_data(profile_soup, all, played, winrate, number):

    favchamp = profile_soup.find('div', attrs={'id':'favchamps'}).findAll('div', attrs={'class':'name'})[all]
    favchamp_parsed = parse_text(favchamp)

    favchamp_rank = profile_soup.find('div', attrs={'id':'favchamps'}).findAll('span', attrs={'class':'rank'})[all]
    favchamp_rank_parsed = parse_text(favchamp_rank)

    # favchamp_regrank = profile_soup.find('div', attrs={'id':'favchamps'}).findAll('span', attrs={'class':'regionalRank'})[all]
    # favchamp_regrank_parsed = parse_text(favchamp_regrank)

    favchamp_kills = profile_soup.find('div', attrs={'id':'favchamps'}).findAll('span', attrs={'class':'kills'})[all]
    favchamp_kills_parsed = parse_text(favchamp_kills)

    favchamp_deaths = profile_soup.find('div', attrs={'id':'favchamps'}).findAll('span', attrs={'class':'deaths'})[all]
    favchamp_deaths_parsed = parse_text(favchamp_deaths)

    favchamp_assists = profile_soup.find('div', attrs={'id':'favchamps'}).findAll('span', attrs={'class':'assists'})[all]
    favchamp_assists_parsed = parse_text(favchamp_assists)

    favchamp_played = profile_soup.find('div', attrs={'id':'favchamps'}).findAll('td')[played]
    favchamp_played_parsed = parse_tag(favchamp_played, 1)

    favchamp_winrate = profile_soup.find('div', attrs={'id':'favchamps'}).findAll('td')[winrate].find('progressbar')['data-value']
    favchamp_winrate_parsed = parse_tag(favchamp_winrate, 0)

    favchamp_dict = {f'favchamp{number}': [favchamp_parsed],
                     f'favchamp{number}_rank': [favchamp_rank_parsed],
                     # f'favchamp{number}_regrank': [favchamp_regrank_parsed],
                     f'favchamp{number}_kills': [favchamp_kills_parsed],
                     f'favchamp{number}_deaths': [favchamp_deaths_parsed],
                     f'favchamp{number}_assists': [favchamp_assists_parsed],
                     f'favchamp{number}_played': [favchamp_played_parsed],
                     f'favchamp{number}_winrate': [favchamp_winrate_parsed]}

    favchamp_df = pd.DataFrame(favchamp_dict)

    return favchamp_df


################################################################################

def get_role_data(profile_soup, number):

    # iterate on list based on len
    role = profile_soup.find('div', attrs={'id':'profileRoles'}).findAll('td')[number * 3 - 3]
    role_parsed = parse_text(role)

    role_played = profile_soup.find('div', attrs={'id':'profileRoles'}).findAll('td')[number * 3 - 2]
    role_played_parsed = parse_tag(role_played, 1)

    role_winrate = profile_soup.find('div', attrs={'id':'profileRoles'}).findAll('td')[number * 3 - 1]
    role_winrate_parsed = parse_tag(role_winrate, 1)


    role_dict = {f'role{number}': [role_parsed],
                 f'role{number}_played': [role_played_parsed],
                 f'role{number}_winrate': [role_winrate_parsed]}

    role_df = pd.DataFrame(role_dict)

    return role_df

################################################################################

def get_playswith_data(profile_soup, number):

    # iterate on list based on len
    playswith = profile_soup.find('div', attrs={'class':'box box-padding-10'}).findAll('td')[number * 3 - 3].find('div', {'class':'txt name'})
    playswith_name_parsed = parse_text(playswith).split('\n')[0].rstrip(' ')
    playswith_rank_parsed = parse_text(playswith).split('\n')[1]

    playswith_played = profile_soup.find('div', attrs={'class':'box box-padding-10'}).findAll('td')[number * 3 - 2]
    playswith_played_parsed = parse_tag(playswith_played, 2)

    playswith_winrate = profile_soup.find('div', attrs={'class':'box box-padding-10'}).findAll('td')[number * 3 - 1]
    playswith_winrate_parsed = parse_tag(playswith_winrate, 2)

    playswith_dict = {f'playswith{number}_name': [playswith_name_parsed],
                      f'playswith{number}_rank': [playswith_rank_parsed],
                      f'playswith{number}_played': [playswith_played_parsed],
                      f'playswith{number}_winrate': [playswith_winrate_parsed]}

    playswith_df = pd.DataFrame(playswith_dict)

    return playswith_df

################################################################################

def get_profile_data(profile_soup):

    tier = profile_soup.find('div', attrs={'class':'leagueTier'})
    tier_parsed = parse_text(tier)

    queue = profile_soup.find('span', attrs={'class':'queue'})
    queue_parsed = parse_text(queue)

    rank = profile_soup.find('span', attrs={'class':'highlight'})
    rank_parsed = parse_text(rank)

    region_rank = profile_soup.find('a', attrs={'class':'regionalRank'})
    region_rank_parsed = parse_text(region_rank)

    lp = profile_soup.find('span', attrs={'class':'leaguePoints'})
    lp_parsed = parse_text(lp)

    wins = profile_soup.find('span', attrs={'class':'winsNumber'})
    wins_parsed = parse_text(wins)

    losses = profile_soup.find('span', attrs={'class':'lossesNumber'})
    losses_parsed = parse_text(losses)

    games = profile_soup.find('div', attrs={'id':'graphDD2'})
    games_parsed = parse_text(games)

    winrate = profile_soup.find('div', attrs={'id':'graphDD3'})
    winrate_parsed = parse_text(winrate)

    avg_kills = profile_soup.find('div', attrs={'class':'number'}).find('span', attrs={'class':'kills'})
    avg_kills_parsed = parse_text(avg_kills)

    avg_deaths = profile_soup.find('div', attrs={'class':'number'}).find('span', attrs={'class':'deaths'})
    avg_deaths_parsed = parse_text(avg_deaths)

    avg_assists = profile_soup.find('div', attrs={'class':'number'}).find('span', attrs={'class':'assists'})
    avg_assists_parsed = parse_text(avg_assists)

    tags_len = len(profile_soup.find('div', attrs={'class':'box tags-box'}).findAll('div'))
    tags_list = []
    for i in range(tags_len):
        tags = profile_soup.find('div', attrs={'class':'box tags-box'}).findAll('div')[i]
        tags_parsed = parse_text(tags)
        tags_list.append(tags_parsed)

    profile_dict = {'tier': [tier_parsed],
                    'queue': [queue_parsed],
                    'rank': [rank_parsed],
                    'region_rank': [region_rank_parsed],
                    'lp': [lp_parsed],
                    'wins': [wins_parsed],
                    'losses': [losses_parsed],
                    'games': [games_parsed],
                    'winrate': [winrate_parsed],
                    'avg_kills': [avg_kills_parsed],
                    'avg_deaths': [avg_deaths_parsed],
                    'avg_assists': [avg_assists_parsed],
                    'tags': [tags_list]}

    profile_df = pd.DataFrame(profile_dict)


    return profile_df

################################################################################

def get_game_links(profile_soup):

    games = profile_soup.find('table', attrs={'class':'data_table relative recentGamesTable'}).findAll('td', attrs={'class': 'championCellLight'})

    games_len = len(games)
    games_list = []
    for i in range(games_len):
        game = games[i].find('a')['href']
        games_list.append(game)

    return games_list

################################################################################

def get_game_soup(game):

    game_url = 'https://www.leagueofgraphs.com/'+game
    hdr = {'User-Agent': 'Mozilla/5.0'}
    game_req = Request(game_url,headers=hdr)
    game_html = urlopen(game_req)
    game_soup = bs(game_html, "lxml")


    return game_soup


def get_game_basic_data(game_soup, game_participant, game):

    # game_participant = 'participant8'
    # game = 'match/na/3416358986#participant8'
    # game_soup = get_game_soup(game)

    game_legend = game_soup.find('div', {'data-tab-target': game_participant}).find('img')['alt']

    game_tags = game_soup.find('div', attrs={'data-tab-id': game_participant}).find('div', attrs={'class':'box tags-box'}).findAll('div')
    game_tags_list = []
    for game_tag in game_tags:
        game_tag_parsed = parse_text(game_tag)
        game_tags_list.append(game_tag_parsed)

    game_kills = game_soup.find('div', attrs={'data-tab-id': game_participant}).find('span', attrs={'class':'kills'})
    game_kills_parsed = parse_text(game_kills)

    game_deaths = game_soup.find('div', attrs={'data-tab-id': game_participant}).find('span', attrs={'class':'deaths'})
    game_deaths_parsed = parse_text(game_deaths)

    game_assists = game_soup.find('div', attrs={'data-tab-id': game_participant}).find('span', attrs={'class':'assists'})
    game_assists_parsed = parse_text(game_assists)

    game_subnum = game_soup.find('div', attrs={'data-tab-id': game_participant}).find('div', {'class':'subNumbers'})
    game_subnum_parsed = parse_text(game_subnum).split(' - \r\n')
    game_minions_parsed = game_subnum_parsed[0]
    game_gold_parsed = game_subnum_parsed[1].lstrip(' ')

    game_items = game_soup.find('div', attrs={'data-tab-id': game_participant}).find('table', {'class': 'data_table match_items_table'}).findAll('tr')
    game_items_len = len(game_items)
    game_items_list = []
    for i in range(1, game_items_len):
        game_items_time_parsed = parse_text(game_items[i])
        game_items_time_list = []
        for game_item in game_items[i].findAll('img'):
            game_item_parsed = parse_tag(game_item, 1)
            game_items_time_list.append(game_item_parsed)
        game_items_list.append({game_items_time_parsed: game_items_time_list})

    game_runes = game_soup.find('div', attrs={'data-tab-id': game_participant}).findAll('div', {'class': 'box box-padding-10-10'})[2].findAll('div', {'class': 'txt'})
    game_runes_list = []
    for game_rune in game_runes:
        game_run_parsed = parse_text(game_rune)
        game_runes_list.append(game_run_parsed)

    game_basic_dict = {'game_url': [game],
                 'game_tags': [game_tags_list],
                 'game_runs': [game_runes_list],
                 'legend': [game_legend],
                 'kills': [game_kills_parsed],
                 'deaths': [game_deaths_parsed],
                 'assists': [game_assists_parsed],
                 'minions': [game_minions_parsed],
                 'gold': [game_gold_parsed]}

    game_basic_df = pd.DataFrame(game_basic_dict)

    return game_basic_df


def get_legend_dmg_data(dmg_chart, chart_legends, legend):

    legend_name = parse_tag(chart_legends[legend], 1)

    dmg_chart_row1_parsed = parse_text(dmg_chart[2].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row1_values_parsed = parse_text(dmg_chart[2].findAll('span')[legend])

    dmg_chart_row2_parsed = parse_text(dmg_chart[3].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row2_values_parsed = parse_text(dmg_chart[3].findAll('span')[legend])

    dmg_chart_row3_parsed = parse_text(dmg_chart[4].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row3_values_parsed = parse_text(dmg_chart[4].findAll('span')[legend])

    dmg_chart_row4_parsed = parse_text(dmg_chart[7].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row4_values_parsed = parse_text(dmg_chart[7].findAll('span')[legend])

    dmg_chart_row5_parsed = parse_text(dmg_chart[8].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row5_values_parsed = parse_text(dmg_chart[8].findAll('span')[legend])

    dmg_chart_row6_parsed = parse_text(dmg_chart[9].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row6_values_parsed = parse_text(dmg_chart[9].findAll('span')[legend])

    dmg_chart_row7_parsed = parse_text(dmg_chart[10].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row7_values_parsed = parse_text(dmg_chart[10].findAll('span')[legend])

    dmg_chart_row8_parsed = parse_text(dmg_chart[11].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row8_values_parsed = parse_text(dmg_chart[11].findAll('span')[legend])

    dmg_chart_row9_parsed = parse_text(dmg_chart[12].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row9_values_parsed = parse_text(dmg_chart[12].findAll('span')[legend])

    dmg_chart_row10_parsed = parse_text(dmg_chart[13].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row10_values_parsed = parse_text(dmg_chart[13].findAll('span')[legend])

    dmg_chart_row11_parsed = parse_text(dmg_chart[14].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row11_values_parsed = parse_text(dmg_chart[14].findAll('span')[legend])

    dmg_chart_row12_parsed = parse_text(dmg_chart[15].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row12_values_parsed = parse_text(dmg_chart[15].findAll('span')[legend])

    dmg_chart_row13_parsed = parse_text(dmg_chart[16].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row13_values_parsed = parse_text(dmg_chart[16].findAll('span')[legend])

    dmg_chart_row14_parsed = parse_text(dmg_chart[18].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row14_values_parsed = parse_text(dmg_chart[18].findAll('span')[legend])

    dmg_chart_row15_parsed = parse_text(dmg_chart[19].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row15_values_parsed = parse_text(dmg_chart[19].findAll('span')[legend])

    dmg_chart_row16_parsed = parse_text(dmg_chart[20].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row16_values_parsed = parse_text(dmg_chart[20].findAll('span')[legend])

    dmg_chart_row17_parsed = parse_text(dmg_chart[21].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row17_values_parsed = parse_text(dmg_chart[21].findAll('span')[legend])

    dmg_chart_row18_parsed = parse_text(dmg_chart[22].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row18_values_parsed = parse_text(dmg_chart[22].findAll('span')[legend])

    dmg_chart_row19_parsed = parse_text(dmg_chart[23].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row19_values_parsed = parse_text(dmg_chart[23].findAll('span')[legend])

    dmg_chart_row20_parsed = parse_text(dmg_chart[25].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row20_values_parsed = parse_text(dmg_chart[25].findAll('span')[legend])

    dmg_chart_row21_parsed = parse_text(dmg_chart[26].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row21_values_parsed = parse_text(dmg_chart[26].findAll('span')[legend])

    dmg_chart_row22_parsed = parse_text(dmg_chart[27].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row22_values_parsed = parse_text(dmg_chart[27].findAll('span')[legend])

    dmg_chart_row23_parsed = parse_text(dmg_chart[28].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row23_values_parsed = parse_text(dmg_chart[28].findAll('span')[legend])

    dmg_chart_row24_parsed = parse_text(dmg_chart[30].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row24_values_parsed = parse_text(dmg_chart[30].findAll('span')[legend])

    dmg_chart_row25_parsed = parse_text(dmg_chart[31].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row25_values_parsed = parse_text(dmg_chart[31].findAll('span')[legend])

    dmg_chart_row26_parsed = parse_text(dmg_chart[32].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row26_values_parsed = parse_text(dmg_chart[32].findAll('span')[legend])

    dmg_chart_row27_parsed = parse_text(dmg_chart[33].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row27_values_parsed = parse_text(dmg_chart[33].findAll('span')[legend])

    dmg_chart_row28_parsed = parse_text(dmg_chart[34].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row28_values_parsed = parse_text(dmg_chart[34].findAll('span')[legend])

    dmg_chart_row29_parsed = parse_text(dmg_chart[35].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row29_values_parsed = parse_text(dmg_chart[35].findAll('span')[legend])

    dmg_chart_row30_parsed = parse_text(dmg_chart[37].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row30_values_parsed = parse_text(dmg_chart[37].findAll('span')[legend])

    dmg_chart_row31_parsed = parse_text(dmg_chart[38].find('td', {'class': 'text-left statTitle'}))
    dmg_chart_row31_values_parsed = parse_text(dmg_chart[38].findAll('span')[legend])


    dmg_chart_dict = {'legend': [legend_name],
                      dmg_chart_row1_parsed: [dmg_chart_row1_values_parsed],
                      dmg_chart_row2_parsed: [dmg_chart_row2_values_parsed],
                      dmg_chart_row3_parsed: [dmg_chart_row3_values_parsed],
                      dmg_chart_row4_parsed: [dmg_chart_row4_values_parsed],
                      dmg_chart_row5_parsed: [dmg_chart_row5_values_parsed],
                      dmg_chart_row6_parsed: [dmg_chart_row6_values_parsed],
                      dmg_chart_row7_parsed: [dmg_chart_row7_values_parsed],
                      dmg_chart_row8_parsed: [dmg_chart_row8_values_parsed],
                      dmg_chart_row9_parsed: [dmg_chart_row9_values_parsed],
                      dmg_chart_row10_parsed: [dmg_chart_row10_values_parsed],
                      dmg_chart_row11_parsed: [dmg_chart_row11_values_parsed],
                      dmg_chart_row12_parsed: [dmg_chart_row12_values_parsed],
                      dmg_chart_row13_parsed: [dmg_chart_row13_values_parsed],
                      dmg_chart_row14_parsed: [dmg_chart_row14_values_parsed],
                      dmg_chart_row15_parsed: [dmg_chart_row15_values_parsed],
                      dmg_chart_row16_parsed: [dmg_chart_row16_values_parsed],
                      dmg_chart_row17_parsed: [dmg_chart_row17_values_parsed],
                      dmg_chart_row18_parsed: [dmg_chart_row18_values_parsed]}

    misc_chart_dict = {'legend': [legend_name],
                      dmg_chart_row19_parsed: [dmg_chart_row19_values_parsed],
                      dmg_chart_row20_parsed: [dmg_chart_row20_values_parsed],
                      dmg_chart_row21_parsed: [dmg_chart_row21_values_parsed],
                      dmg_chart_row22_parsed: [dmg_chart_row22_values_parsed],
                      dmg_chart_row23_parsed: [dmg_chart_row23_values_parsed],
                      dmg_chart_row24_parsed: [dmg_chart_row24_values_parsed],
                      dmg_chart_row25_parsed: [dmg_chart_row25_values_parsed],
                      dmg_chart_row26_parsed: [dmg_chart_row26_values_parsed],
                      dmg_chart_row27_parsed: [dmg_chart_row27_values_parsed],
                      dmg_chart_row28_parsed: [dmg_chart_row28_values_parsed],
                      dmg_chart_row29_parsed: [dmg_chart_row29_values_parsed],
                      dmg_chart_row30_parsed: [dmg_chart_row30_values_parsed],
                      dmg_chart_row31_parsed: [dmg_chart_row31_values_parsed]}


    dmg_chart_df = pd.DataFrame(dmg_chart_dict)
    misc_chart_df = pd.DataFrame(misc_chart_dict)


    return dmg_chart_df, misc_chart_df


def get_game_dmg_data(game_soup):

    chart_legends = game_soup.find('div', attrs={'data-tab-id': 'matchDataTable'}).findAll('th')

    dmg_chart = game_soup.find('div', attrs={'data-tab-id': 'matchDataTable'}).findAll('tr')

    legend1_dmg_df, legend1_misc_df = get_legend_dmg_data(dmg_chart, chart_legends, 1)
    legend2_dmg_df, legend2_misc_df = get_legend_dmg_data(dmg_chart, chart_legends, 2)
    legend3_dmg_df, legend3_misc_df = get_legend_dmg_data(dmg_chart, chart_legends, 3)
    legend4_dmg_df, legend4_misc_df = get_legend_dmg_data(dmg_chart, chart_legends, 4)
    legend5_dmg_df, legend5_misc_df = get_legend_dmg_data(dmg_chart, chart_legends, 5)
    legend6_dmg_df, legend6_misc_df = get_legend_dmg_data(dmg_chart, chart_legends, 6)
    legend7_dmg_df, legend7_misc_df = get_legend_dmg_data(dmg_chart, chart_legends, 7)
    legend8_dmg_df, legend8_misc_df = get_legend_dmg_data(dmg_chart, chart_legends, 8)
    legend9_dmg_df, legend9_misc_df = get_legend_dmg_data(dmg_chart, chart_legends, 9)

    legends_dmg_df = pd.concat([
        legend1_dmg_df,
        legend2_dmg_df,
        legend3_dmg_df,
        legend4_dmg_df,
        legend5_dmg_df,
        legend6_dmg_df,
        legend7_dmg_df,
        legend8_dmg_df,
        legend9_dmg_df])

    legends_misc_df = pd.concat([
        legend1_misc_df,
        legend2_misc_df,
        legend3_misc_df,
        legend4_misc_df,
        legend5_misc_df,
        legend6_misc_df,
        legend7_misc_df,
        legend8_misc_df,
        legend9_misc_df])

    return legends_dmg_df, legends_misc_df


def get_game_data(game):

    # game = '/match/na/3402844986#participant4'

    game_participant = game.split('#')[1]
    game_soup = get_game_soup(game)

    game_basic_df = get_game_basic_data(game_soup, game_participant, game)
    game_dmg_df, game_misc_df = get_game_dmg_data(game_soup)
    game_dmg_df.reset_index(drop=True, inplace=True)
    game_misc_df.reset_index(drop=True, inplace=True)

    game_dmg_df['game'] = game
    game_misc_df['game'] = game

    return game_basic_df, game_dmg_df, game_misc_df

################################################################################

def load_s3(s3_bucket, input_filename, output_filename):
        s3 = boto3.resource('s3')
        s3.meta.client.upload_file(input_filename, s3_bucket, output_filename)
        print ('COMPLETE: ' + input_filename + ' loaded into s3://' +
               s3_bucket + ' as ' + output_filename)

################################################################################

def get_parameters():
    ## get the env
    user = sys.argv[1] if len(sys.argv) > 1 else 'rickyyytan'
    user = user.upper()
    return user


def parse_leagueofgraphs():

    start_time = dt.datetime.now()
    print('script started at:', start_time)

    # username = 'rickyyytan'
    username = get_parameters()
    print('parsing profile:', username)

    profile_soup = get_profile_soup(username)

    favchamp1_df = get_favchamp_data(profile_soup, 0, 1, 2, 1)
    favchamp2_df = get_favchamp_data(profile_soup, 1, 4, 5, 2)
    favchamp3_df = get_favchamp_data(profile_soup, 2, 7, 8, 3)
    favchamp4_df = get_favchamp_data(profile_soup, 3, 10, 11, 4)
    favchamps_df = pd.concat([favchamp1_df, favchamp2_df, favchamp3_df, favchamp4_df], axis=1)

    role1_df = get_role_data(profile_soup, 1)
    role2_df = get_role_data(profile_soup, 2)
    role3_df = get_role_data(profile_soup, 3)
    role4_df = get_role_data(profile_soup, 4)
    roles_df = pd.concat([role1_df, role2_df, role3_df, role4_df], axis=1)

    playswith1_df = get_playswith_data(profile_soup, 1)
    playswith2_df = get_playswith_data(profile_soup, 2)
    playswith3_df = get_playswith_data(profile_soup, 3)
    playswith4_df = get_playswith_data(profile_soup, 4)
    playswith_df = pd.concat([playswith1_df, playswith2_df, playswith3_df, playswith4_df], axis=1)

    profile_df = get_profile_data(profile_soup)

    games_list = get_game_links(profile_soup)

    games_basic_df = pd.DataFrame()
    games_dmg_df = pd.DataFrame()
    games_misc_df = pd.DataFrame()

    for game in games_list:
        print('parsing game:', game)
        game_basic_df, game_dmg_df, game_misc_df = get_game_data(game)
        games_basic_df = pd.concat([games_basic_df, game_basic_df])
        games_dmg_df = pd.concat([games_dmg_df, game_dmg_df])
        games_misc_df = pd.concat([games_misc_df, game_misc_df])

    games_stats_df = games_dmg_df.merge(games_misc_df, on=['legend', 'game'], how='left')
    games_combined_df = game_basic_df.merge(
        games_stats_df,
        left_on=['game_url', 'legend'],
        right_on=['game', 'legend'],
        how='left')

    final_dfs = [('profile', profile_df), ('favchamps', favchamps_df), ('roles', roles_df), ('playswith', playswith_df), ('games_stats', games_stats_df), ('games_combined', games_combined_df)]
    s3_bucket = 'leagueofgraphs'

    for final_df in final_dfs:
        filename = '/home/ubuntu/ubuntu/brian_dwh/league/league_temp_output/'+final_df[0]+'.csv'
        file = final_df[1]
        output_filename = final_df[0]+'/'+final_df[0]+'_'+today+'.csv'
        file.to_csv(filename, index=False, encoding='utf-8')
        load_s3(s3_bucket=s3_bucket, input_filename=filename, output_filename=output_filename)

    # import boto3
    # from botocore.exceptions import NoCredentialsError
    #
    # ACCESS_KEY = 'AKIAJIOOOLGXBFH5FJYA'
    # SECRET_KEY = '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'
    #
    #
    #
    # def upload_to_aws(local_file, bucket, s3_file):
    #     s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
    #                       aws_secret_access_key=SECRET_KEY)
    #c
    #     try:
    #         s3.upload_file(local_file, bucket, s3_file)
    #         print("Upload Successful")
    #         return True
    #     except FileNotFoundError:
    #         print("The file was not found")
    #         return False
    #     except NoCredentialsError:
    #         print("Credentials not available")
    #         return False
    #
    #
    # uploaded = upload_to_aws(input_filename, s3_bucket, output_filename)
    #

    time = (dt.datetime.now() - start_time)
    print("--- {} ---".format(dt.timedelta(seconds=time.seconds)))

if __name__ == '__main__':
    parse_leagueofgraphs()
