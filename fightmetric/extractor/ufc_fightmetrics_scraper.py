"""
@author: Brian
@date: 07.25.18
@title: ufc_fightmetrics_scraper.py

This script uses bs4 to crawl all ufc fights, events, fighter stats, and fightstats from fightmetric.com.

"""
############################################################################

from bs4 import BeautifulSoup as bs

import pandas as pd
import numpy as np
import os
import urllib.request
import string
import re
import datetime

from time import sleep

start_time = datetime.datetime.now()
print('script started at:', start_time)
########################################################################################################################################################
"""
1. get fighter profile URLs for every fighter
"""

# set range of alphabet letters to iterate on fighters by last name letter pages
alphabet = list(string.ascii_lowercase)
# create empty list to populate with ifhgter profile URLs
fighter_links = []

# iterate on every letter for fighter last names to get profile URLs
for letter in alphabet:
    fighters_url = "http://www.fightmetric.com/statistics/fighters?char="+letter+"&page=all"
    fighters_html = urllib.request.urlopen(fighters_url)                                        # pull html from one letter page
    fighters_soup = bs(fighters_html, "lxml")                                                   # make it into a soup to be able to parse
    fighters = fighters_soup.find('table', attrs={'class':'b-statistics__table'})               # parse out the section w/ the data
    rows = fighters.findAll('tr')                                                               # create list of all 'tr' tags (containing fighter profile URLs)

    # iterate on list of tr tags containg fighter profile URLs and parse out a href tags containing URL
    for row in rows:
        try:                                                                                    # use try except statement to avoid old URLs that are shit causing errors
            f_link = row.find('a')['href']
            fighter_links.append(f_link)                                                        # append each URL to fighter_links list
        except Exception as ex:
            #print(ex, fighters_url)
            continue

    print('crawling all fighters with last names beginning with: ', letter)

print('total number of fighters crawled:', len(fighter_links))

########################################################################################################################################################
"""
2. iterate on every fighter URL and get basic information for each
"""

# fighter initial stats
fighter_df = pd.DataFrame(columns=['name', 'nickname', 'record', 'height', 'weight',
                                                    'reach', 'stance', 'birthdate', 'splm', 'str acc',
                                                    'sapmn', 'st def', 'td avg', 'td acc', 'td def', 'sub avg', 'profile'])

counter = 0

for fighter_link in fighter_links:

    counter = counter + 1
    print(counter, 'of', len(fighter_links),'- crawling fighter profile:', fighter_link)

    fighter_html = urllib.request.urlopen(fighter_link)
    fighter_soup = bs(fighter_html, "lxml")

    fighter_name = fighter_soup.find('span', {'class':'b-content__title-highlight'})
    fighter_nick = fighter_soup.find('p', {'class':'b-content__Nickname'})
    fighter_record = fighter_soup.find('span', {'class':'b-content__title-record'})
    fighter_box = fighter_soup.find('ul', {'class':'b-list__box-list'}) #need for fighter_info
    fighter_info = fighter_box.findAll('li')
    fighter_box2 = fighter_soup.find('div', {'class':'b-list__info-box-left clearfix'})
    fighter_stats = fighter_box2.findAll('li')

    name = str(fighter_name.get_text()).replace('\n','').replace('\b','').replace('            ', '').replace('    ', '')
    nickname = str(fighter_nick.get_text()).replace('\n','').replace('\t','').replace('            ', '').replace('  ', '')
    record = str(fighter_record.get_text()).replace('\n','').replace('\t','').replace('            ', '').replace('    ', '').split(':')[1]
    height = fighter_info[0].get_text().replace('\n','').replace('\t','').replace('            ', '').replace('    ', '').split(':')[1]
    weight = fighter_info[1].get_text().replace('\n','').replace('\t','').replace('            ', '').replace('    ', '').split(':')[1]
    reach = fighter_info[2].get_text().replace('\n','').replace('\t','').replace('            ', '').replace('    ', '').split(':')[1]
    stance = fighter_info[3].get_text().replace('\n','').replace('\t','').replace('            ', '').replace('    ', '').split(':')[1]
    birth = fighter_info[4].get_text().replace('\n','').replace('\t','').replace('            ', '').replace('    ', '').split(':')[1]
    splm = fighter_stats[0].get_text().replace('\n','').replace('\t','').replace('            ', '').replace('    ', '').split(':')[1]
    stracc = fighter_stats[1].get_text().replace('\n','').replace('\t','').replace('            ', '').replace('    ', '').split(':')[1]
    sapmn = fighter_stats[2].get_text().replace('\n','').replace('\t','').replace('            ', '').replace('    ', '').split(':')[1]
    stdef = fighter_stats[3].get_text().replace('\n','').replace('\t','').replace('            ', '').replace('    ', '').split(':')[1]
    tdavg = fighter_stats[5].get_text().replace('\n','').replace('\t','').replace('            ', '').replace('    ', '').split(':')[1]
    tdacc = fighter_stats[6].get_text().replace('\n','').replace('\t','').replace('            ', '').replace('    ', '').split(':')[1]
    tddef = fighter_stats[7].get_text().replace('\n','').replace('\t','').replace('            ', '').replace('    ', '').split(':')[1]
    subavg = fighter_stats[8].get_text().replace('\n','').replace('\t','').replace('            ', '').replace('    ', '').split(':')[1]

    fighter_stats = {
                    'name': [name],
                    'nickname': [nickname],
                    'record': [record],
                    'height': [height],
                    'weight': [weight],
                    'reach': [reach],
                    'stance': [stance],
                    'birthdate': [birth],
                    'splm': [splm],
                    'str acc': [stracc],
                    'sapmn': [sapmn],
                    'st def': [stdef],
                    'td avg': [tdavg],
                    'td acc': [tdacc],
                    'td def': [tddef],
                    'sub avg': [subavg],
                    'profile': [fighter_link]}
    appending_fighter_df = pd.DataFrame(fighter_stats, columns = ['name', 'nickname', 'record', 'height', 'weight',
                                                    'reach', 'stance', 'birthdate', 'splm', 'str acc',
                                                    'sapmn', 'st def', 'td avg', 'td acc', 'td def', 'sub avg', 'profile'])

    try:
        fighter_df = pd.concat([fighter_df, appending_fighter_df])
    except NameError as nameerror_ex:
        fighter_df = appending_fighter_df

fighter_df = fighter_df.reset_index(drop=True)
fighter_df.shape
fighter_df.head(20)
############################################################################
"""
## 3. parse data from each event ##
"""

events_url = "http://www.fightmetric.com/statistics/events/completed?page=all"
events_html = urllib.request.urlopen(events_url)
events_soup = bs(events_html, "lxml")
events_table = events_soup.find('tbody')

events_urls = []
events_names = []

# event urls and names
for events_link in events_table.findAll('a', href=True):
    events_url = events_link.get('href')
    events_urls.append(events_url)
    events_names.append(events_link.get_text().lstrip('\n                          ').rstrip('\n                        '))

# event dates
events_dates = []
for events_date in events_table.findAll('span',{'class':'b-statistics__date'}):
    #event_dates.append(event_date.get_text())
    events_dates.append(events_date.get_text().replace('\n                          ','').replace('\n                        ',''))

# event locations
events_locations = []
for events_row in events_table.findAll('tr')[1:]:
    events_location = events_row.find('td',{'class':'b-statistics__table-col b-statistics__table-col_style_big-top-padding'})
    events_locations.append(str(events_location).split('>')[1].replace('\n                    ','').replace('\n                  </td',''))

print(events_urls[:2])
print(events_names[:2])
print(events_dates[:2])
print(events_locations[:2])

events = {
        'event': events_urls,
        'event name': events_names,
        'date': events_dates,
        'location': events_locations}

events_df = pd.DataFrame(events, columns = ['event', 'event name', 'date', 'location'])
events_df.shape
events_df.head(20)

############################################################################
"""
4. parse data from each fight
"""

fight_urls = []

# have to skip first one as it's "upcoming" so do [1:n]
for event_url in events_df['event'][1:]:

    event_html = urllib.request.urlopen(event_url)
    event_soup = bs(event_html, "lxml")
    fights_table = event_soup.find('tbody')
    fights_urls = fights_table.findAll('tr', attrs={'data-link': re.compile("^http://www.fightmetric.com/fight-details")})

    for i in fights_urls:
        fight = str(i).split(' onclick="doNav')[0]
        fight_url = fight.split('data-link="')[1].replace('"','')
        fights = {'event': event_url, 'fight': fight_url}
        fight_urls.append(fights)

event_fights_df = pd.DataFrame(fight_urls)
event_fights_df.head()
print(event_fights_df.shape)

############################################################################

fights_details = []
counter2 = 0

for fight_url in event_fights_df['fight']:

    counter2 = counter2 + 1
    print(counter2, 'of', len(event_fights_df['fight']), '- crawling fight details for:', fight_url)


    fight_html = urllib.request.urlopen(fight_url)
    fight_soup = bs(fight_html, "lxml")
    fight_end = fight_soup.find('p', attrs={'class':'b-fight-details__text'})
    fight_end_details = fight_end.findAll('i', {'class':'b-fight-details__text-item'})

    #fight_fighter1 = fight_soup.find('h3',{'class':'b-fight-details__person-name'})
    fight_event = fight_soup.find('h2', attrs={'class':'b-content__title'}).find('a').get_text().replace('\n','').replace('  ','')
    #fight_fighter2 = fight_soup.find('h3', attrs={'class':'b-fight-details__person-name'}).find('span').get_text().replace('\n','').replace('  ','')
    fight_division = fight_soup.find('i', attrs={'class':'b-fight-details__fight-title'}).get_text().replace('\n','').replace('  ','')
    fight_method = fight_soup.find('i', attrs={'style':'font-style: normal'}).get_text().replace('\n','').replace('  ','')
    fight_round = str(fight_end_details[0].get_text().split('\n')[4]).replace('        ','')
    fight_time = str(fight_end_details[1].get_text().split('\n')[5]).replace('        ','')
    fight_rounds = str(fight_end_details[2].get_text().split('\n')[4]).replace('        ','')
    fight_ref = str(fight_end_details[3].get_text().split('\n')[5]).replace('        ','')
    fight_end_details = fight_soup.findAll('p', {'class':'b-fight-details__text'})[1].get_text().replace('\n','').lstrip('          Details:                                                                                                                                                                                                                                                                                                                              ').rstrip('                        ')

    fight_details = {
                    'event': fight_event,
                    'division': fight_division,
                    'method': fight_method,
                    'round': fight_round,
                    'time': fight_time,
                    'rounds': fight_rounds,
                    'ref': fight_ref,
                    'end details': fight_end_details,
                    'fight': fight_url}

    fights_details.append(fight_details)

fights_details_df = pd.DataFrame(fights_details)
print(fights_details_df.shape)
fights_details_df.head()
########################################################################################################################################################

def get_fight_stats():
    fighter_stats = []

    fighter1_url = str(fight1_stats[0]).split('" style="color:#B10101">')[0].split('href="')[1]
    fighter1_name = str(fight1_stats[0]).split('" style="color:#B10101">')[1].rstrip(' </a>\n</p>')
    fighter2_url = str(fight1_stats[1]).split('" style="color:#B10101">')[0].split('href="')[1]
    fighter2_name = str(fight1_stats[1]).split('" style="color:#B10101">')[1].rstrip(' </a>\n</p>')
    fighter1_allrd_kd = str(fight1_stats[2].get_text()).replace('\n    ','').replace('\n      ','').replace('  ','')
    fighter2_allrd_kd = str(fight1_stats[3].get_text()).replace('\n    ','').replace('\n      ','').replace('  ','')
    fighter1_allrd_sig = str(fight1_stats[4].get_text()).split(' of ')[0].replace('\n    ','').replace('\n      ','').replace('  ','')
    fighter2_allrd_sig = str(fight1_stats[5].get_text()).split(' of ')[0].replace('\n    ','').replace('\n      ','').replace('  ','')
    fighter1_allrd_sig_att = str(fight1_stats[4].get_text()).split(' of ')[1].replace('\n    ','').replace('\n      ','').replace('  ','')
    fighter2_allrd_sig_att = str(fight1_stats[5].get_text()).split(' of ')[1].replace('\n    ','').replace('\n      ','').replace('  ','')
    fighter1_allrd_sig_perc = str(fight1_stats[6].get_text()).replace('\n    ','').replace('\n      ','').replace('  ','')
    fighter2_allrd_sig_perc = str(fight1_stats[7].get_text()).replace('\n    ','').replace('\n      ','').replace('  ','')
    fighter1_allrd_totstrk = str(fight1_stats[8].get_text()).split(' of ')[0].replace('\n    ','').replace('\n      ','').replace('  ','')
    fighter2_allrd_totstrk = str(fight1_stats[9].get_text()).split(' of ')[0].replace('\n    ','').replace('\n      ','').replace('  ','')
    fighter1_allrd_totstrk_att = str(fight1_stats[8].get_text()).split(' of ')[1].replace('\n    ','').replace('\n      ','').replace('  ','')
    fighter2_allrd_totstrk_att = str(fight1_stats[9].get_text()).split(' of ')[1].replace('\n    ','').replace('\n      ','').replace('  ','')
    fighter1_allrd_td = str(fight1_stats[10].get_text()).split(' of ')[0].replace('\n    ','').replace('\n      ','').replace('  ','')
    fighter2_allrd_td = str(fight1_stats[11].get_text()).split(' of ')[0].replace('\n    ','').replace('\n      ','').replace('  ','')
    fighter1_allrd_td_att = str(fight1_stats[10].get_text()).split(' of ')[1].replace('\n    ','').replace('\n      ','').replace('  ','')
    fighter2_allrd_td_att = str(fight1_stats[11].get_text()).split(' of ')[1].replace('\n    ','').replace('\n      ','').replace('  ','')
    fighter1_allrd_td_perc = str(fight1_stats[12].get_text()).replace('\n    ','').replace('\n      ','').replace('  ','')
    fighter2_allrd_td_perc = str(fight1_stats[13].get_text()).replace('\n    ','').replace('\n      ','').replace('  ','')
    fighter1_allrd_sub_att = str(fight1_stats[14].get_text()).replace('\n    ','').replace('\n      ','').replace('  ','')
    fighter2_allrd_sub_att = str(fight1_stats[15].get_text()).replace('\n    ','').replace('\n      ','').replace('  ','')
    fighter1_allrd_pass = str(fight1_stats[16].get_text()).replace('\n    ','').replace('\n      ','').replace('  ','')
    fighter2_allrd_pass = str(fight1_stats[17].get_text()).replace('\n    ','').replace('\n      ','').replace('  ','')
    fighter1_allrd_rev = str(fight1_stats[18].get_text()).replace('\n    ','').replace('\n      ','').replace('  ','')
    fighter2_allrd_rev = str(fight1_stats[19].get_text()).replace('\n    ','').replace('\n      ','').replace('  ','')

    try:
        fighter1_outcome = fighter1.find('i',{'class':'b-fight-details__person-status b-fight-details__person-status_style_green'}).get_text().lstrip('\n    ').rstrip('\n  ')
    except AttributeError:
        fighter1_outcome = fighter1.find('i',{'class':'b-fight-details__person-status b-fight-details__person-status_style_gray'}).get_text().lstrip('\n    ').rstrip('\n  ')
    try:
        fighter2_outcome = fighter2.find('i',{'class':'b-fight-details__person-status b-fight-details__person-status_style_gray'}).get_text().lstrip('\n    ').rstrip('\n  ')
    except AttributeError:
        fighter2_outcome = fighter2.find('i',{'class':'b-fight-details__person-status b-fight-details__person-status_style_green'}).get_text().lstrip('\n    ').rstrip('\n  ')


    fighter1_stats = {
                    'name': fighter1_name,
                    'profile': fighter1_url,
                    'kd': fighter1_allrd_kd,
                    'sig': fighter1_allrd_sig,
                    'sig att': fighter1_allrd_sig_att,
                    'sig perc': fighter1_allrd_sig_perc,
                    'tot strk': fighter1_allrd_totstrk,
                    'tot strk att': fighter1_allrd_totstrk_att,
                    'td': fighter1_allrd_td,
                    'td att': fighter1_allrd_td_att,
                    'sub att': fighter1_allrd_sub_att,
                    'pass': fighter1_allrd_pass,
                    'rev': fighter1_allrd_rev,
                    'outcome': fighter1_outcome}

    fighter2_stats = {
                    'name': fighter2_name,
                    'profile': fighter2_url,
                    'kd': fighter2_allrd_kd,
                    'sig': fighter2_allrd_sig,
                    'sig att': fighter2_allrd_sig_att,
                    'sig perc': fighter2_allrd_sig_perc,
                    'tot strk': fighter2_allrd_totstrk,
                    'tot strk att': fighter2_allrd_totstrk_att,
                    'td': fighter2_allrd_td,
                    'td att': fighter2_allrd_td_att,
                    'sub att': fighter2_allrd_sub_att,
                    'pass': fighter2_allrd_pass,
                    'rev': fighter2_allrd_rev,
                    'outcome': fighter2_outcome}

    fighter_stats.append(fighter1_stats)
    fighter_stats.append(fighter2_stats)


    return fighter_stats
    #return fighter1_url, fighter1_name, fighter2_url, fighter2_name, fighter1_allrd_kd, fighter2_allrd_kd, fighter1_allrd_sig, fighter2_allrd_sig, fighter1_allrd_sig_att, fighter2_allrd_sig_att, fighter1_allrd_sig_perc, fighter2_allrd_sig_perc, fighter1_allrd_totstrk, fighter2_allrd_totstrk, fighter1_allrd_totstrk_att, fighter2_allrd_totstrk_att, fighter1_allrd_td, fighter2_allrd_td, fighter1_allrd_td_att, fighter2_allrd_td_att, fighter1_allrd_td_perc, fighter2_allrd_td_perc, fighter1_allrd_sub_att, fighter2_allrd_sub_att, fighter1_allrd_pass, fighter2_allrd_pass, fighter1_allrd_rev, fighter2_allrd_rev
########################################################################################################################################################

fights = []
counter3 = 0

for fight1 in fights_details_df['fight']:

    counter3 = counter3 + 1
    print(counter3, 'of', len(fights_details_df['fight']), '- crawling fight stats for:', fight1)

    try:

        fight1_html = urllib.request.urlopen(fight1)
        fight1_soup = bs(fight1_html, "lxml")
        fight1_stats = fight1_soup.findAll('p', attrs={'class':'b-fight-details__table-text'})
        fighter1 = fight1_soup.findAll('div',{'class':'b-fight-details__person'})[0]
        fighter2 = fight1_soup.findAll('div',{'class':'b-fight-details__person'})[1]
        print('getting fight stats for fight:', fight1)
        # 1 round=len8,  len76
        # 2 round=len10, len114
        # 3 round=len12, len152
        # 4 round=len14, len190
        # 5 round=len16, len228

        if len(fight1_stats) == 76:
            #print('1 round fight')
            fight_stats = get_fight_stats()
            fight_stats[0].update({'fight':fight1})
            fight_stats[1].update({'fight':fight1})
            #print(fight_stats)
            fights.append(fight_stats)


        elif len(fight1_stats) == 114:
            #print('2 round fight')
            fight_stats = get_fight_stats()
            fight_stats[0].update({'fight':fight1})
            fight_stats[1].update({'fight':fight1})
            #print(fight_stats)
            fights.append(fight_stats)



        elif len(fight1_stats) == 152:
            #print('3 round fight')
            fight_stats = get_fight_stats()
            fight_stats[0].update({'fight':fight1})
            fight_stats[1].update({'fight':fight1})
            #print(fight_stats)
            fights.append(fight_stats)


        elif len(fight1_stats) == 190:
            #print('4 round fight')
            fight_stats = get_fight_stats()
            fight_stats[0].update({'fight':fight1})
            fight_stats[1].update({'fight':fight1})
            #print(fight_stats)
            fights.append(fight_stats)


        elif len(fight1_stats) == 228:
            #print('5 round fight')
            fight_stats = get_fight_stats()
            fight_stats[0].update({'fight':fight1})
            fight_stats[1].update({'fight':fight1})

            #print(fight_stats)
            fights.append(fight_stats)

            #fight_stats_df = pd.DataFrame(get_fight_stats())
    except Exception as ex:
        print('error:', ex)
        continue
########################################################################################################################################################

fights_flat = [y for x in fights for y in x]

fight_stats_df = pd.DataFrame(fights_flat)
fight_stats_df.head()
fight_stats_df.shape
########################################################################################################################################################


fighter_df.to_csv('fighters.to_csv', encoding='utf8', index=False)
events_df.to_csv('events.csv', encoding='utf8', index=False)
event_fights_df.to_csv('event_fights.csv', encoding='utf8', index=False)
fights_details_df.to_csv('fight_details.csv', encoding='utf8', index=False)
fight_stats_df.to_csv('fight_stats.csv', encoding='utf8', index=False)

time = (datetime.datetime.now() - start_time)
print("--- {} ---".format(datetime.timedelta(seconds=time.seconds)))
