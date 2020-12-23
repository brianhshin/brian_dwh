# -*- coding: utf-8 -*-

"""
author: Brian Shin
email: brianhesungshin@gmail.com
"""

"""
Builds rawdata tables for all CODWARZONE tables:
(profile_rawdata, game_details_rawdata, game_stats_rawdata).
script args can be passed through command line ex:
python warzone_rawdata_local.py --drop_tables Y
"""
import os
from datetime import datetime
import sqlite3
import pandas as pd
import numpy as np
import datetime as dt
import argparse

today = dt.datetime.now().strftime("%Y-%m-%d")
os.chdir('~/brian_dwh/codwarzone/output/current')

def get_local_files(file):

    df = pd.read_csv(file)
    df = df.fillna(0)
    df.replace(np.nan, '0', inplace=True)

    game_data = ['game_details', 'game_stats']

    if any(x in file for x in game_data):
        df['game_id'] = df['game_id'].astype('str')

    print(f'{file} df shape: {df.shape}')

    return df


def create_connection(drop=True):
  """
    Create a database connection to the SQLite database specified by db_file.
    :param db_file: database file
    :return: Connection object or None
  """
  conn = None
  try:
    # connect to db specified in run_etl_sim
    db = 'codwarzone.db'
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    if drop == True:
        # drop tables for each ETL run
        cur.execute('DROP TABLE IF EXISTS {}'.format('profile_rawdata'))
        cur.execute('DROP TABLE IF EXISTS {}'.format('game_details_rawdata'))
        cur.execute('DROP TABLE IF EXISTS {}'.format('game_stats_rawdata'))

        print('dropping all tables.')
    else:
        print('did not drop all tables.')

  except Error as e:
    print(e)

  return conn


def profile_rawdata(conn, df):

    cur = conn.cursor()

    profile_create_sql = """
        CREATE TABLE IF NOT EXISTS profile_rawdata (
            profile_id TEXT NOT NULL,
            profile_date TEXT NOT NULL,
            gamer_id TEXT NOT NULL,
            playtime TEXT NOT NULL,
            games TEXT NOT NULL,
            level TEXT NOT NULL,
            level_rank TEXT NOT NULL,
            wins TEXT NOT NULL,
            wins_rank TEXT NOT NULL,
            kd TEXT NOT NULL,
            kd_rank TEXT NOT NULL,
            dmg_per_game TEXT NOT NULL,
            top5 TEXT NOT NULL,
            top5_rank TEXT NOT NULL,
            top10 TEXT NOT NULL,
            top10_rank TEXT NOT NULL,
            top25 TEXT NOT NULL,
            top25_rank TEXT NOT NULL,
            kills TEXT NOT NULL,
            deaths TEXT NOT NULL,
            downs TEXT NOT NULL,
            avg_life TEXT NOT NULL,
            avg_life_rank TEXT NOT NULL,
            score TEXT NOT NULL,
            score_per_min TEXT NOT NULL,
            score_per_min_rank TEXT NOT NULL,
            score_per_game TEXT NOT NULL,
            cash TEXT NOT NULL,
            contracts TEXT NOT NULL,
            contracts_rank TEXT NOT NULL,
            win_perc TEXT NOT NULL,
            win_perc_rank TEXT NOT NULL
        );
        """

    cur.execute(profile_create_sql)
    columns = str(list(df.columns)).replace("'","").replace("'","").replace("[", "").replace("]", "")

    for i in range(df.shape[0]):
        values = str(list(df.values[i])).replace("[", "").replace("]", "")
        insert_sql = f"""INSERT OR IGNORE INTO profile_rawdata ({columns}) VALUES({values});"""
        cur.execute(insert_sql)

    conn.commit()
    print('inserted values into profile_rawdata')


def game_details_rawdata(conn, df):

    cur = conn.cursor()

    game_details_create_sql = """
        CREATE TABLE IF NOT EXISTS game_details_rawdata (
            game_id TEXT NOT NULL,
            game_date TEXT NOT NULL,
            game_time TEXT NOT NULL,
            gamer_id TEXT NOT NULL,
            game_type TEXT NOT NULL,
            placement TEXT NOT NULL,
            kills TEXT NOT NULL,
            cache_open TEXT NOT NULL,
            damage TEXT NOT NULL,
            damage_per_min TEXT NOT NULL
        );
        """

    cur.execute(game_details_create_sql)

    columns = str(list(df.columns)).replace("'","").replace("'","").replace("[", "").replace("]", "")

    for i in range(df.shape[0]):
        values = str(list(df.values[i])).replace("[", "").replace("]", "")
        insert_sql = f"""INSERT OR IGNORE INTO game_details_rawdata ({columns}) VALUES({values});
        """
        cur.execute(insert_sql)

    conn.commit()
    print('inserted values into game_details_rawdata')


def game_stats_rawdata(conn, df):

    cur = conn.cursor()

    game_stats_create_sql = """
        CREATE TABLE IF NOT EXISTS game_stats_rawdata (
            game_id TEXT NOT NULL,
            gamer_id TEXT NOT NULL,
            kills TEXT NOT NULL,
            deaths TEXT NOT NULL,
            assists TEXT NOT NULL,
            kd TEXT NOT NULL,
            damage TEXT NOT NULL,
            score TEXT NOT NULL,
            score_per_min TEXT NOT NULL,
            wall_bangs TEXT NOT NULL,
            headshots TEXT NOT NULL,
            reviver TEXT NOT NULL,
            team_placement TEXT NOT NULL,
            time_played TEXT NOT NULL,
            total_xp TEXT NOT NULL,
            score_xp TEXT NOT NULL,
            match_xp TEXT NOT NULL,
            challenge_xp TEXT NOT NULL,
            medal_xp TEXT NOT NULL,
            bonus_xp TEXT NOT NULL,
            misc_xp TEXT NOT NULL,
            gulag_kills TEXT NOT NULL,
            gulag_deaths TEXT NOT NULL,
            distance_traveled TEXT NOT NULL,
            percent_time_moving TEXT NOT NULL,
            team_survival TEXT NOT NULL,
            team_wiped TEXT NOT NULL,
            executions TEXT NOT NULL,
            nearmisses TEXT NOT NULL,
            kiosk_buy TEXT NOT NULL,
            damage_taken TEXT NOT NULL,
            mission_pickup_tablet TEXT NOT NULL,
            last_stand_kill TEXT NOT NULL,
            longest_streak TEXT NOT NULL,
            cache_open TEXT NOT NULL
        );
        """

    cur.execute(game_stats_create_sql)

    columns = str(list(df.columns)).replace("'","").replace("'","").replace("[", "").replace("]", "")

    for i in range(df.shape[0]):
        values = str(list(df.values[i])).replace("[", "").replace("]", "")
        insert_sql = f"""INSERT OR IGNORE INTO game_stats_rawdata ({columns}) VALUES({values});"""
        cur.execute(insert_sql)

    conn.commit()
    print('inserted values into game_stats_rawdata')


def drop_tables_arg():

    parser = argparse.ArgumentParser(description='this python script will run CODWARZONE.RAWDATA tables.')
    parser.add_argument('--drop_tables', default='N', help='If Y, this drops all of the tables in CODWARZONE.RAWDATA and then kicks off a run of the most recent load. If N, it appends.')

    args=parser.parse_args()

    drop_tables = args.drop_tables.upper()

    return drop_tables


def create_rawdata_tables():

    files = os.listdir()

    for file in files:
        if file.split('.')[1] != 'csv':
            files.remove(file)

        if 'profile' in file:
            profile = get_local_files(file)
            profile_rawdata(conn, profile)
        elif 'game_details' in file:
            game_details = get_local_files(file)
            game_details_rawdata(conn, game_details)
        elif 'game_stats' in file:
            game_stats = get_local_files(file)
            game_stats_rawdata(conn, game_stats)

    print('updated CODWARZONE.RAWDATA tables. itspizzatime.jpeg')


def get_args():

    parser = argparse.ArgumentParser(description='warzone_rawdata_local.py build rawdata tables for all CODWARZONE tables.')
    parser.add_argument('--drop_tables', default='N', help='drop tables, or not (Y or N). default is N.')

    args = parser.parse_args()

    drop_tables = args.drop_tables.upper()

    return drop_tables

if __name__ == '__main__':

    drop_tables = get_args()

    if drop_tables == 'Y':
        print('dropping tables.')
        drop_check = input('Are you sure you want to drop all the tables in CODWARZONE.RAWDATA (Y/N)?')
        if drop_check.upper() != 'Y':
            print('peace- cancelling script.')
            sys.exit()
        else:
            print('dropping tables in CODWARZONE.RAWDATA.')
            conn = create_connection(drop=True)
    else:
        conn = create_connection(drop=False)

    create_rawdata_tables()
