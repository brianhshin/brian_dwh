# -*- coding: utf-8 -*-

"""
author: Brian Shin
date: 04.28.2020
email: brianhesungshin@gmail.com
"""

"""

"""
import os
from sys import argv
from random import randint
from datetime import datetime
from operator import itemgetter
from pprint import pprint
import sqlite3
import itertools
import operator
import boto3
import datetime as dt
import pandas as pd

today = dt.datetime.now().strftime("%Y%m%d")

# this function Downloads a file from S3 onto the local machine
def download_file_s3(bucket, s3_location, local_location, file_type):
    # bucket - Bucket Name
    # s3Location - File Name on S3
    # localLocation - The desired local file name and path to indicate where it
    #                 should be downloaded to
    # fileType - The file type (ie Parquet, CSV, JSON, etc )

    s3 = boto3.resource('s3')
    s3.Bucket(bucket).download_file(s3_location, local_location)

    if file_type.lower() == 'parquet':
        df = pd.read_parquet(local_location)
    elif file_type.lower() == 'csv':
        df = pd.read_csv(local_location)
    elif file_type.lower() == 'pickle':
        df = pd.read_pickle(local_location)
    elif file_type.lower() == 'json':
        df = pd.read_json(local_location)
    else:
        raise ValueError('Unknown: FileType in DownloadFile_S3')

    try:
        os.remove(local_location)
    except OSError:
        pass

    return df


def get_s3_file(category):
    df = download_file_s3(
        'leagueofgraphs',
        f'{category}/{category}_{today}.csv',
        '/home/ubuntu/ubuntu/brian_dwh/league/league_temp_output/temp',
        'csv')
    return df


def get_s3_files():
    favchamps = get_s3_file('favchamps')
    games_combined = get_s3_file('games_combined')
    games_stats = get_s3_file('games_stats')
    playswith = get_s3_file('playswith')
    profile = get_s3_file('profile')
    roles = get_s3_file('roles')

    print('favchamps shape:', favchamps.shape, favchamps.columns)
    print('games_combined shape:', favchamps.shape, favchamps.columns)
    print('games_stats shape:', games_stats.shape, games_stats.columns)
    print('playswith shape:', playswith.shape, playswith.columns)
    print('profile shape:', profile.shape, profile.columns)
    print('roles shape:', roles.shape, roles.columns)

    return favchamps, games_combined, games_stats, playswith, profile, roles


def create_connection(db):
  """
    Create a database connection to the SQLite database specified by db_file.
    :param db_file: database file
    :return: Connection object or None
  """
  conn = None
  try:
    # connect to db specified in run_etl_sim
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    # drop tables for each ETL run
    cur.execute('DROP TABLE IF EXISTS {}'.format('profile_staging'))
    cur.execute('DROP TABLE IF EXISTS {}'.format('favchamp_staging'))
    cur.execute('DROP TABLE IF EXISTS {}'.format('role_staging'))
    cur.execute('DROP TABLE IF EXISTS {}'.format('playswith_staging'))
    cur.execute('DROP TABLE IF EXISTS {}'.format('game_staging'))
    cur.execute('DROP TABLE IF EXISTS {}'.format('game_dmg_staging'))
    cur.execute('DROP TABLE IF EXISTS {}'.format('game_misc_staging'))

  except Error as e:
    print(e)

  return conn


def profile_staging():

    profile_db = 'profile_staging.db'
    conn = create_connection(profile_db)
    cur = conn.cursor()
    profile_create_sql = """
        CREATE TABLE IF NOT EXISTS profile_staging (
        	date TEXT PRIMARY KEY,
        	tier TEXT NOT NULL,
        	queue TEXT NOT NULL,
            rank INTEGER NOT NULL,
        	region_rank TEXT NOT NULL,
        	lp INTEGER NOT NULL,
        	wins INTEGER NOT NULL,
        	losses INTEGER NOT NULL,
        	games INTEGER NOT NULL,
        	winrate TEXT NOT NULL,
        	avg_kills INTEGER NOT NULL,
        	avg_deaths INTEGER NOT NULL,
        	avg_assists INTEGER NOT NULL,
        	tags TEXT NOT NULL
        );
        """

    cur.execute(profile_create_sql)
    conn.commit()


def favchamp_staging():

    favchamp_db = 'favchamp_staging.db'
    conn = create_connection(favchamp_db)
    cur = conn.cursor()
    favchamp_create_sql = """
        CREATE TABLE IF NOT EXISTS favchamp_staging (
        	date TEXT PRIMARY KEY,
            favchamp1 TEXT NOT NULL,
            favchamp1_rank TEXT NOT NULL,
            favchamp1_regrank TEXT NOT NULL,
            favchamp1_kills TEXT NOT NULL,
            favchamp1_deaths TEXT NOT NULL,
            favchamp1_assists TEXT NOT NULL,
            favchamp1_played TEXT NOT NULL,
            favchamp1_winrate TEXT NOT NULL,
            favchamp2 TEXT NOT NULL,
            favchamp2_rank TEXT NOT NULL,
            favchamp2_regrank TEXT NOT NULL,
            favchamp2_kills TEXT NOT NULL,
            favchamp2_deaths TEXT NOT NULL,
            favchamp2_assists TEXT NOT NULL,
            favchamp2_played TEXT NOT NULL,
            favchamp2_winrate TEXT NOT NULL,
            favchamp3 TEXT NOT NULL,
            favchamp3_rank TEXT NOT NULL,
            favchamp3_regrank TEXT NOT NULL,
            favchamp3_kills TEXT NOT NULL,
            favchamp3_deaths TEXT NOT NULL,
            favchamp3_assists TEXT NOT NULL,
            favchamp3_played TEXT NOT NULL,
            favchamp3_winrate TEXT NOT NULL,
            favchamp4 TEXT NOT NULL,
            favchamp4_rank TEXT NOT NULL,
            favchamp4_regrank TEXT NOT NULL,
            favchamp4_kills TEXT NOT NULL,
            favchamp4_deaths TEXT NOT NULL,
            favchamp4_assists TEXT NOT NULL,
            favchamp4_played TEXT NOT NULL,
            favchamp4_winrate TEXT NOT NULL
        );
        """

    cur.execute(favchamp_create_sql)
    conn.commit()


def role_staging():

    role_db = 'role_staging.db'
    conn = create_connection(role_db)
    cur = conn.cursor()
    role_create_sql = """
        CREATE TABLE IF NOT EXISTS role_staging (
            date TEXT PRIMARY KEY,
            role1 TEXT NOT NULL,
            role1_played TEXT NOT NULL,
            role1_winrate TEXT NOT NULL,
            role2 TEXT NOT NULL,
            role2_played TEXT NOT NULL,
            role2_winrate TEXT NOT NULL,
            role3 TEXT NOT NULL,
            role3_played TEXT NOT NULL,
            role3_winrate TEXT NOT NULL,
            role4 TEXT NOT NULL,
            role4_played TEXT NOT NULL,
            role4_winrate TEXT NOT NULL
        );
        """

    cur.execute(role_create_sql)
    conn.commit()


def playswith_staging():

    playswith_db = 'playswith_staging.db'
    conn = create_connection(playswith_db)
    cur = conn.cursor()
    playswith_create_sql = """
        CREATE TABLE IF NOT EXISTS playswith_staging (
            playswith_id TEXT PRIMARY KEY,
            date TEXT NOT NULL,
            gamer_id TEXT NOT NULL,
            playswith1_name TEXT NOT NULL,
            playswith1_rank TEXT NOT NULL,
            playswith1_played TEXT NOT NULL,
            playswith1_winrate TEXT NOT NULL
        );
        """

    cur.execute(playswith_create_sql)
    conn.commit()


def game_staging():

    game_db = 'game_staging.db'
    conn = create_connection(game_db)
    cur = conn.cursor()
    game_create_sql = """
        CREATE TABLE IF NOT EXISTS game_staging (
            game_id TEXT PRIMARY KEY,
            game_played_date TEXT NOT NULL,
            tags TEXT NOT NULL,
            runes TEXT NOT NULL,
            legend TEXT NOT NULL,
            kills TEXT NOT NULL,
            deaths TEXT NOT NULL,
            assists TEXT NOT NULL
        );
        """

    cur.execute(game_create_sql)
    conn.commit()


def game_dmg_staging():

    game_dmg_db = 'game_dmg_staging.db'
    conn = create_connection(game_dmg_db)
    cur = conn.cursor()
    game_dmg_create_sql = """
        CREATE TABLE IF NOT EXISTS game_dmg_staging (
            game_id TEXT PRIMARY KEY,
            largest_killing_spree TEXT NOT NULL,
            largest_multikill TEXT NOT NULL,
            crowd_control_score TEXT NOT NULL,
            physical_dmg_to_champs TEXT NOT NULL,
            magic_dmg_to_champs TEXT NOT NULL,
            true_dmg_to_champs TEXT NOT NULL,
            total_dmg_dealt TEXT NOT NULL,
            physical_dmg_dealt TEXT NOT NULL,
            magic_dmg_dealt TEXT NOT NULL,
            true_dmg_dealt TEXT NOT NULL,
            total_dmg_to_turrets TEXT NOT NULL,
            total_dmg_to_objs TEXT NOT NULL,
            dmg_healed TEXT NOT NULL,
            dmg_taken TEXT NOT NULL,
            physical_dmg_taken TEXT NOT NULL,
            magic_dmg_taken TEXT NOT NULL,
            true_dmg_taken TEXT NOT NULL
        );
        """

    cur.execute(game_dmg_create_sql)
    conn.commit()


def game_misc_staging():

    game_misc_db = 'game_misc_staging.db'
    conn = create_connection(game_misc_db)
    cur = conn.cursor()
    game_misc_create_sql = """
        CREATE TABLE IF NOT EXISTS game_misc_staging (
            game_id TEXT PRIMARY KEY,
            vision_score TEXT NOT NULL,
            wards TEXT NOT NULL,
            wards_killed TEXT NOT NULL,
            control_wards_purch TEXT NOT NULL,
            gold_earned	gold_spent TEXT NOT NULL,
            minions_killed TEXT NOT NULL,
            neutral_minions_killed TEXT NOT NULL,
            neutral_minions_killed_in_team_jungle TEXT NOT NULL,
            neutral_minions_killed_in_enemy_jungle TEXT NOT NULL,
            towers_destroyed TEXT NOT NULL,
            inhibitors_destroyed TEXT NOT NULL
        );
        """

    cur.execute(game_misc_create_sql)
    conn.commit()


def create_staging_tables():
    get_s3_files()
    profile_staging()
    favchamp_staging()
    role_staging()
    playswith_staging()
    game_staging()
    game_dmg_staging()
    game_misc_staging()


#
# cur = conn.cursor()
#
# insert_sql = f"""INSERT INTO profile_staging ({columns}) VALUES({values});"""
#
# cur.execute(insert_sql)





#
#   # # get column names using dict keys of the first row of deduped_updates
#   # columns = ', '.join(deduped_updates[0].keys())
#   # values = ':id, :name, :cookies, :updated_at'
#   #
#   # iterate on each row of deduped_updates to insert/update into table
#   # for deduped_update in deduped_updates:
#   #
#   #   # create list of values from deduped_update dict row
#   #   row_values = list(deduped_update.values())
#   #   # id has been defined as primary key, so if the id already exists, ignore. else, insert row values
#   #   insert_sql = """
#   #   INSERT OR IGNORE INTO joes({})
#   #   VALUES({})
#   #   ;
#   #   """.format(columns,values)
#   #   cur = conn.cursor()
#   #   cur.execute(insert_sql, row_values)
#   #   conn.commit()
#   #
#   #   # if id exists but updated_at is more recent than what exists in the table, update row for that id
#   #   update_sql = """
#   #   UPDATE joes SET name = '{}', cookies = {}, updated_at = {} WHERE (id = {} AND updated_at < {})
#   #   ;
#   #   """.format(row_values[1],row_values[2],row_values[3],row_values[0],row_values[3])
#   #   cur = conn.cursor()
#   #   cur.execute(update_sql)
#   #   conn.commit()
#
#
# def update_sqlite_newest_user_data(expected_user_rows, conn):
#   """
#     Upsert NEWEST_USER_DATA into separate table to be used in compare_final_user_rows_without_fetching to compare results through SQL.
#   """
#
#   cur = conn.cursor()
#   # create new table for expected values from NEWEST_USER_DATA
#   cur.execute("""
#     CREATE TABLE IF NOT EXISTS joes_expected (
#     	id INTEGER PRIMARY KEY,
#     	name TEXT NOT NULL,
#     	cookies INTEGER NOT NULL,
#     	updated_at INTEGER NOT NULL
#     );
#     """)
#
#   columns = 'id, name, cookies, updated_at'
#   values = ':id, :name, :cookies, :updated_at'
#   for y in range(len(expected_user_rows)):
#       expected_user_row = expected_user_rows[y]
#       sql = 'INSERT OR IGNORE INTO joes_expected ({}) VALUES({});'.format(columns, values)
#       cur.execute(sql, expected_user_row)
#
#
# def compare_final_user_rows_without_fetching(expected_user_rows, conn):
#   """
#      Compare sqlitedb users table rows to values in expected_user_rows by users.id without fetching into python for manipulation (aka use SQL).
#      Using the 2 tables created through update_sqlite_user_without_fetching (joes) and update_sqlite_newest_user_data (joes_expected), compare results.
#   """
#   # create joes_expected table of NEWEST_USER_DATA values
#   update_sqlite_newest_user_data(expected_user_rows, conn)
#
#   # use EXCEPT to run minuses on any deviations that table joe has from joe_expected
#   cur = conn.cursor()
#   cur.execute("""SELECT * FROM joes_expected EXCEPT SELECT * FROM joes;""")
#
#   # create empty list to be populated with any minuses results
#   except_results = []
#   except_rows = cur.fetchall()
#   columns = ['id', 'name', 'cookies', 'updated_at']
#   for except_row in except_rows:
#     minuses = dict(zip(columns, except_row))
#     except_results.append(minuses)
#
#   # use LEFT JOIN (as another option) on id to see any differences that table joe has from joe_expected
#   cur.execute("""
#       SELECT *
#       FROM joes_expected
#       LEFT JOIN joes ON joes_expected.id = joes.id
#       WHERE joes_expected.name != joes.name
#       OR joes_expected.cookies != joes.cookies
#       OR joes_expected.updated_at != joes.updated_at
#       ;
#       """)
#
#   # create empty list to be populated with any joined differences results
#   join_results = []
#   join_rows = cur.fetchall()
#   columns = ['id', 'name', 'cookies', 'updated_at']
#   for join_row in join_rows:
#     diffs = dict(zip(columns,join_row))
#     join_results.append(diffs)
#
#   # if results list is empty, joes and joes_expected are identical matches
#   if len(except_results) < 1 and len(join_results) < 1:
#     all_rows_match = True
#   else:
#     all_rows_match = False
#
#   # print statements for added visibility in QA, especially when results are not a 1:1 match
#   # for minuses and diffs that may show up
#   print('minuses:', len(except_results))
#   pprint(except_results)
#   print('joined_diffs:', len(join_results))
#   pprint(join_results)
#   print()
#
#   # print values in joes table
#   cur.execute("""SELECT * FROM joes ORDER BY id ASC;""")
#   joes_rows = cur.fetchall()
#   print('---------joes---------')
#   pprint(joes_rows)
#   print()
#
#   # print values in joes_expected table
#   cur.execute("""SELECT * FROM joes_expected ORDER BY id ASC;""")
#   joes_expected_rows = cur.fetchall()
#   conn.commit()
#   print('-----joes_expected----')
#   pprint(joes_expected_rows)
#   print()
#
#   return all_rows_match
#
#
# def run_etl_sim(runs):
#   """
#     runs our little etl sim - feel free to edit for debugging but must remain unedited in final product
#   """
#
#   db = 'joes.db'
#   conn = create_connection(db)
#
#   for _ in range(0, runs):
#     updates = generate_user_updates(1,2)
#     deduped_updates = remove_outdated_duplicates(updates)
#     update_sqlite_user_without_fetching(deduped_updates, conn)
#
#   if compare_final_user_rows_without_fetching(NEWEST_USER_DATA, conn):
#     print("Success! User rows match expected values.")
#   else:
#     print("Failure! User rows do not match expected values.")


if __name__ == '__main__':
    create_staging_tables()
