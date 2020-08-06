# -*- coding: utf-8 -*-

"""
author: Brian Shin
date: 04.28.2020
email: brianhesungshin@gmail.com
"""

"""

"""
import os
from datetime import datetime
import sqlite3
import pandas as pd
import numpy as np
import datetime as dt
import argparse


today = dt.datetime.now().strftime("%Y-%m-%d")
os.chdir('/Users/brianshin/brian/work/repo/brian_dwh/league/output')

def get_local_files():

    favchamps = pd.read_csv('favchamps.csv')
    games_combined = pd.read_csv('games_combined.csv')
    games_stats = pd.read_csv('games_stats.csv')
    playswith = pd.read_csv('playswith.csv')
    profile = pd.read_csv('profile.csv').drop(columns='tags')
    roles = pd.read_csv('roles.csv')

    print('favchamps shape:', favchamps.shape)
    print('games_combined shape:', games_combined.shape)
    print('games_stats shape:', games_stats.shape)
    print('playswith shape:', playswith.shape)
    print('profile shape:', profile.shape)
    print('roles shape:', roles.shape)

    return favchamps, games_combined, games_stats, playswith, profile, roles


def create_connection(drop=True):
  """
    Create a database connection to the SQLite database specified by db_file.
    :param db_file: database file
    :return: Connection object or None
  """
  conn = None
  try:
    # connect to db specified in run_etl_sim
    db = 'leagueofgraphs.db'
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    if drop == True:
        # drop tables for each ETL run
        cur.execute('DROP TABLE IF EXISTS {}'.format('profile_staging'))
        cur.execute('DROP TABLE IF EXISTS {}'.format('favchamp_staging'))
        cur.execute('DROP TABLE IF EXISTS {}'.format('role_staging'))
        cur.execute('DROP TABLE IF EXISTS {}'.format('playswith_staging'))
        cur.execute('DROP TABLE IF EXISTS {}'.format('game_staging'))
        cur.execute('DROP TABLE IF EXISTS {}'.format('game_dmg_staging'))
        cur.execute('DROP TABLE IF EXISTS {}'.format('game_misc_staging'))
        print('dropping all tables.')
    else:
        print('did not drop all tables.')

  except Error as e:
    print(e)

  return conn


def profile_staging(conn, df):

    # df = profile
    # profile_db = 'profile_staging.db'
    # conn = create_connection(profile_db)
    cur = conn.cursor()

    # if drop_table == True:
    #     cur.execute('DROP TABLE IF EXISTS {}'.format('profile_staging'))
    #     print('dropped table: profile_staging')

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
        	avg_assists INTEGER NOT NULL
        );
        """

    cur.execute(profile_create_sql)

    columns = str(list(df.columns)).replace("'","").replace("'","").replace("[", "").replace("]", "")
    values = str(list(df.values[0])).replace("[", "").replace("]", "")

    insert_sql = f"""INSERT INTO profile_staging ({columns}) VALUES({values});"""
    cur.execute(insert_sql)
    conn.commit()
    print('inserted values into profile_staging')


def favchamp_staging(conn, df):

    # favchamp_db = 'favchamp_staging.db'
    # conn = create_connection(favchamp_db)
    cur = conn.cursor()

    # if drop_table == True:
    #     cur.execute('DROP TABLE IF EXISTS {}'.format('favchamp_staging'))
    #     print('dropped table: favchamp_staging')


    favchamp_create_sql = """
        CREATE TABLE IF NOT EXISTS favchamp_staging (
        	date TEXT PRIMARY KEY,
            favchamp1 TEXT NOT NULL,
            favchamp1_rank TEXT NOT NULL,
            favchamp1_kills TEXT NOT NULL,
            favchamp1_deaths TEXT NOT NULL,
            favchamp1_assists TEXT NOT NULL,
            favchamp1_played TEXT NOT NULL,
            favchamp1_winrate TEXT NOT NULL,
            favchamp2 TEXT NOT NULL,
            favchamp2_rank TEXT NOT NULL,
            favchamp2_kills TEXT NOT NULL,
            favchamp2_deaths TEXT NOT NULL,
            favchamp2_assists TEXT NOT NULL,
            favchamp2_played TEXT NOT NULL,
            favchamp2_winrate TEXT NOT NULL,
            favchamp3 TEXT NOT NULL,
            favchamp3_rank TEXT NOT NULL,
            favchamp3_kills TEXT NOT NULL,
            favchamp3_deaths TEXT NOT NULL,
            favchamp3_assists TEXT NOT NULL,
            favchamp3_played TEXT NOT NULL,
            favchamp3_winrate TEXT NOT NULL,
            favchamp4 TEXT NOT NULL,
            favchamp4_rank TEXT NOT NULL,
            favchamp4_kills TEXT NOT NULL,
            favchamp4_deaths TEXT NOT NULL,
            favchamp4_assists TEXT NOT NULL,
            favchamp4_played TEXT NOT NULL,
            favchamp4_winrate TEXT NOT NULL
        );
        """
    cur.execute(favchamp_create_sql)

    columns = str(list(df.columns)).replace("'","").replace("'","").replace("[", "").replace("]", "")
    values = str(list(df.values[0])).replace("[", "").replace("]", "")

    insert_sql = f"""INSERT INTO favchamp_staging ({columns}) VALUES({values});"""
    cur.execute(insert_sql)
    conn.commit()
    print('inserted values into favchamp_staging')


def role_staging(conn, df):

    # role_db = 'role_staging.db'
    # conn = create_connection(role_db)
    cur = conn.cursor()

    # if drop_table == True:
    #     cur.execute('DROP TABLE IF EXISTS {}'.format('role_staging'))
    #     print('dropped table: role_staging')

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

    columns = str(list(df.columns)).replace("'","").replace("'","").replace("[", "").replace("]", "")
    values = str(list(df.values[0])).replace("[", "").replace("]", "")

    insert_sql = f"""INSERT INTO role_staging ({columns}) VALUES({values});"""
    cur.execute(insert_sql)
    conn.commit()
    print('inserted values into role_staging')


def playswith_staging(conn, df):

    # playswith_db = 'playswith_staging.db'
    # conn = create_connection(playswith_db)
    cur = conn.cursor()

    # if drop_table == True:
    #     cur.execute('DROP TABLE IF EXISTS {}'.format('playswith_staging'))
    #     print('dropped table: playswith_staging')

    playswith_create_sql = """
        CREATE TABLE IF NOT EXISTS playswith_staging (
            date TEXT PRIMARY KEY,
            playswith1_name TEXT NOT NULL,
            playswith1_rank TEXT NOT NULL,
            playswith1_played TEXT NOT NULL,
            playswith1_winrate TEXT NOT NULL,
            playswith2_name TEXT NOT NULL,
            playswith2_rank TEXT NOT NULL,
            playswith2_played TEXT NOT NULL,
            playswith2_winrate TEXT NOT NULL,
            playswith3_name TEXT NOT NULL,
            playswith3_rank TEXT NOT NULL,
            playswith3_played TEXT NOT NULL,
            playswith3_winrate TEXT NOT NULL,
            playswith4_name TEXT NOT NULL,
            playswith4_rank TEXT NOT NULL,
            playswith4_played TEXT NOT NULL,
            playswith4_winrate TEXT NOT NULL
        );
        """

    cur.execute(playswith_create_sql)

    columns = str(list(df.columns)).replace("'","").replace("'","").replace("[", "").replace("]", "")
    values = str(list(df.values[0])).replace("[", "").replace("]", "")

    insert_sql = f"""INSERT INTO playswith_staging ({columns}) VALUES({values});"""
    cur.execute(insert_sql)
    conn.commit()
    print('inserted values into playswith_staging')


def game_staging(conn, df):

    # game_db = 'game_staging.db'
    # conn = create_connection(game_db)
    cur = conn.cursor()

    # if drop_table == True:
    #     cur.execute('DROP TABLE IF EXISTS {}'.format('game_staging'))
    #     print('dropped table: game_staging')

    game_create_sql = """
        CREATE TABLE IF NOT EXISTS game_staging (
            date TEXT NOT NULL,
            game_id TEXT NOT NULL,
            legend TEXT NOT NULL,
            largest_killing_spree TEXT NOT NULL,
            largest_multikill TEXT NOT NULL,
            crowd_control_score TEXT NOT NULL,
            total_dmg_to_champs TEXT NOT NULL,
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
            true_dmg_taken TEXT NOT NULL,
            self_mtgted_dmg TEXT NOT NULL,
            vision_score TEXT NOT NULL,
            wards TEXT NOT NULL,
            wards_killed TEXT NOT NULL,
            control_wards_purch TEXT NOT NULL,
            gold_earned TEXT NOT NULL,
            gold_spent TEXT NOT NULL,
            minions_killed TEXT NOT NULL,
            neutral_minions_killed TEXT NOT NULL,
            neutral_minions_killed_in_team_jungle TEXT NOT NULL,
            neutral_minions_killed_in_enemy_jungle TEXT NOT NULL,
            towers_destroyed TEXT NOT NULL,
            inhibitors_destroyed TEXT NOT NULL
        );
        """

    cur.execute(game_create_sql)

    columns = str(list(df.columns)).replace("'","").replace("'","").replace("[", "").replace("]", "")
    values = str(list(df.values[0])).replace("[", "").replace("]", "")

    insert_sql = f"""INSERT INTO game_staging ({columns}) VALUES({values});"""
    cur.execute(insert_sql)
    conn.commit()
    print('inserted values into game_staging')


# included in game_stats?
def game_dmg_staging(df):

    game_dmg_db = 'game_dmg_staging.db'
    conn = create_connection(game_dmg_db)
    cur = conn.cursor()
    game_dmg_create_sql = """
        CREATE TABLE IF NOT EXISTS game_dmg_staging (
            game_id TEXT PRIMARY KEY,
            largest_killing_spree TEXT NOT NULL,
            largest_multikill TEXT NOT NULL,
            crowd_control_score TEXT NOT NULL,
            total_dmg_to_champs TEXT NOT NULL,
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

    columns = str(list(df.columns)).replace("'","").replace("'","").replace("[", "").replace("]", "")
    values = str(list(df.values[0])).replace("[", "").replace("]", "")

    insert_sql = f"""INSERT INTO game_dmg_staging ({columns}) VALUES({values});"""
    cur.execute(insert_sql)

    conn.commit()

# included in game_stats?
def game_misc_staging(df):

    game_misc_db = 'game_misc_staging.db'
    conn = create_connection(game_misc_db)
    cur = conn.cursor()
    game_misc_create_sql = """
        CREATE TABLE IF NOT EXISTS game_misc_staging (
            game_id TEXT PRIMARY KEY,
            self_mtgted_dmg TEXT NOT NULL,
            vision_score TEXT NOT NULL,
            wards TEXT NOT NULL,
            wards_killed TEXT NOT NULL,
            control_wards_purch TEXT NOT NULL,
            gold_earned	TEXT NOT NULL,
            gold_spent TEXT NOT NULL,
            minions_killed TEXT NOT NULL,
            neutral_minions_killed TEXT NOT NULL,
            neutral_minions_killed_in_team_jungle TEXT NOT NULL,
            neutral_minions_killed_in_enemy_jungle TEXT NOT NULL,
            towers_destroyed TEXT NOT NULL,
            inhibitors_destroyed TEXT NOT NULL
        );
        """

    cur.execute(game_misc_create_sql)

    columns = str(list(df.columns)).replace("'","").replace("'","").replace("[", "").replace("]", "")
    values = str(list(df.values[0])).replace("[", "").replace("]", "")

    insert_sql = f"""INSERT INTO game_misc_staging ({columns}) VALUES({values});"""
    cur.execute(insert_sql)

    conn.commit()


def drop_tables_arg():

    parser = argparse.ArgumentParser(description='this python script will run LEAGUEOFGRAPHS staging tables.')
    parser.add_argument('--drop_tables', default='N', help='If Y, this drops all of the tables in LEAGUEOFGRAPHS and then kicks off a run of the most recent load. If N, it appends.')

    args=parser.parse_args()

    drop_tables = args.drop_tables.upper()

    return drop_tables


def create_staging_tables():
    favchamps, games_combined, games_stats, playswith, profile, roles = get_local_files()
    profile_staging(conn, profile)
    favchamp_staging(conn, favchamps)
    role_staging(conn, roles)
    playswith_staging(conn, playswith)
    game_staging(conn, games_combined)
    # game_dmg_staging(games_combined)
    # game_misc_staging(games_combined)
    print('updated league tables. DONE.')



if __name__ == '__main__':

    drop_tables = drop_tables_arg()

    if drop_tables == 'Y':
        print('dropping tables.')
        drop_check = input('Are you sure you want to drop all the tables in LEAGUEOFGRAPHS (y/n)? ')
        if drop_check.lower() != 'y':
            print('peace- cancelling script.')
            sys.exit()
        else:
            print('dropping tables in LEAGUEOFGRAPHS.')
            conn = create_connection(drop=True)
    else:
        conn = create_connection(drop=False)

    create_staging_tables()
