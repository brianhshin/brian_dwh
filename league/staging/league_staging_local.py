# -*- coding: utf-8 -*-

"""
author: Brian Shin
email: brianhesungshin@gmail.com
"""

"""
Builds staging tables for all CODWARZONE tables:
(profile_staging, game_details_staging, game_stats_staging).
script args can be passed through command line ex:
python warzone_staging_local.py

This staging part should drop tables, so I commented out
the drop_tables argument.

"""
import os
from datetime import datetime
import sqlite3
import pandas as pd
import numpy as np
import datetime as dt
import argparse


today = dt.datetime.now().strftime("%Y-%m-%d")
os.chdir('/Users/brianshin/brian/tinker/brian_dwh/codwarzone/output')

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
        cur.execute('DROP TABLE IF EXISTS {}'.format('profile_staging'))
        cur.execute('DROP TABLE IF EXISTS {}'.format('game_details_staging'))
        cur.execute('DROP TABLE IF EXISTS {}'.format('game_stats_staging'))

        print('dropping all tables.')
    else:
        print('did not drop all tables.')

  except Error as e:
    print(e)

  return conn


def profile_staging(conn):

    cur = conn.cursor()

    profile_staging_sql = """
        CREATE TABLE IF NOT EXISTS profile_staging AS
            SELECT
                CAST(profile_id AS VARCHAR) AS profile_id,
                DATE(profile_date) AS profile_date,
                CAST(gamer_id AS VARCHAR) AS gamer_id,
                CAST(playtime AS VARCHAR) AS playtime,
                CAST(REPLACE(SUBSTR(games, 1, INSTR(games, ' ') - 1), ',', '') AS INT) AS games,
                CAST(SUBSTR(level, INSTR(level, ' ') + 1) AS INT) AS level,
                CAST(level_rank AS VARCHAR) AS level_rank,
                CAST(REPLACE(wins, ',', '') AS INT) AS wins,
                CAST(wins_rank AS VARCHAR) AS wins_rank,
                CAST(kd AS FLOAT) as kd,
                CAST(kd_rank AS FLOAT) AS kd_rank,
                CAST(REPLACE(dmg_per_game, ',', '') AS FLOAT) AS dmg_per_game,
                CAST(REPLACE(top5, ',', '') AS INT) AS top5,
                CAST(top5_rank AS VARCHAR) AS top5_rank,
                CAST(REPLACE(top10, ',', '') AS INT) AS top10,
                CAST(top10_rank AS VARCHAR) AS top10_rank,
                CAST(REPLACE(top25, ',', '') AS INT) AS top25,
                CAST(top25_rank AS VARCHAR) AS top25_rank,
                CAST(REPLACE(kills, ',', '') AS INT) AS kills,
                CAST(REPLACE(deaths, ',', '') AS INT) AS deaths,
                CAST(REPLACE(downs, ',', '') AS INT) AS downs,
                CAST(avg_life AS VARCHAR) AS avg_life,
                CAST(avg_life_rank AS VARCHAR) AS avg_life_rank,
                CAST(REPLACE(score, ',', '') AS INT) AS score,
                CAST(REPLACE(score_per_min, ',', '') AS FLOAT) AS score_per_min,
                CAST(score_per_min_rank AS VARCHAR) AS score_per_min_rank,
                CAST(REPLACE(score_per_game, ',', '') AS FLOAT) AS score_per_game,
                CAST(REPLACE(cash, ',', '') AS INT) AS cash,
                CAST(REPLACE(contracts, ',', '') AS INT) AS contracts,
                CAST(contracts_rank AS VARCHAR) AS contracts_rank,
                CAST(REPLACE(win_perc, ',', '') AS FLOAT) / 100 AS win_perc,
                CAST(win_perc_rank AS VARCHAR) AS win_perc_rank
            FROM profile_rawdata
        ;
        """

    cur.execute(profile_staging_sql)
    conn.commit()
    print('inserted values into profile_staging')


def game_details_staging(conn):

    cur = conn.cursor()

    game_details_staging_sql = """
        CREATE TABLE IF NOT EXISTS game_details_staging AS
            SELECT
            	CAST(game_id AS VARCHAR) AS game_id,
            	DATE(game_date) AS game_date,
            	CASE
            		WHEN INSTR(game_time, 'AM') > 0 AND INSTR(game_time, '12:') > 0
            			THEN TIME(REPLACE(REPLACE(game_time, ' AM', ''), '12:', '00:'))
            		WHEN INSTR(game_time, 'AM') > 0
            			THEN TIME(REPLACE(game_time, ' AM', ''))
            		ELSE TIME(REPLACE(game_time, ' PM', ''), '+12 hours')
            	END game_time,
            	CAST(gamer_id AS VARCHAR) AS gamer_id,
            	CAST(game_type AS VARCHAR) AS game_type,
            	CAST(placement AS INT) AS placement,
            	CAST(kills AS INT) AS kills,
            	CAST(cache_open AS INT) AS cache_open,
                CAST(REPLACE(damage, ',','') AS INT) AS damage,
                CAST(REPLACE(damage_per_min, ',','') AS FLOAT) AS damage_per_min
            FROM game_details_rawdata
            WHERE game_type != 'Warzone Rumble'
        ;
        """

    cur.execute(game_details_staging_sql)
    conn.commit()
    print('inserted values into game_details_staging')


def game_stats_staging(conn):

    cur = conn.cursor()

    game_stats_staging_sql = """
        CREATE TABLE IF NOT EXISTS game_stats_staging AS
            SELECT
                CAST(game_id AS VARCHAR) AS game_id,
                CAST(gamer_id AS VARCHAR) AS gamer_id,
                DATE(game_date) AS game_date,
                CAST(game_time AS VARCHAR) AS game_time,
                CAST(kills AS INT) AS kills,
                CAST(deaths AS INT) AS deaths,
                CAST(assists AS INT) AS assists,
                CAST(kd AS FLOAT) AS kd,
                CAST(REPLACE(damage, ',','') AS INT) AS damage,
                CAST(REPLACE(score, ',','') AS INT) AS score,
                CAST(REPLACE(score_per_min, ',','') AS FLOAT) AS score_per_min,
                CAST(wall_bangs AS INT) AS wall_bangs,
                CAST(headshots AS INT) AS headshots,
                CAST(reviver AS INT) AS reviver,
                CAST(team_placement AS INT) AS team_placement,
                CAST(time_played AS VARCHAR) AS time_played,
                CAST(REPLACE(total_xp, ',','') AS INT) AS total_xp,
                CAST(REPLACE(score_xp, ',','') AS INT) AS score_xp,
                CAST(REPLACE(match_xp, ',','') AS INT) AS match_xp,
                CAST(REPLACE(challenge_xp, ',','') AS INT) AS challenge_xp,
                CAST(REPLACE(medal_xp, ',','') AS INT) AS medal_xp,
                CAST(REPLACE(bonus_xp, ',','') AS INT) AS bonus_xp,
                CAST(REPLACE(misc_xp, ',','') AS INT) AS misc_xp,
                CAST(gulag_kills AS INT) AS gulag_kills,
                CAST(gulag_deaths AS INT) AS gulag_deaths,
                CAST(distance_traveled AS INT) AS distance_traveled,
                CAST(REPLACE(percent_time_moving, ',', '') AS FLOAT) / 100 AS percent_time_moving,
                CAST(team_survival AS VARCHAR) AS team_survival,
                CAST(team_wiped AS INT) AS team_wiped,
                CAST(executions AS INT) AS executions,
                CAST(nearmisses AS INT) AS nearmisses,
                CAST(kiosk_buy AS INT) AS kiosk_buy,
                CAST(REPLACE(damage_taken, ',','') AS INT) AS damage_taken,
                CAST(mission_pickup_tablet AS INT) AS mission_pickup_tablet,
                CAST(last_stand_kill  AS INT) AS last_stand_kill,
                CAST(longest_streak AS INT) AS longest_streak,
                CAST(cache_open AS INT) AS cache_open
            FROM game_stats_rawdata
        ;
        """

    cur.execute(game_stats_staging_sql)
    conn.commit()
    print('inserted values into game_stats_staging')


def drop_tables_arg():

    parser = argparse.ArgumentParser(description='this python script will run CODWARZONE.STAGING tables.')
    parser.add_argument('--drop_tables', default='N', help='If Y, this drops all of the tables in CODWARZONE.STAGING and then kicks off a run of the most recent load. If N, it appends.')

    args=parser.parse_args()

    drop_tables = args.drop_tables.upper()

    return drop_tables


def create_staging_tables():

    profile_staging(conn)
    game_details_staging(conn)
    game_stats_staging(conn)
    print('updated CODWARZONE.STAGING tables. itspizzatime.jpeg')


if __name__ == '__main__':

    # drop_tables = drop_tables_arg()
    # drop_tables = 'Y'

    # if drop_tables == 'Y':
    #     print('dropping tables.')
    #     drop_check = input('Are you sure you want to drop all the tables in CODWARZONE.STAGING (y/n)? ')
    #     if drop_check.lower() != 'y':
    #         print('peace- cancelling script.')
    #         sys.exit()
    #     else:
    #         print('dropping tables in CODWARZONE.STAGING')
    #         conn = create_connection(drop=True)
    # else:
    #     conn = create_connection(drop=False)

    conn = create_connection(drop=True)
    create_staging_tables()
