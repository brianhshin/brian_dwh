# -*- coding: utf-8 -*-

"""
author: Brian Shin
email: brianhesungshin@gmail.com
"""

"""
Builds prod tables for all CODWARZONE tables:
(profile_dim, game_details_dim, game_stats_dim).
script args can be passed through command line ex:
python warzone_prod_local.py --drop_tables Y
"""
import os
from datetime import datetime
import sqlite3
import pandas as pd
import numpy as np
import datetime as dt
import argparse


today = dt.datetime.now().strftime("%Y-%m-%d")
os.chdir('/Users/brianshin/brian/tinker/brian_dwh/codwarzone/output/current')

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
        cur.execute('DROP TABLE IF EXISTS {}'.format('profile_dim'))
        cur.execute('DROP TABLE IF EXISTS {}'.format('game_details_dim'))
        cur.execute('DROP TABLE IF EXISTS {}'.format('game_stats_dim'))

        print('dropping all tables.')
    else:
        print('did not drop all tables.')

  except Error as e:
    print(e)

  return conn


def profile_dim(conn):

    cur = conn.cursor()

    profile_create_sql = """
        CREATE TABLE IF NOT EXISTS profile_dim (
        	profile_id TEXT PRIMARY KEY,
        	profile_date DATE NOT NULL,
        	gamer_id TEXT NOT NULL,
        	playtime TEXT NOT NULL,
        	games INT NOT NULL,
        	level INT NOT NULL,
        	level_rank TEXT NOT NULL,
        	wins INT NOT NULL,
        	wins_rank TEXT NOT NULL,
        	kd FLOAT NOT NULL,
        	kd_rank TEXT NOT NULL,
        	dmg_per_game INT NOT NULL,
        	top5 INT NOT NULL,
        	top5_rank TEXT NOT NULL,
        	top10 INT NOT NULL,
        	top10_rank TEXT NOT NULL,
        	top25 INT NOT NULL,
        	top25_rank TEXT NOT NULL,
        	kills INT NOT NULL,
        	deaths INT NOT NULL,
        	downs INT NOT NULL,
        	avg_life TEXT NOT NULL,
        	avg_life_rank TEXT NOT NULL,
        	score INT NOT NULL,
        	score_per_min FLOAT NOT NULL,
        	score_per_min_rank TEXT NOT NULL,
        	score_per_game FLOAT NOT NULL,
        	cash INT NOT NULL,
        	contracts INT NOT NULL,
        	contracts_rank TEXT NOT NULL,
        	win_perc FLOAT NOT NULL,
        	win_perc_rank TEXT NOT NULL,
        	last_modified_timestamp TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP,
        	created_timestamp TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP
        );
        """

    profile_prod_sql = """
        INSERT INTO profile_dim
        	SELECT
        		profile_id,
        		profile_date,
        		gamer_id,
        		playtime,
        		games,
        		level,
        		level_rank,
        		wins,
        		wins_rank,
        		kd,
        		kd_rank,
        		dmg_per_game,
        		top5,
        		top5_rank,
        		top10,
        		top10_rank,
        		top25,
        		top25_rank,
        		kills,
        		deaths,
        		downs,
        		avg_life,
        		avg_life_rank,
        		score,
        		score_per_min,
        		score_per_min_rank,
        		score_per_game,
        		cash,
        		contracts,
        		contracts_rank,
        		win_perc,
        		win_perc_rank,
        		CURRENT_TIMESTAMP,
        		CURRENT_TIMESTAMP
        	FROM profile_staging
        	WHERE TRUE
        	ORDER BY profile_date ASC, gamer_id ASC
        ON CONFLICT(profile_id) DO UPDATE SET
        	profile_date = excluded.profile_date,
        	gamer_id = excluded.gamer_id,
        	playtime = excluded.playtime,
        	games = excluded.games,
        	level = excluded.level,
        	level_rank = excluded.level_rank,
        	wins = excluded.wins,
        	wins_rank = excluded.wins_rank,
        	kd = excluded.kd,
        	kd_rank = excluded.kd_rank,
        	dmg_per_game = excluded.dmg_per_game,
        	top5 = excluded.top5,
        	top5_rank = excluded.top5_rank,
        	top10 = excluded.top10,
        	top10_rank = excluded.top10_rank,
        	top25 = excluded.top25,
        	top25_rank = excluded.top25_rank,
        	kills = excluded.kills,
        	deaths = excluded.deaths,
        	downs = excluded.downs,
        	avg_life = excluded.avg_life,
        	avg_life_rank = excluded.avg_life_rank,
        	score = excluded.score,
        	score_per_min = excluded.score_per_min,
        	score_per_min_rank = excluded.score_per_min_rank,
        	score_per_game = excluded.score_per_game,
        	cash = excluded.cash,
        	contracts = excluded.contracts,
        	contracts_rank = excluded.contracts_rank,
        	win_perc = excluded.win_perc,
        	win_perc_rank = excluded.win_perc_rank,
        	last_modified_timestamp = CURRENT_TIMESTAMP
        ;
        """
    cur.execute(profile_create_sql)
    cur.execute(profile_prod_sql)
    conn.commit()
    print('inserted values into profile_staging')


def game_details_dim(conn):

    cur = conn.cursor()

    game_details_create_sql = """
        CREATE TABLE IF NOT EXISTS game_details_dim (
        	game_details_id VARCHAR PRIMARY KEY,
            game_id VARCHAR NOT NULL,
        	game_date DATE NOT NULL,
        	game_time TIME NOT NULL,
        	gamer_id TEXT VARCHAR NULL,
        	game_type TEXT NOT NULL,
        	placement INT NOT NULL,
        	kills INT NOT NULL,
        	cache_open INT NOT NULL,
        	damage INT NOT NULL,
        	damage_per_min FLOAT NOT NULL,
        	last_modified_timestamp TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP,
        	created_timestamp TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP
        );
        """

    game_details_prod_sql = """
        INSERT INTO game_details_dim
        	SELECT
                game_id || '_' || replace(gamer_id, '#', '_'),
            	game_id,
            	game_date,
            	game_time,
            	gamer_id,
            	game_type,
            	placement,
            	kills,
            	cache_open,
            	damage,
            	damage_per_min,
        		CURRENT_TIMESTAMP,
        		CURRENT_TIMESTAMP
        	FROM game_details_staging
        	WHERE TRUE
        	ORDER BY game_date ASC, game_time ASC
        ON CONFLICT(game_details_id) DO UPDATE SET
            game_id = excluded.game_id,
        	game_date = excluded.game_date,
        	game_time = excluded.game_time,
        	gamer_id = excluded.gamer_id,
        	game_type = excluded.game_type,
        	placement = excluded.placement,
        	kills = excluded.kills,
        	cache_open = excluded.cache_open,
        	damage = excluded.damage,
        	damage_per_min = excluded.damage_per_min,
        	last_modified_timestamp = CURRENT_TIMESTAMP
        ;
        """

    cur.execute(game_details_create_sql)
    cur.execute(game_details_prod_sql)
    conn.commit()
    print('inserted values into game_details_dim')


def game_stats_dim(conn):

    cur = conn.cursor()

    game_stats_create_sql = """
        CREATE TABLE IF NOT EXISTS game_stats_dim (
        	game_stats_id VARCHAR PRIMARY KEY,
            game_id VARCHAR NOT NULL,
        	gamer_id VARCHAR NOT NULL,
        	game_date DATE NOT NULL,
        	game_time TIME NOT NULL,
        	kills INT NOT NULL,
        	deaths INT NOT NULL,
        	assists INT NOT NULL,
        	kd FLOAT NOT NULL,
        	damage INT NOT NULL,
        	score INT NOT NULL,
        	score_per_min FLOAT NOT NULL,
        	wall_bangs INT NOT NULL,
        	headshots INT NOT NULL,
        	reviver INT NOT NULL,
        	team_placement INT NOT NULL,
        	team_wiped INT NOT NULL,
        	time_played VARCHAR NOT NULL,
        	total_xp INT NOT NULL,
        	score_xp INT NOT NULL,
        	match_xp INT NOT NULL,
        	challenge_xp INT NOT NULL,
        	medal_xp INT NOT NULL,
        	bonus_xp INT NOT NULL,
        	misc_xp INT NOT NULL,
        	gulag_kills INT NOT NULL,
        	gulag_deaths INT NOT NULL,
        	distance_traveled INT NOT NULL,
        	percent_time_moving FLOAT NOT NULL,
        	team_survival VARCHAR NOT NULL,
        	executions INT NOT NULL,
        	nearmisses INT NOT NULL,
        	kiosk_buy INT NOT NULL,
        	damage_taken INT NOT NULL,
        	mission_pickup_tablet INT NOT NULL,
        	last_stand_kill INT NOT NULL,
        	longest_streak INT NOT NULL,
        	cache_open INT NOT NULL,
        	last_modified_timestamp TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP,
        	created_timestamp TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP
        );
        """

    game_stats_prod_sql = """
        INSERT INTO game_stats_dim
        	SELECT
                stats.game_id || '_' || replace(stats.gamer_id, '#', '_'),
            	stats.game_id,
            	stats.gamer_id,
            	details.game_date,
            	details.game_time,
            	stats.kills,
            	stats.deaths,
            	stats.assists,
            	stats.kd,
            	stats.damage,
            	stats.score,
            	stats.score_per_min,
            	stats.wall_bangs,
            	stats.headshots,
            	stats.reviver,
            	stats.team_placement,
            	stats.team_wiped,
            	stats.time_played,
            	stats.total_xp,
            	stats.score_xp,
            	stats.match_xp,
            	stats.challenge_xp,
            	stats.medal_xp,
            	stats.bonus_xp,
            	stats.misc_xp,
            	stats.gulag_kills,
            	stats.gulag_deaths,
            	stats.distance_traveled,
            	stats.percent_time_moving,
            	stats.team_survival,
            	stats.executions,
            	stats.nearmisses,
            	stats.kiosk_buy,
            	stats.damage_taken,
            	stats.mission_pickup_tablet,
            	stats.last_stand_kill,
            	stats.longest_streak,
            	stats.cache_open,
        		CURRENT_TIMESTAMP,
        		CURRENT_TIMESTAMP
        	FROM game_stats_staging stats
        	INNER JOIN game_details_staging details ON stats.game_id = details.game_id
        	WHERE TRUE
        	ORDER BY details.game_date ASC, details.game_time ASC
        ON CONFLICT(game_stats_id) DO UPDATE SET
            game_id = excluded.game_id,
        	gamer_id = excluded.gamer_id,
        	game_date = excluded.game_date,
        	game_time = excluded.game_time,
        	kills = excluded.kills,
        	deaths = excluded.deaths,
        	assists = excluded.assists,
        	kd = excluded.kd,
        	damage = excluded.damage,
        	score = excluded.score,
        	score_per_min = excluded.score_per_min,
        	wall_bangs = excluded.wall_bangs,
        	headshots = excluded.headshots,
        	reviver = excluded.reviver,
        	team_placement = excluded.team_placement,
        	team_wiped = excluded.team_wiped,
        	time_played = excluded.time_played,
        	total_xp = excluded.total_xp,
        	score_xp = excluded.score_xp,
        	match_xp = excluded.match_xp,
        	challenge_xp = excluded.challenge_xp,
        	medal_xp = excluded.medal_xp,
        	bonus_xp = excluded.bonus_xp,
        	misc_xp = excluded.misc_xp,
        	gulag_kills = excluded.gulag_kills,
        	gulag_deaths = excluded.gulag_deaths,
        	distance_traveled = excluded.distance_traveled,
        	percent_time_moving = excluded.percent_time_moving,
        	team_survival = excluded.team_survival,
        	executions = excluded.executions,
        	nearmisses = excluded.nearmisses,
        	kiosk_buy = excluded.kiosk_buy,
        	damage_taken = excluded.damage_taken,
        	mission_pickup_tablet = excluded.mission_pickup_tablet,
        	last_stand_kill = excluded.last_stand_kill,
        	longest_streak = excluded.longest_streak,
        	cache_open = excluded.cache_open,
            last_modified_timestamp = CURRENT_TIMESTAMP
        ;
        """

    cur.execute(game_stats_create_sql)
    cur.execute(game_stats_prod_sql)
    conn.commit()
    print('inserted values into game_stats_dim')


def drop_tables_arg():

    parser = argparse.ArgumentParser(description='this python script will run CODWARZONE.PROD tables.')
    parser.add_argument('--drop_tables', default='N', help='If Y, this drops all of the tables in CODWARZONE.PROD and then kicks off a run of the most recent load. If N, it appends.')

    args=parser.parse_args()

    drop_tables = args.drop_tables.upper()

    return drop_tables


def create_staging_tables():

    profile_dim(conn)
    game_details_dim(conn)
    game_stats_dim(conn)
    print('updated CODWARZONE.PROD tables. itspizzatime.jpeg')


if __name__ == '__main__':

    drop_tables = drop_tables_arg()

    if drop_tables == 'Y':
        print('dropping tables.')
        drop_check = input('Are you sure you want to drop all the tables in CODWARZONE.PROD (y/n)? ')
        if drop_check.lower() != 'y':
            print('peace- cancelling script.')
            sys.exit()
        else:
            print('dropping tables in CODWARZONE.PROD.')
            conn = create_connection(drop=True)
    else:
        conn = create_connection(drop=False)

    create_staging_tables()
