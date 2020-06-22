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


def test_profile_s3_download():
    profile_s3 = download_file_s3(
        'leagueofgraphs',
        'profile/profile_20200513.csv',
        '/home/ubuntu/ubuntu/brian_dwh/league/league_temp_output',
        'csv')
    print(profile_s3.shape)
    print(profile_s3.head())






#
#
# def create_connection(db):
#   """
#     Create a database connection to the SQLite database specified by db_file.
#     :param db_file: database file
#     :return: Connection object or None
#   """
#   conn = None
#   try:
#     # connect to db specified in run_etl_sim
#     conn = sqlite3.connect(db)
#     cur = conn.cursor()
#     # drop tables for each ETL run
#     cur.execute('DROP TABLE IF EXISTS {}'.format('profile_staging'))
#   except Error as e:
#     print(e)
#
#   return conn
#
# db = 'profile_staging.db'
# conn = create_connection(db)
#
#
# def create_table(deduped_updates, conn):
#   """
#     Upsert deduped_updates into users table without fetching rows into python for manipulation (aka use SQL).
#     Creates the table, inserts new rows, and updates existing rows with more recent data.
#   """
#
#   cur = conn.cursor()
#   # create table joes
#   cur.execute("""
#     CREATE TABLE IF NOT EXISTS profile_staging (
#     	date TEXT PRIMARY KEY,
#     	tier TEXT NOT NULL,
#     	queue TEXT NOT NULL,
#         rank INTEGER NOT NULL,
#     	region_rank TEXT NOT NULL,
#     	lp INTEGER NOT NULL,
#     	wins INTEGER NOT NULL,
#     	losses INTEGER NOT NULL,
#     	games INTEGER NOT NULL,
#     	winrate TEXT NOT NULL,
#     	avg_kills INTEGER NOT NULL,
#     	avg_deaths INTEGER NOT NULL,
#     	avg_assists INTEGER NOT NULL,
#     	tags TEXT NOT NULL,
#     );
#     """)
#
#     insert_sql = """
#       INSERT OR IGNORE INTO profile_staging({})
#       VALUES({})
#       ;
#       """.format(columns,values)
#       cur = conn.cursor()
#       cur.execute(insert_sql, row_values)
#       conn.commit()
#
#       # if id exists but updated_at is more recent than what exists in the table, update row for that id
#       update_sql = """
#       UPDATE joes SET name = '{}', cookies = {}, updated_at = {} WHERE (id = {} AND updated_at < {})
#       ;
#       """.format(row_values[1],row_values[2],row_values[3],row_values[0],row_values[3])
#       cur = conn.cursor()
#       cur.execute(update_sql)
#       conn.commit()
#
#
#
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
    test_profile_s3_download()
