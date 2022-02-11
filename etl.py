import glob
import os
import pandas as pd
import psycopg2
from psycopg2 import Error

from sql_queries import *


def get_files(path):
    files_path = []
    # traverse all folder and its subfolder from top to bottom
    for root, dirs, files in os.walk(path):
        # creating a pattern so we only get the path that matches that pattern
        json_pattern = os.path.join(root, '*.json')
        file_list = glob.glob(json_pattern)
        for file in file_list:
            files_path.append(os.path.abspath(file))
    return files_path


def extract_load_logs_data(cur):
    # example file for testing
    log_json_files_paths = get_files("data/log_data")
    for path in log_json_files_paths:
        df = pd.read_json(path, lines=True)
        for index, row in df.iterrows():
            insert_users_data(cur, row)
            insert_time_data(cur, row)


def insert_time_data(cur, row):
    start_time = row.ts
    hour = pd.to_datetime(start_time, unit='ms').hour
    day = pd.to_datetime(start_time, unit='ms').day
    week = pd.to_datetime(start_time, unit='ms').week
    month = pd.to_datetime(start_time, unit='ms').month
    year = pd.to_datetime(start_time, unit='ms').year
    weekday = pd.to_datetime(start_time, unit='ms').dayofweek
    time_data = {'start_time': start_time, 'hour': hour, 'day': day, 'week': week, 'month': month,
                 'year': year, 'weekday': weekday}
    try:
        cur.execute(insert_time_table, (
            time_data["start_time"], time_data["hour"], time_data["day"], time_data["week"], time_data["month"],
            time_data["year"], time_data["weekday"]))
    except (Exception, Error) as error:
        print("Error while inserting into PostgreSQL", error)


def insert_users_data(cur, row):
    try:
        value = row[["userId", "firstName", "lastName", "gender", "level"]]
        if value.userId:
            cur.execute(insert_users_table, value)
    except (Exception, Error) as error:
        print("Error while inserting users into PostgreSQL", error)


def extract_load_songs_data(cur):
    # example file for testing
    song_json_files_paths = get_files("data/song_data")
    for path in song_json_files_paths:
        df = pd.read_json(path, lines=True)
        for index, row in df.iterrows():
            insert_song_data(cur, row)
            insert_artist_data(cur, row)


def insert_artist_data(cur, row):
    try:
        value = row[
            ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]]
        cur.execute(insert_artists_table, value)
    except (Exception, Error) as error:
        print("Error while inserting artist_data into PostgreSQL", error)


def insert_song_data(cur, row):
    try:
        value = row[["song_id", "artist_id", "title", "year", "duration"]]
        cur.execute(insert_songs_table, value)
    except (Exception, Error) as error:
        print("Error while inserting song_data into PostgreSQL", error)


def main():
    # connect to sparkify database
    conn = psycopg2.connect(user="postgres",
                            password="constantine",
                            host="127.0.0.1",
                            port="5432",
                            database="sparkify")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    extract_load_songs_data(cur)
    extract_load_logs_data(cur)
    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
