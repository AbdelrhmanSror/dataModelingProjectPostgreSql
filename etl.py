import glob
import os
import pandas as pd
import psycopg2
from psycopg2 import Error
import psycopg2.extras

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
        df = pd.read_json(path, lines=True).query("page == 'NextSong'")
        print(len(df))
        time_data = [time_data_mapping(row) for index, row in df.iterrows()]
        # [x for x in my_list if x.attribute == value]
        user_data = [row[["userId", "firstName", "lastName", "gender", "level"]] for index, row in df.iterrows()
                     if row.userId]
        song_plays_data = []
        for index, row in df.iterrows():
            # fetching the matched song_id and artist_id from both song and artist tables
            song_id, artist_id = get_song_artist_id(cur, row)
            song_plays_data.append(pd.Series(
                {"time": row.ts, "userId": row.userId, "level": row.level, "songId": song_id,
                 "artistId": artist_id,
                 "sessionId": row.sessionId, "location": row.location, "userAgent": row.userAgent},
                index=["time", "userId", "level", "songId", "artistId", "sessionId", "location", "userAgent"]))
        # bulk insert into time table
        psycopg2.extras.execute_batch(cur, insert_time_table, time_data)
        # bulk insert into users table
        psycopg2.extras.execute_batch(cur, insert_users_table, user_data)
        # bulk insert into song_plays table
        psycopg2.extras.execute_batch(cur, insert_songs_plays_table, song_plays_data)


def get_song_artist_id(cur, row):
    # fetching the matched song_id and artist_id from both song and artist tables
    cur.execute(song_select, (row.song, row.artist, str(row.length)))
    results = cur.fetchone()
    song_id, artist_id = None, None
    if results:
        song_id, artist_id  = results
    return song_id, artist_id


def time_data_mapping(row):
    start_time = row.ts
    hour = pd.to_datetime(start_time, unit='ms').hour
    day = pd.to_datetime(start_time, unit='ms').day
    week = pd.to_datetime(start_time, unit='ms').week
    month = pd.to_datetime(start_time, unit='ms').month
    year = pd.to_datetime(start_time, unit='ms').year
    weekday = pd.to_datetime(start_time, unit='ms').dayofweek
    return start_time, hour, day, week, month, year, weekday


def extract_load_songs_data(cur):
    # example file for testing
    song_json_files_paths = get_files("data/song_data")
    try:
        # reading all json files then map it to database model
        songs_data = [pd.read_json(path, lines=True)[["song_id", "artist_id", "title", "year", "duration"]].values[0]
                      for path in song_json_files_paths]
        artists_data = [pd.read_json(path, lines=True)[
                            ["artist_id", "artist_name", "artist_location", "artist_latitude",
                             "artist_longitude"]].values[0] for path in song_json_files_paths]
        # bulk insert
        psycopg2.extras.execute_batch(cur, insert_songs_table, songs_data)
        psycopg2.extras.execute_batch(cur, insert_artists_table, artists_data)
    # cur.execute(insert_songs_table,value)
    except (Exception, Error) as error:
        print("Error while inserting into PostgreSQL", error)


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
