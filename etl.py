import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Extracting data from JSON file from data/song_data and executing INSERT queries to append data to 
    songs and artists table in sparkifydb database

    :param cur: The database's cursor
    :param filepath: JSON file path
    :return: None
    """
    # open song file
    df = pd.read_json(filepath, typ='series')

    # insert song record
    song_data = (df["song_id"], df["title"], df["artist_id"], df["year"], df["duration"])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = (df["artist_id"], df["artist_name"], df["artist_location"],
                   df["artist_latitude"], df["artist_longitude"])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Extracting data from JSON file from data/log_data and executing INSERT queries to append data to time,
    users, songplays table in sparkifydb database

    **NOTE: In order to find data to feed into artist_id and song_id columns in songplays table, the queries execution
    on songs and artists tables to extract data.

    :param cur: The database's cursor
    :param filepath: JSON file path
    :return: None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = list(zip(list(pd.DatetimeIndex(t.values)),
                         list(pd.DatetimeIndex(t.values).hour),
                         list(pd.DatetimeIndex(t.values).day),
                         list(pd.DatetimeIndex(t.values).week),
                         list(pd.DatetimeIndex(t.values).month),
                         list(pd.DatetimeIndex(t.values).year),
                         list(pd.DatetimeIndex(t.values).weekday)))
    column_labels = ["start_time", "hour", "day", "week", "month", "year", "weekday"]

    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].copy()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.length, row.artist))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid,
                         artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Finding all JSON files and applying "process_song_file" and "process_log_file" function on every JSON files found

    :param cur: The database's cursor
    :param conn: The database's connection
    :param filepath: The path lead to JSON files. In this cases, it will be "data/log_data" and "data/song_data"
    :param func: Function to extracting, transforming, and loading data to sparkifydb tables
    :return: None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    conn.close()


if __name__ == "__main__":
    main()
