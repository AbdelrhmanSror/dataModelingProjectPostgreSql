from sql_queries import *
import psycopg2
from psycopg2 import Error


def main():
    cursor, conn = init_database()
    createTables(cursor, conn)


def init_database():
    try:
        # connect to default database
        conn = psycopg2.connect(user="postgres",
                                password="constantine",
                                host="127.0.0.1",
                                port="5432",
                                database="postgres")
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        # create sparkify database
        cur.execute("DROP DATABASE IF EXISTS sparkify")
        cur.execute("CREATE DATABASE sparkify")
        # close connection to default database
        conn.close()
        # connect to sparkify database
        conn = psycopg2.connect(user="postgres",
                                password="constantine",
                                host="127.0.0.1",
                                port="5432",
                                database="sparkify")
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        return cur, conn
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)


def createTables(cursor, conn):
    try:
        # creating tables
        cursor.execute(create_song_plays_table)
        cursor.execute(create_songs_table)
        cursor.execute(create_artists_table)
        cursor.execute(create_users_table)
        cursor.execute(create_time_table)
        # closing connection
        close(cursor, conn)
        print("successfully creating table")
    except (Exception, Error) as error:
        close(cursor, conn)
        print("Error while creating table songplays", error)


def close(cursor, connection):
    # closing connection
    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()
