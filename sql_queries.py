# SERIAL is not a true data type, but is simply shorthand notation that tells Postgres to create a auto incremented,
# unique identifier for the specified column.

# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
create_song_plays_table = """create table if not exists songplays  (songplay_id SERIAL PRIMARY KEY,start_time varchar ,user_id int ,
level varchar,song_id varchar ,artist_id varchar ,session_id int ,location varchar,user_agent varchar)"""

# user_id, first_name, last_name, gender, level
create_users_table = """ create table if not exists users (user_id int PRIMARY KEY, first_name varchar 
,last_name varchar,gender varchar,level varchar)"""

# song_id, title, artist_id, year, duration
create_songs_table = """create table if not exists songs (song_id varchar PRIMARY KEY,title varchar ,artist_id 
varchar,year int ,duration varchar ) """

# artist_id, name, location, latitude, longitude

create_artists_table = """create table if not exists artists (artist_id varchar PRIMARY KEY,name varchar 
,location varchar,latitude varchar ,longitude varchar ) """

# start_time, hour, day, week, month, year, weekday
create_time_table = """create table if not exists time (start_time varchar PRIMARY KEY, hour varchar 
,day varchar ,week varchar ,month varchar,year varchar,weekday varchar)"""

drop_song_plays_table = "drop table songplays"
drop_users_table = "drop table users"
drop_songs_table = "drop table songs"
drop_artists_table = "drop table artists"

insert_songs_plays_table = """insert into songplays (start_time ,user_id ,
level ,song_id  ,artist_id  ,session_id  ,location ,user_agent )values(%s,%s,%s,%s,
%s,%s,%s,%s)"""

insert_users_table = """insert into users (user_id,first_name,last_name,gender,level) values(%s,%s,%s,%s,
%s) on conflict(user_id) do nothing """

insert_songs_table = """insert into songs (song_id,artist_id,title,year,duration) values (%s,%s,%s,%s,
%s)  ON CONFLICT (song_id) DO NOTHING """

insert_artists_table = """insert into artists (artist_id,name,location,latitude,longitude) values (%s,%s,%s,%s,
%s) on conflict (artist_id) do nothing """

insert_time_table = """insert into time (start_time,hour,day,week,month,year,weekday) values (%s,%s,%s,%s,%s,%s,
%s) on conflict(start_time) do nothing """

song_select = """select s.song_id, a.artist_id from songs as s 
                    JOIN artists as a on s.artist_id= a.artist_id
                    where s.title = (%s) 
                    and s.duration = (%s) 
                    and a.name=(%s) """