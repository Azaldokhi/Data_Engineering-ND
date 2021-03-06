# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

#Create Fact Table
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
	songplay_id serial NOT NULL,
	start_time timestamp NOT NULL,
	user_id int NOT NULL,
	level varchar ,
	song_id varchar ,
	artist_id varchar ,
	session_id int,
	location varchar ,
	user_agent varchar,
    CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id)
);
""")
#Create Dim Tables
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
    user_id varchar ,
	first_name varchar,
	last_name varchar ,
	gender varchar ,
	level varchar ,
    CONSTRAINT users_pkey PRIMARY KEY (user_id)
);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
    song_id varchar, 
    title varchar , 
    artist_id varchar, 
    year int, 
    duration numeric ,
    CONSTRAINT songs_pkey PRIMARY KEY (song_id)
);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar ,
	name varchar(100) ,
	location varchar(100) ,
	lattitude float4 ,
	longitude float4 ,
    CONSTRAINT artists_pkey PRIMARY KEY (artist_id)
);
""")

time_table_create = ("""
CREATE TABLE time(
    start_time timestamp,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER,
    PRIMARY KEY (start_time)
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays
    (start_time, user_id, level,song_id, artist_id, session_id, location, user_agent)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (songplay_id) DO NOTHING;
""")

user_table_insert = ("""
    INSERT INTO users
    (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
     ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level;
""")

song_table_insert = ("""
    INSERT INTO songs 
    (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artists
    (artist_id, name, location, lattitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO NOTHING;
""")


time_table_insert = ("""
        INSERT INTO time 
        (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS

song_select = ("""
select s2.song_id, a2.artist_id
from songs s2
join artists a2 on s2.artist_id = a2.artist_id
where s2.title = %s AND a2.name = %s AND s2.duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create,time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop,time_table_drop]