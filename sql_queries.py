import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop       = "DROP TABLE IF EXISTS songplay_table"
user_table_drop           = "DROP TABLE IF EXISTS user_table"
song_table_drop           = "DROP TABLE IF EXISTS song_table"
artist_table_drop         = "DROP TABLE IF EXISTS artist_table"
time_table_drop           = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
                               CREATE TABLE IF NOT EXISTS staging_events_table (
                                   event_id INT IDENTITY(0,1) NOT NULL SORTKEY DISTKEY,
                                   artist VARCHAR,
                                   auth VARCHAR,
                                   firstname VARCHAR,
                                   gender VARCHAR,
                                   itemInSession INTEGER,
                                   lastname VARCHAR,
                                   length FLOAT,
                                   level VARCHAR,
                                   location VARCHAR,
                                   method VARCHAR,
                                   page VARCHAR,
                                   registration BIGINT,
                                   sessionId INTEGER,
                                   song VARCHAR,
                                   status INTEGER,
                                   ts BIGINT,
                                   userAgent VARCHAR,
                                   userId INTEGER);
                               """) 


staging_songs_table_create = ("""
                              CREATE TABLE IF NOT EXISTS staging_songs_table (
                               num_songs int not null,
                                artist_id char (18) not null,
                                artist_latitude varchar,
                                artist_longitude varchar,
                                artist_location varchar,
                                artist_name varchar not null,
                                song_id char (18) not null,
                                title varchar not null,
                                duration numeric not null,
                                year int not null
                            )
                        """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay_table (
                            songplay_id INT        NOT NULL PRIMARY KEY sortkey,
                            start_time TIMESTAMP   NOT NULL,
                            user_id    INT         NOT NULL,
                            level      VARCHAR(22) NOT NULL,
                            song_id    INT         NOT NULL,
                            artist_id  CHAR         NOT NULL,
                            session_id INT         NOT NULL,
                            location   VARCHAR(22) NOT NULL,
                            user_agent VARCHAR(22) NOT NULL
                            )

""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS user_table (
                        user_id    INT         NOT NULL PRIMARY KEY distkey sortkey,
                        first_name VARCHAR(22) NOT NULL,
                        last_name  VARCHAR(22) NOT NULL,
                        gender     VARCHAR(22) NOT NULL,
                        level      VARCHAR(22) NOT NULL
                        )
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song_table (
                        song_id   VARCHAR      NOT NULL PRIMARY KEY sortkey,
                        title     VARCHAR      NOT NULL,
                        artist_id VARCHAR      NOT NULL,
                        year      INTEGER      NOT NULL,
                        duration  FLOAT        NOT NULL
                        )
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist_table (
                        artist_id VARCHAR         NOT NULL PRIMARY KEY sortkey,
                        name      VARCHAR(22) NOT NULL,
                        location  VARCHAR       NOT NULL,
                        latitude  VARCHAR       NOT NULL,
                        longitude VARCHAR       NOT NULL
                        )
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time_table (
                        start_time INT NOT NULL PRIMARY KEY sortkey,
                        hour       INT NOT NULL,  
                        day        INT NOT NULL,
                        week       INT NOT NULL,
                        month      INT NOT NULL,
                        year       INT NOT NULL,
                        weekday    VARCHAR(22) NOT NULL
                        )
""")

# STAGING TABLES

staging_events_copy = ("""
                        copy staging_events_table FROM {}
                        iam_role {}
                        dateformat 'auto'
                        TIMEFORMAT as 'epochmillisecs'
                        format as json {}
                        compupdate off region 'us-west-2';
                    """).format(
                            config.get("S3", "LOG_DATA"),
                            config.get("IAM_ROLE", "ARN"),
                            config.get("S3", "LOG_JSONPATH")
                        )

staging_songs_copy = ("""
                        COPY staging_songs_table FROM {} 
                        iam_role {}
                        json 'auto'
                        compupdate off region 'us-west-2';
                        """).format(config.get("S3",'SONG_DATA'),
                                    config.get("IAM_ROLE", "ARN")
                                   )

# FINAL TABLES

#songplay_table_insert = ("""
 #                           INSERT INTO songplay_table 
  #                                          (songplay_id, 
   #                                         start_time, 
    #                                        user_id, 
     #                                       level, 
      #                                      song_id, 
       #                                     artist_id, 
        #                                    session_id, 
         #                                   location, 
          #                                  user_agent)                           
           #                 ON CONFLICT (songplay_id) DO NOTHING;
#""")                            
                            
user_table_insert     = (""" 
                            INSERT INTO user_table
                                            (user_id, 
                                            first_name, 
                                            last_name, 
                                            gender, 
                                            level)
                            SELECT DISTINCT
                                            userId,
                                            firstname,
                                            lastname,
                                            gender, 
                                            level
                            FROM staging_events_table
                            WHERE (userId) IS NOT NULL;     
""")

song_table_insert = ("""
                        INSERT INTO song_table
                                        (song_id, 
                                        title, 
                                        artist_id, 
                                        year, 
                                        duration)
                        SELECT DISTINCT
                                        song_id,
                                        title,
                                        artist_id,
                                        year,
                                        duration
                        FROM staging_songs_table
                        WHERE (song_id) IS NOT NULL;
                                                
""")

artist_table_insert = ("""
                        INSERT INTO artist_table
                                        (artist_id, 
                                        name, 
                                        location, 
                                        latitude, 
                                        longitude)
                        SELECT DISTINCT
                                        artist_id,
                                        artist_name,
                                        artist_location,
                                        artist_latitude,
                                        artist_longitude
                        FROM staging_songs_table
                        WHERE (artist_id) IS NOT NULL;                        
""")

#time_table_insert = ("""INSERT INTO time_table
 #                       (start_time, hour, day, week, month, year, weekday)
  #                      VALUES(%s, %s, %s, %s, %s, %s, %s)
   #                     ON CONFLICT (start_time) DO NOTHING;
#""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert]
