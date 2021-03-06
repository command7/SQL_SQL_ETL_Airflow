class SqlQueries:
    songplay_table_insert = ("""
        INSERT INTO {} (playid, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT
                md5(events.start_time) playid,
                events.start_time as start_time,
                events.userid as user_id,
                events.level as level,
                songs.song_id as song_id,
                songs.artist_id as artist_id,
                events.sessionid as session_id,
                events.location as location,
                events.useragent as user_agent
        FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
        LEFT JOIN staging_songs songs
        ON events.song = songs.title
        AND events.artist = songs.artist_name
        AND events.length = songs.duration
        WHERE user_id IS NOT NULL
        AND start_time IS NOt NULL
    """)

    user_table_insert = ("""
        INSERT INTO {} (userid, first_name, last_name, gender, level)
        SELECT distinct userid as userid,
            firstname as first_name,
            lastname as last_name,
            gender,
            level
        FROM staging_events
        WHERE page='NextSong'
        AND userid IS NOT NULL
    """)

    song_table_insert = ("""
        INSERT INTO {} (songid, title, artist_id, year, duration)
        SELECT distinct song_id as songid, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        INSERT INTO {} (artistid, name, location, lattitude, longitude)
        SELECT distinct artist_id as artistid,
            artist_name as name,
            artist_location as location,
            artist_latitude as lattitude,
            artist_longitude as longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        INSERT INTO {} (start_time, hour, day, week, month, year, weekday)
        SELECT start_time,
            extract(hour from start_time) as hour,
            extract(day from start_time) as day,
            extract(week from start_time) as week, 
            extract(month from start_time) as month,
            extract(year from start_time) as year,
            extract(dayofweek from start_time) as weekday
        FROM songplays
    """)

    truncate_table = "TRUNCATE TABLE {}"