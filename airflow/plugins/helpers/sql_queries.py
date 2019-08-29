class SqlQueries:
   
    songplay_table_insert = ("""
        {truncate} {table_name_trunc}
        INSERT into {table_name}
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_event
            WHERE page='NextSong') events
            LEFT JOIN staging_song songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        {truncate} {table_nam_trunc}
        INSERT into {table_name}
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_event
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        {truncate} {table_nam_trunc}
        INSERT into {table_name}
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_song
    """)

    artist_table_insert = ("""
        {truncate} {table_nam_trunc}
        INSERT into {table_name}
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_song
    """)

    time_table_insert = ("""
        {truncate} {table_nam_trunc}
        INSERT into {table_name}
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)