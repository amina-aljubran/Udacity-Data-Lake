# Project Title

Data Lake for a music streaming startup (Sparkify)

## About the project

This project is building an ETL pipeline that extracts the data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.

## Project Datasets

There are two datasets that reside in S3
    1. Song data: s3://udacity-dend/song_data
        - path example: song_data/A/A/B/TRAABJL12903CDCF1A.json
        - Data example: 
            '''
            {
                "num_songs": 1, 
                "artist_id": "ARJIE2Y1187B994AB7", 
                "artist_latitude": null, 
                "artist_longitude": null, 
                "artist_location": "", 
                "artist_name": "Line Renaud", 
                "song_id": "SOUPIRU12A6D4FA1E1", 
                "title": "Der Kleine Dompfaff", 
                "duration": 152.92036, 
                "year": 0
            }
            '''         
    2. Log data: s3://udacity-dend/log_data
        - path example: log_data/2018/11/2018-11-12-events.json
        - Data example (First and se): 
            '''
            {
              artist,auth,firstName,gender,itemInSession,lastName,length,
                    level,location,method,page,registration,sessionId,song,
                    status,ts,userId
              A Fine Frenzy,Logged In,Anabelle,F,0,Simpson,267.91138,free,
                    "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD",PUT,NextSong,
                    1.54104E+12,256,Almost Lover (Album Version),200,1.54138E+12,69
            }
            '''
            
## Schema Analysis

It is a star schema as following:
    1. Fact Table
       - songplays: records in log data associated with song plays i.e. records with page "NextSong"
            * songplay_id, 
                start_time, 
                user_id, 
                level, 
                song_id,
                artist_id, 
                session_id, 
                location, 
                user_agent
    2. Dimension Tables
        - users: users in the app
            * user_id, 
                first_name, 
                last_name, 
                gender, 
                level
        - songs: songs in music database
            * song_id, 
                title, 
                artist_id, 
                year, 
                duration
        - artists: artists in music database
            * artist_id, 
                name, 
                location, 
                lattitude, 
                longitude
        - time: timestamps of records in songplays broken down into specific units
            * start_time, 
                hour, 
                day, 
                week, 
                month, 
                year, 
                weekday

### Determine The primary and foreign key

    - SONGPLAYS.songplay_id (PK)
    - SONGPLAYS.user_id     (FK) linked to  USERS.user_id     (PK)
    - SONGPLAYS.song_id     (FK) linked to  SONGS.song_id     (PK)
    - SONGPLAYS.artist_id   (FK) linked to  ARTISTS.artist_id (PK)
    - SONGPLAYS.start_time  (FK) linked to  TIME.start_time   (PK)
Where (PK) is primary key, and (FK) is foreign key.

## How to run it?

Write in the terminal:
    python etl.py

