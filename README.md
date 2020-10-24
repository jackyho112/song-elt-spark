# Data Modeling with Spark

In this project, we are extracting data regarding song plays from JSON files into Parquet files with Spark.

## Dataset 

The dataset comes from the [Million Song Dataset](http://millionsongdataset.com/). The data should contain two folders, one for the log and one for the songs. The song dataset should contain metadata about a song and its artist. The log dataset should contain acitivity records of played songs.

## Schema

In the output files, we are creating the following tables/datasets.

songplays - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
users - user_id, first_name, last_name, gender, level
songs - song_id, title, artist_id, year, duration
artists - artist_id, name, location, latitude, longitude
time - start_time, hour, day, week, month, year, weekday

## Running the scripts

This project contains a script, `etl.py`. It runs the pipeline to extract the records from the JSON files, transform them into the proper datasets, and save them as Parquet files. It requires `dwh.cfg` to be filled before being run.

To run the scripts, run the following code in your terminal:
```bash
python etl.py
```

or the following in your notebook:
```python
!python etl.py
```