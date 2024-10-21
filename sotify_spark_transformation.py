import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import explode, col, to_date
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame
import boto3
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

s3_path = "s3://spotify-etl-project-manas/raw_data/to_processed/"
source_dyf = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    connection_options={"paths": [s3_path]},
    format="json",
)
spotify_df = source_dyf.toDF()
def process_album(df): 
    df = df.withColumn("items", explode("items")).select(col("items.track.album.id").alias("album_id"), 
                                               col("items.track.album.name").alias("album_name"),
                                               col("items.track.album.release_date").alias("release_date"),
                                               col("items.track.album.total_tracks").alias("total_tracks"),
                                               col("items.track.album.external_urls.spotify").alias("url")
                                               ).drop_duplicates(['album_id'])
    return df

def process_artists(df): 
    df_items_exploded = df.select(explode(col("items")).alias("item"))
    df_artist_exploded = df_items_exploded.select(explode(col("item.track.artists")).alias("artists"))
    df_artist = df_artist_exploded.select(col("artists.id").alias("artist_id"),
                         col("artists.name").alias("artist_name"),
                         col("artists.external_urls.spotify").alias("external_url")).drop_duplicates(['artist_id'])
    return df_artist

def process_songs(df) : 
    df_items_exploded = df.select(explode(col("items")).alias("item"))
    df_songs = df_items_exploded.select(
    col("item.track.id").alias("song_id"),
    col("item.track.name").alias("song_name"),
    col("item.track.duration_ms").alias("duration_ms"),
    col("item.track.external_urls.spotify").alias("url"),
    col("item.track.popularity").alias("popularity"),
    col("item.added_at").alias("song_added"),
    col("item.track.album.id").alias("album_id"),
    col("item.track.artists")[0]["id"].alias("artist_id")
    ).drop_duplicates(['song_id'])
    
    df_songs = df_songs.withColumn("song_added",to_date(col("song_added")))
    return df_songs

album_df = process_album(spotify_df)
artist_df = process_artists(spotify_df)
song_df = process_songs(spotify_df)

def write_to_s3(df, path_suffix, format_type = "csv"):
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": "s3://spotify-etl-project-manas/transformed_data/" + path_suffix},
        format=format_type
    )

write_to_s3(album_df,"album_data/album_tansforment_{}".format(datetime.now().strftime("%Y%m%d")),'csv')
write_to_s3(artist_df, "artist_data/artist_transformed_{}".format(datetime.now().strftime("%Y%m%d")), 'csv')
write_to_s3(song_df, "songs_data/song_transformed_{}".format(datetime.now().strftime("%Y%m%d")), 'csv')


def list_s3_objects(bucket, prefix):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    # Extract keys that end with '.json'
    keys = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.json')]
    return keys

bucket_name = "spotify-etl-project-manas"
prefix = "raw_data/to_processed/"
spotify_keys = list_s3_objects(bucket_name, prefix)


def move_and_delete_files(spotify_keys, Bucket):
    s3_resource = boto3.resource('s3')
    
    for key in spotify_keys:
        # Define the source for copying
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }

        # Define the destination key
        destination_key = 'raw_data/processed/' + key.split("/")[-1]

        # Copy the file to the new location
        s3_resource.meta.client.copy(copy_source, Bucket, destination_key)

        # Delete the original file
        s3_resource.Object(Bucket, key).delete()
move_and_delete_files(spotify_keys, bucket_name)        
        

job.commit()