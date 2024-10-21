import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3 # this is package created by aws for programatically comunicate with aws services
from datetime import datetime

def lambda_handler(event, context):
    client_id = os.environ.get('client_id') # this is how we can use environment variable which we saved under configuration like cilent id and cliend secret 
    client_secret = os.environ.get('client_secret')
    
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id , client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    playlists = sp.user_playlists('spotify')
    
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=1333723a6eff4b7f"
    playlist_URI = playlist_link.split('/')[-1].split('?')[0]
    data = sp.playlist_tracks(playlist_URI)
    
    filename = 'spotify_raw_'+str(datetime.now()) + '.json'
    client = boto3.client('s3')
    
    client.put_object(
        Bucket = 'spotify-etl-project-manas',
        Key = 'raw_data/to_processed/' + filename,
        Body  = json.dumps(data)
        )
        
    glue = boto3.client("glue")
    gluejobname = "Spotify_Transformation_Job"
    
    try : 
        runID = glue.start_job_run(JobName = gluejobname)
        status = glue.get_job_run(JobName = gluejobname, RunId = runID['JobRunId'])
        print('JobStatus : ', status['JobRun']['JobRunState'])
    except Exception as e : 
        print(e)