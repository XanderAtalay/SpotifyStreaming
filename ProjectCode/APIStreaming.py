#!/usr/bin/env python
# coding: utf-8

# In[1]:


import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from lyricsgenius import Genius
import re
import contextlib
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
nltk.download('vader_lexicon')
from secrets import *
from ProjectFunctions import *
from datetime import datetime
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import pymysql

def get_dataframe(user_id, pwd, host_name, db_name, sql_query):
    conn_str = f"mysql+pymysql://{user_id}:{pwd}@{host_name}/{db_name}"
    sqlEngine = create_engine(conn_str, pool_recycle=3600)
    connection = sqlEngine.connect()
    dframe = pd.read_sql(sql_query, connection);
    connection.close()
    
    return dframe


def set_dataframe(user_id, pwd, host_name, db_name, df, table_name, pk_column, db_operation):
    conn_str = f"mysql+pymysql://{user_id}:{pwd}@{host_name}/{db_name}"
    
    sqlEngine = create_engine(conn_str, pool_recycle=3600)
    connection = sqlEngine.connect()
    
    if db_operation == "insert":
        df.to_sql(table_name, con=connection, index=False, if_exists='replace')
        sqlEngine.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({pk_column});")
            
    elif db_operation == "update":
        df.to_sql(table_name, con=connection, index=False, if_exists='append')
    
    connection.close()

# The following code is used to access the spotify API using my client id and secret.
client_credentials_manager = SpotifyClientCredentials(client_id=spotify_client_id, client_secret=spotify_client_secret)
spotify = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

print("Connected to Spotify API Sucessfully")


# This code connects to the Genius API, which I'll be using to obtain song lyrics.
genius = Genius(genius_api_token)

print("Connected to Genius API Sucessfully")

currentTime = str(datetime.now()).replace(" ", "T")[:(len(str(datetime.now()).replace(" ", "T")) - 7)]

currentFeaturedPlaylists = spotify.featured_playlists(locale=None, country=None, timestamp=currentTime, limit=50, offset=0)['playlists']



# Initialize lists to use to create data frame columns.
playlistIDs = []
playlistNames = []
playlistDescriptions = []
playlistImageURLs = []

# Iterate through each album, adding the id, name, release date, and number of tracks to their respective lists.
for playlist in currentFeaturedPlaylists['items']:
    playlistIDs.append(playlist['id'])
    playlistNames.append(playlist['name'])
    playlistDescriptions.append(playlist['description'])
    playlistImageURLs.append(playlist['images'][0]['url'])
    

# Create a new data frame using the album information.
currentFeaturedPlaylistsTable = pd.DataFrame(data = {
    'playlist_key' : list(range(0,len(playlistIDs))),
    'playlist_id' : playlistIDs,
    'playlist_name' : playlistNames,
    'playlist_description' : playlistDescriptions,
    'playlist_image_url' : playlistImageURLs,
    'featured_dt' : currentTime
})
                         
print("Playlist Table Created")


                         
playlistIDs = []
trackIDs = []
trackNames = []
trackArtists = []
trackAlbum = []
trackAddedAt = []
trackDurations = []
trackPopularities = []
trackExplicit = []


for playlistId in currentFeaturedPlaylistsTable['playlist_id']:
    playlistTracks = spotify.playlist_items(playlistId, limit = 50)['items']
    playlistName = currentFeaturedPlaylistsTable
    for trackInfo in playlistTracks:
        track = trackInfo['track']
        playlistIDs.append(playlistId)
        trackAddedAt.append(trackInfo['added_at'])
        trackIDs.append(track['id'])
        trackNames.append(track['name'])
        
        artists = []
        for artistInfo in track['artists']:
            artists.append(artistInfo['name'])
        
        trackArtists.append(artists[0])
        
        trackAlbum.append(track['album']['name'])
        trackDurations.append(track['duration_ms'])
        trackExplicit.append(track['explicit'])
        trackPopularities.append(track['popularity'])

currentFeaturedTracksTable = pd.DataFrame(data = {
    'track_key' : list(range(0,len(trackIDs))),
    'playlist_id' : playlistIDs,
    'track_id' : trackIDs,
    'track_name' : trackNames,
    'track_artists' : trackArtists,
    'track_album' : trackAlbum,
    'track_date_added' : trackAddedAt,
    'track_duration_ms' : trackDurations,
    'Explicit' :  trackExplicit,
    'Popularity' : trackPopularities
})


print("Featured Tracks Table Compiled")


import warnings; warnings.simplefilter('ignore')

currentFeaturedTrackAnalysisTable = currentFeaturedTracksTable[['track_key', 'track_id', 'track_name']]

trackDanceability = []
trackEnergy = []
trackKey = []
trackLoudness = []
trackSpeechiness = []
trackAcousticness = []
trackInstrumentalness = []
trackLiveness = []
trackValence = []
trackTempo = []

for track in currentFeaturedTrackAnalysisTable['track_id']:
    audio_features = spotify.audio_features(track)
    
    try:
        trackDanceability.append(audio_features[0]['danceability'])
        trackEnergy.append(audio_features[0]['energy'])
        trackKey.append(audio_features[0]['key'])
        trackLoudness.append(audio_features[0]['loudness'])
        trackSpeechiness.append(audio_features[0]['speechiness'])
        trackAcousticness.append(audio_features[0]['acousticness'])
        trackInstrumentalness.append(audio_features[0]['instrumentalness'])
        trackLiveness.append(audio_features[0]['liveness'])
        trackValence.append(audio_features[0]['valence'])
        trackTempo.append(audio_features[0]['tempo'])
    
    except:
        trackDanceability.append(None)
        trackEnergy.append(None)
        trackKey.append(None)
        trackLoudness.append(None)
        trackSpeechiness.append(None)
        trackAcousticness.append(None)
        trackInstrumentalness.append(None)
        trackLiveness.append(None)
        trackValence.append(None)
        trackTempo.append(None)
        

# currentFeaturedTrackAnalysisTable['Danceability'] = trackDanceability
currentFeaturedTrackAnalysisTable['Energy'] = trackEnergy
currentFeaturedTrackAnalysisTable['Key'] = trackKey
currentFeaturedTrackAnalysisTable['Loudness'] = trackLoudness
currentFeaturedTrackAnalysisTable['Speechiness'] = trackSpeechiness
currentFeaturedTrackAnalysisTable['Acousticness'] = trackAcousticness
currentFeaturedTrackAnalysisTable['Instrumentalness'] = trackInstrumentalness
currentFeaturedTrackAnalysisTable['Liveness'] = trackLiveness
currentFeaturedTrackAnalysisTable['Valence'] = trackValence
currentFeaturedTrackAnalysisTable['Tempo'] = trackTempo

print("Track Analysis Table Created")



trackLyricsTable = currentFeaturedTracksTable[["track_key", "track_id", "track_name"]]
songLyrics = []

for index, row in currentFeaturedTracksTable.iterrows():
#     with contextlib.redirect_stdout(None): #This makes sure that we don't see console output during this st
        songLyrics.append(get_lyrics(row['track_name'], row['track_artists']))
    
trackLyricsTable["track_lyrics"] = songLyrics

trackLyricSentimentTable = trackLyricsTable
SentimentAnalysis = SentimentIntensityAnalyzer()

trackNeg = []
trackNeu = []
trackPos = []
trackComp = []

for lyrics in trackLyricsTable['track_lyrics']:
    try:
        SA = SentimentAnalysis.polarity_scores(lyrics)
        trackNeg.append(SA['neg'])
        trackPos.append(SA['pos'])
        trackNeu.append(SA['neu'])
        trackComp.append(SA['compound'])
    except:
        trackNeg.append(None)
        trackPos.append(None)
        trackNeu.append(None)
        trackComp.append(None)
    
trackLyricSentimentTable['track_negative_sentiment'] = trackNeg
trackLyricSentimentTable['track_neutral_sentiment'] = trackNeu
trackLyricSentimentTable['track_positive_sentiment'] = trackPos
trackLyricSentimentTable['track_compound_sentiment'] = trackComp

trackLyricSentimentTable = trackLyricSentimentTable.drop(columns = ['track_lyrics'])

print("Track Lyric Sentiment Table Created")

host_name = "localhost"
host_ip = "10.0.0.217"
port = "3306"

user_id = "root"
pwd = "Xander22"
dst_database = "CurrentSpotifyFeatured"
src_database = "AllTimeSpotifyFeatured"

exec_sql = f"CREATE DATABASE `{dst_database}`;"

conn_str = f"mysql+pymysql://{user_id}:{pwd}@{host_name}"
sqlEngine = create_engine(conn_str, pool_recycle=3600)
sqlEngine.execute(exec_sql) #create db
sqlEngine.execute("USE " + dst_database + ";") # select new db

# This will throw an error if the data frame already exists.

db_operation = "insert"

tables = [('currentFeaturedPlaylists', currentFeaturedPlaylistsTable, 'playlist_key'),
          ('currentFeaturedTracks', currentFeaturedTracksTable, 'track_key'),
          ('currentFeaturedTrackAnalysis', currentFeaturedTrackAnalysisTable, 'track_key'),
          ('currentFeaturedTrackLyricSentiment', trackLyricSentimentTable, 'track_key')]

for table_name, dataframe, primary_key in tables:
    set_dataframe(user_id, pwd, host_name, dst_database, dataframe, table_name, primary_key, db_operation)

print("Tables uploaded to SQL Database")