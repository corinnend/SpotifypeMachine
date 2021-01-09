# ----------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------
#                                  SPOTIFYPE MACHINE
# ----------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------
# Load external packages
import warnings
warnings.filterwarnings('ignore')

import spotipy
import spotipy.util as util
from spotipy.oauth2 import SpotifyClientCredentials

import mysql.connector
from mysql.connector import Error
import sqlalchemy
    
import pandas as pd
import numpy as np
import datetime
import requests
from bs4 import BeautifulSoup
import re

# =====================================================================
# Function to execute
# =====================================================================
def run_spm():
    
    # =================================================================
    # Load credential variables
    # =================================================================
    with open('api_keys.txt', 'r') as r:
        v = r.read().split(',')

    MYSQL_PW = v[0]
    CLINET_ID = v[1]
    CLINET_SECRET = v[2]
    REDIRECT_URI = v[3]
    USERNAME = v[4]
    PLAYLIST_ID = v[5]
    SCOPE = 'playlist-modify-public'
    
    # =================================================================
    # Authenticate
    # =================================================================
    sp = spotipy.Spotify(
    auth = util.prompt_for_user_token(
        username = USERNAME,
        scope = SCOPE,
        client_id = CLINET_ID,
        client_secret = CLINET_SECRET,
        redirect_uri = REDIRECT_URI))
    
    # =================================================================
    # For storing and loading tables into a local database
    # =================================================================
    # Store 
    def store_data(data, tbl, replace=False):
        conn = sqlalchemy.create_engine(
            'mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
            format('root', MYSQL_PW, '127.0.0.1', 'spotifype'), 
            pool_recycle=1, pool_timeout=57600).connect()

        if replace:
            write_as = 'replace'
        else:
            write_as = 'append'

        data.to_sql(con=conn, 
                    name=tbl,
                    index=False,
                    if_exists=write_as,
                    chunksize=100)

        conn.close()

    # Load
    def load_data(tbl):
        conn = sqlalchemy.create_engine(
            'mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
            format('root', MYSQL_PW, '127.0.0.1', 'spotifype'), 
            pool_recycle=1, pool_timeout=57600).connect()

        query = 'SELECT * FROM ' + tbl
        table = conn.execute(query)
        data = pd.DataFrame(table)
        data.columns = table.keys()
        conn.close()
        return data


    # =================================================================
    # For collecting songs on hypem.com to add
    # =================================================================
    def hypem_tracks():
        
        # -------------------------------------------------------
        # Scrape URLS
        # -------------------------------------------------------
        hm_urls = ['https://hypem.com/popular', 
                   'https://hypem.com/popular/2', 
                   'https://hypem.com/popular/3']


        hm_data = pd.DataFrame()

        for u in hm_urls:
            
            # Request page content
            hm_soup = BeautifulSoup(requests.get(u).content, 'html.parser')

            # Define lists to append
            titles = []
            artists = []
            track_ids = []

            # Loop through HTML
            for s in hm_soup.find_all('div',attrs={'class':'section-player'}):

                # Parse song titles
                try:
                    titles.append(s.find('span', attrs={'class':'base-title'}).text)
                except:
                    titles.append(None)     
                    
                # Parse artists
                try:   
                    artists.append(s.find('a', attrs={'class':'artist'}).text)
                except:
                    artists.append(None)  
                    
                # Parse Spotify track IDs
                try: 
                    track_ids.append(s.find('a',attrs={'href':re.compile('go/spotify')})['href'] \
                                     .replace('/go/spotify_track/', ''))
                except:
                    track_ids.append(None)
                    
            # Compile and remove songs without a Spotify ID     
            hm_out = pd.DataFrame({
                'title':titles, 
                'artist':artists,
                'track_id':track_ids}).dropna().reset_index(drop=True)

            # Append data
            hm_data = pd.concat([hm_data, hm_out], axis=0, ignore_index=True)

        # -------------------------------------------------------
        # Get song information from Spotify
        # -------------------------------------------------------
        # Assign columns to fill
        hm_data['released'] = None
        hm_data['artist_id'] = None
        hm_data['genres'] = None
    
        # Loop through hypem tracks
        for s in range(len(hm_data)):

            # Assign release date
            hm_data['released'][s] = sp.track(hm_data['track_id'][s]).get('album').get('release_date')

            # Assign artist ID
            hm_data['artist_id'][s] = sp.track(hm_data['track_id'][s]) \
                .get('album') \
                .get('artists')[0] \
                .get('uri').replace('spotify:artist:', '')

            # Assign genres
            hm_data['genres'][s] = sp.artist(hm_data['artist_id'][s]) \
                .get('genres')
            
        # Convert released date to year
        hm_data['released'] = pd.to_datetime(hm_data['released']).dt.year  


        # -------------------------------------------------------
        # Keep tracks to add
        # -------------------------------------------------------
        # Load preferred genres
        sp_genres = list(load_data('spotify_genres')['genre'])

        # Get tracks IDs already posted 
        hist_track_ids = list(load_data('hypem_data')['track_id'])

        # Assign new columns to fill
        hm_data['preferred_genre'] = 'N'
        hm_data['add_song'] = None

        # Loop through top songs
        for s in range(len(hm_data)):

            # Loop through genres and assign if it contains a preferred genre
            for g in hm_data['genres'][s]:
                if g in sp_genres:
                    hm_data['preferred_genre'][s] = 'Y'

            # If release date was within a year 
            # and song has not been posted to the playlist 
            # and genre is a preferred genre, then add
            if ((hm_data['released'][s] >= 2020) &
                (hm_data['track_id'][s] not in hist_track_ids) & 
                (hm_data['preferred_genre'][s] == 'Y')):
                hm_data['add_song'][s] = 'Y'
            else:
                hm_data['add_song'][s] = 'N'

        # Keep songs to post
        hm_data = hm_data[(hm_data['add_song'] == 'Y')].reset_index(drop=True) \
        [['title', 'artist', 'track_id', 'released', 'artist_id', 'genres']]

        return hm_data

    # =================================================================
    # Update Spotify playlist and store new songs in a local db
    # =================================================================
    hm_data = hypem_tracks()
    
    # If no new songs were collected
    if hm_data.empty:
        print('No songs to add.')

    else:
        # Add song posted date
        hm_data['posted_on'] = str(datetime.datetime.now().date())

        # Convert list of genres to string
        hm_data['genres'] = hm_data['genres'].astype(str)

        # Store new data
        store_data(hm_data, 'hypem_data', replace=False)

        # Add songs to playlist
        sp.user_playlist_add_tracks(
            user=USERNAME, 
            playlist_id=PLAYLIST_ID, 
            tracks=list(hm_data['track_id']), 
            position=0)

        print(len(hm_data), 'songs added to Spotifype Machine.')
        
