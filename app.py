import requests
import pandas as pd
import os
import zipfile
from dotenv import load_dotenv
from authlib.integrations.requests_client import OAuth2Session
from functools import lru_cache

# API command to run in terminal
# kaggle datasets download -d nelgiriyewithana/top-spotify-songs-2023

# # Unzip code used to unzip the dataset
# zip_ref = zipfile.ZipFile('top-spotify-songs-2023.zip')
# zip_ref.extractall()
# zip_ref.close()

load_dotenv()

#spotify variables
spotify_auth_url = 'https://accounts.spotify.com/api/token'
client_id = os.environ.get('Client_ID')
client_secret = os.getenv('Client_secret')
spotify_base_url = "https://api.spotify.com/v1"


class SpotifyAPI:
    def __init__(self, spotify_auth_url, client_id, client_secret, spotify_base_url):
        self.spotify_auth_url = spotify_auth_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.spotify_base_url = spotify_base_url

    @lru_cache(maxsize=None)
    def _get_access_token(self):
        session = OAuth2Session(self.client_id, self.client_secret)
        response = session.fetch_token(self.spotify_auth_url, grant_type='client_credentials')
        token = response['access_token']
        # print(token)
        return token
    
    def _get(self, url, **params):

        token = self._get_access_token()
        headers = {
            'Authorization': "Bearer " + token
        }
        response = requests.get(self.spotify_base_url + url, headers=headers, params=params)
        # print(response)
        try:
            body = response.json()
            # print(body)
        except Exception:
            body = {}
        
        code = response.status_code
        if code !=200 and code!=400:
            raise 'Failed to call Spotify API.'
        
        return body, code

s_api = SpotifyAPI(spotify_auth_url, client_id, client_secret, spotify_base_url)

def search_track(track_name, artist_name):
    query = f"{track_name} artist:{artist_name}"
    json_data , code = s_api._get('/search', q=query, type='track')
    try:
        first_result = json_data['tracks']['items'][0]
        track_id = first_result['id']
        return track_id
    except (KeyError, IndexError):
        return None

def track_details(track_id):
    json_data , code = s_api._get(f'/tracks/{track_id}')
    try:
        return json_data['album']['images'][0]['url']
    except:
        return None

data = pd.read_csv('spotify-2023.csv',encoding='ISO-8859-1')
for i, row in data.iterrows():
    track_id = search_track(row['track_name'], row['artist(s)_name'])
    img_url = track_details(track_id)
    data.at[i,'image_url'] = img_url

data.to_csv('updated_file.csv', index=False)

