'''Removed my SQL and Spotify Login info'''

import spotipy
import spotipy.util as util
from spotipy.oauth2 import SpotifyClientCredentials 

import pymysql as mdb
import pandas as pd

class MySQLCon:
    
    def __init__(self):
        '''Your MySQL login configuration'''
        
        self._host = #host
        self._user = #user
        self._password = #password
        self._charset = 'utf8'
        self._unicode = True
        self._con = mdb.connect(host = self._host, 
                      user = self._user, 
                      passwd = self._password, 
                      charset = self._charset,
                      use_unicode = self._unicode);

    def create_database(self, db_name):
        # Query to create a database
        
        db_name = db_name
        create_db_query = "CREATE DATABASE IF NOT EXISTS {db} DEFAULT CHARACTER SET 'utf8'".format(db=db_name)

        # Create a database
        cursor = self._con.cursor()
        cursor.execute(create_db_query)
        cursor.close()


    def create_table(self, db_name, table_name):
        cursor = self._con.cursor()
        table_name = table_name
        # Create a table
        # The {db} and {table} are placeholders for the parameters in the format(....) statement
        create_table_query = '''CREATE TABLE IF NOT EXISTS {db}.{table} 
                                        (ind int,
                                        song_name varchar(250), 
                                        song_id varchar(250), 
                                        danceability float,
                                        energy float,
                                        `key` int,
                                        loudness float,
                                        mode int,
                                        speechiness float,
                                        acousticness float,
                                        instrumentalness float,
                                        liveness float,
                                        valence float,
                                        tempo float,
                                        label int,
                                        PRIMARY KEY(song_id)
                                        )'''.format(db=db_name, table=table_name)
        cursor.execute(create_table_query)
        cursor.close()
        
    def insert_data(self, db_name, table_name, features, ids, names):
        query_template = '''INSERT IGNORE INTO {db}.{table}(ind,
                                            song_name, 
                                            song_id, 
                                            danceability, 
                                            energy, 
                                            `key`,
                                            loudness,
                                            mode,
                                            speechiness,
                                            acousticness,
                                            instrumentalness,
                                            liveness,
                                            valence,
                                            tempo,
                                            label) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''.format(db=db_name, table=table_name)
        cursor = self._con.cursor()
        # names = good_names + bad_names
        # ids = good_ids + bad_ids
        for i in range(len(features)):
            ind = i
            song_name = names[i]
            song_id = ids[i]
            danceability = features[i]['danceability']
            energy = features[i]['energy']
            s_key = features[i]['key']
            loudness = features[i]['loudness']
            mode = features[i]['mode']
            speechiness = features[i]['speechiness']
            acousticness = features[i]['acousticness']
            instrumentalness = features[i]['instrumentalness']
            liveness = features[i]['liveness']
            valence = features[i]['valence']
            tempo = features[i]['tempo']
            label = features[i]['label']

            #print("Inserting station", song_name, "with song id: ", song_id)
            query_parameters = (ind, song_name, song_id, danceability, energy, s_key, loudness, mode, speechiness, acousticness, \
                               instrumentalness, liveness, valence, tempo, label)
            cursor.execute(query_template, query_parameters)

        self._con.commit()
        cursor.close()
        
        
    def fetch_data(self, db_name, table_name):
        cur = self._con.cursor(mdb.cursors.DictCursor)
        cur.execute("SELECT * FROM {db}.{table}".format(db=db_name, table=table_name))
        rows = cur.fetchall()
        cur.close()

        songs_df = pd.DataFrame(list(rows))
        songs_ids = list(songs_df["song_id"])
        
        return songs_ids

class Spotify_Interface:
    
    def __init__(self):
        self._client_id = #your client id (issued by Spotify API)
        self._client_secret = #client secret (issued by Spotify API)

        # my personal spotify user_id and playlist_id for two playlists

        self._abd_id = # user id
        self._playlist_liked = # playlist id
        self._playlist_disliked = # playlist id
        #self._sp = spotipy.Spotify() 
        
    def establish_spotify_connection(self):
        username = ""
        client_credentials_manager = SpotifyClientCredentials(client_id=self._client_id, client_secret=self._client_secret) 
        sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
        scope = 'user-library-read playlist-read-private'
        token = util.prompt_for_user_token(username, scope='user-library-read playlist-read-private',\
                                           client_id="<client id>",\
                                           client_secret="<client secret>", \
                                           redirect_uri='https://localhost:8888/callback/')
        if token:
            sp = spotipy.Spotify(auth=token)
            return sp
        else:
            print("Can't get token for", username)
            
    def fetch_songs(self, playlist_id, user_id='<user_id>'): #append the results in a list
        sp = self.establish_spotify_connection() #only call this
        sourcePlaylist = sp.user_playlist(user_id, playlist_id)
        tracks = sourcePlaylist["tracks"]
        songs = tracks["items"] 
        while tracks['next']:
            tracks = sp.next(tracks)
            for item in tracks["items"]:
                songs.append(item)

        return songs


    def song_name_id_helper(self, playlists):
        ids = []
        names = []
        for playlist in playlists:
            for i in range(len(playlist)):
                if playlist[i]['track']['id'] not in ids:
                    ids.append(playlist[i]['track']['id'])
                    names.append(playlist[i]['track']['name'])
        return ids, names


    def fetch_audio_features(self, ids_array,song_type):
        sp = self.establish_spotify_connection() #only call this

        features = []
        for i in range(0,len(ids_array),50):
            audio_features = sp.audio_features(ids_array[i:i+50])
            for track in audio_features:
                features.append(track)
                if song_type == "like":
                    features[-1]['label'] = 1
                else:
                    features[-1]['label'] = 0


        return features

    
    
 
    
if __name__ == '__main__':
    
    db_name = "spotify_songs"
    table_name = "english_songs"
    
    sqlConnection = MySQLCon() 
    
    #current_ids = sqlConnection.fetch_data(db_name, table_name)
    
    spotify = Spotify_Interface()
    
    #establish_spotify_connection(client_id, client_secret)
    
    liked = spotify.fetch_songs(spotify._playlist_liked)
    disliked = spotify.fetch_songs(spotify._playlist_disliked)
    
    liked_playlists = [liked]
    disliked_playlist = [disliked]
    
    liked_ids, liked_names = spotify.song_name_id_helper(liked_playlists)
    disliked_ids, disliked_names = spotify.song_name_id_helper(disliked_playlist)
    
    liked_features = spotify.fetch_audio_features(liked_ids, "like")
    disliked_features = spotify.fetch_audio_features(disliked_ids, "dislike")
    
    
    #sqlConnection.connect_to_mysql()
    
    sqlConnection.create_database(db_name)
    
    sqlConnection.create_table(db_name, table_name)
    
    sqlConnection.insert_data(db_name, table_name, liked_features, liked_ids, liked_names)
    sqlConnection.insert_data(db_name, table_name, disliked_features, disliked_ids, disliked_names)
    
    
