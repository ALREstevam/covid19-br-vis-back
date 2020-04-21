import csv
import requests
import pandas as pd
import io
from filecache import filecache
import dateutil
import json
import numpy.core.defchararray as npd
from memorize import memorize
from datetime import timezone
import numpy as np
import requests
import time
import os
from timeloop import Timeloop
from datetime import timedelta

tl = Timeloop()
def update_files_every(path_url_tuples, seconds=100):
    for tup in path_url_tuples:
        (lambda: RequestFileFallback.download(tup[0], tup[1]))()
        tl.job(interval=timedelta(seconds=seconds))(lambda: RequestFileFallback.download(tup[0], tup[1]))


class RequestFileFallback:

    @staticmethod
    def file_df(path):
        return pd.read_csv(path)

    @staticmethod
    def url_df(url):
        print(f'[PD_READ_CSV_ONLINE] {url}')
        return pd.read_csv(url)

    @staticmethod
    def download(url, out_path):
        print(f'[GET] CSV {url}')
        file = requests.get(url, timeout=1000)
        open(out_path, 'wb').write(file.content)

    @staticmethod
    def file_age(filepath):
        return time.time() - os.path.getmtime(filepath)

    @staticmethod
    def file_exists(path):
        return os.path.isfile(path)

    @staticmethod
    def download_to_df(url, path):
        RequestFileFallback.download(url, path)
        return RequestFileFallback.file_df(path)

    @staticmethod
    def download_or_fallback(url, path, max_age=60 * 60 * 3):
        if not RequestFileFallback.file_exists(path) or RequestFileFallback.file_age(path) > max_age:
            return RequestFileFallback.download_to_df(url, path)
        try:
            return RequestFileFallback.url_df(url)
        except:
            return RequestFileFallback.download_to_df(url, path)

    @staticmethod
    def read_or_fallback(url, path, max_age):

        if RequestFileFallback.file_exists(path) and RequestFileFallback.file_age(path) < max_age:
            print('READING FILE...')
            return RequestFileFallback.file_df(path)
        else:
            print('DOWNLOADING...')
            return RequestFileFallback.download_to_df(url, path)

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)

class WcotaCsv:
    '''
    Downloads and processes data from csv files hosted on GitHub
    '''
    
    def __init__(self):
        '''
        Defines base urls
        '''
        self.WCOTA_CHANGES_ONLY_URL = 'https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-cities-time_changesOnly.csv'
        self.WCOTA_CHANGES_ONLY_FILE = './app/static/WCOTA_CITIES_TIME_CHANGES_ONLY_CSV.csv'
        

        self.WCOTA_CITIES_TIME_URL = 'https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-cities-time.csv'
        self.WCOTA_CITIES_TIME_FILE = './app/static/WCOTA_CITIES_TIME_CSV.csv'
        
        
        self.IBGE_URL = 'https://raw.githubusercontent.com/kelvins/Municipios-Brasileiros/master/csv/municipios.csv'
        self.IBGE_FILE = './app/static/municipios.csv'

        
    
    @staticmethod
    def df(url, path, max_age=60 * 60 * 5):
        '''
        Downloads a csv file and convers it to a Pandas Dataframe
        '''
        return RequestFileFallback.read_or_fallback(url, path, max_age)



    def ibge_data(self):
        '''
        Generates data related to IBGE's city id
        '''
        with open('./app/static/ufcapitalIBGEcode.json') as json_file:
            capitals = json.load(json_file)

        df_ibge = WcotaCsv.df(self.IBGE_URL, self.IBGE_FILE, 60*60*10) 
        df_ibge.set_index('codigo_ibge', inplace=True)

        return df_ibge, capitals


    def lat_long_apply(self, df_ibge, capitals):
        def code(row):
            if row['city'].startswith('INDEFINIDA/'):
                code_value = capitals[row['state']]['capitalIBGECode']
            else:
                code_value = row['ibgeID']
            return int(code_value)

        def lat(row):
            return df_ibge.loc[ code(row) ].latitude

        def long(row):
            return df_ibge.loc[ code(row) ].longitude

        return lat, long
    
    @memorize(timeout=60 * 60)
    def default_df(self, url, save_path):
        df_ibge, capitals = self.ibge_data()
        lat, long = self.lat_long_apply(df_ibge, capitals)

        df = WcotaCsv.df(url, save_path)
        df = df [ df['state'] != 'TOTAL' ]

        df.insert(len(df.columns), 'lat', np.nan)
        df.insert(len(df.columns), 'long', np.nan)

        df.loc[:, 'date'] = df.apply(lambda row: dateutil.parser.parse(row['date']), axis=1)
        df.loc[:, 'lat'] = df.apply(lat, axis=1)
        df.loc[:, 'long'] = df.apply(long, axis=1) 
        
        print(f'DATAFRAME GENERATED FROM {url} or {save_path.split("/")[-1]}')

        return df

    def generate_cities_time_df(self):
        '''
        Generates a Dataframe from `self.WCOTA_CSV`
        '''
        return self.default_df(self.WCOTA_CHANGES_ONLY_URL, self.WCOTA_CHANGES_ONLY_FILE)

    def generate_df(self):
        '''
        Generates a Datafraeme from `self.WCOTA_CITIES_TIME_CSV`
        '''
        return self.default_df(self.WCOTA_CITIES_TIME_URL, self.WCOTA_CITIES_TIME_FILE)

    @staticmethod
    @memorize(timeout=60 * 60)
    def df_to_data(df_getter):
        '''
        Runs a function that generates a Pandas Dataframe and converts it to standard python data structures
        that can be converted JSON
        '''

        df = df_getter()
        result = []

        for index, row in df.iterrows():

            result.append({
                'date': row['date'],
                'state': row['state'],
                'city': row['city'].split('/')[0],
                'ibgeID': str(row['ibgeID']),
                'newDeaths': row['newDeaths'],
                'deaths': row['deaths'],
                'newCases': row['newCases'],
                'totalCases': row['totalCases'],
                'location': {
                    'lat': row['lat'],
                    'long': row['long'],
                },
            })
        return result

    @staticmethod
    @memorize(timeout=60 * 60)
    def df_to_geojson_data(df_getter):
        '''
        Runs a function that generates a Pandas Dataframe and converts it to standard python data structures
        that when converted to JSON are compliant with geojson standards 
        '''
        df = df_getter()
        features = []

        for index, row in df.iterrows():

            features.append({
                'type': 'Feature',
                'properties': {
                    'date': row['date'],
                    'timestamp': int(row['date'].replace(tzinfo=timezone.utc).timestamp()) * 1000,
                    'state': row['state'],
                    'city': row['city'].split('/')[0],
                    'ibgeID': str(row['ibgeID']),
                    'newDeaths': row['newDeaths'],
                    'deaths': row['deaths'],
                    'newCases': row['newCases'],
                    'totalCases': row['totalCases'],
                },
                'geometry':{
                    'type': 'Point',
                    'coordinates': [ row['long'], row['lat'] ],
                }
            })

        return {
            'type': 'FeatureCollection',
            'features': features 
        }


    def exercise(self):
        self.daily_geojson()
        self.daily_data()
        self.cases_geojson()
        self.cases_data()
        

    @memorize(timeout=60 * 60 * 3)
    def daily_geojson(self):
        '''
        Shortcut for getting the dataframe from `self.WCOTA_CITIES_TIME_CSV` as python's standard data structures 
        that when converted to JSON are compliant with geojson standards
        '''        
        return WcotaCsv.df_to_geojson_data(self.generate_cities_time_df)
        
    @memorize(timeout=60 * 60 * 3)
    def daily_data(self):
        '''
        Shortcut for getting the dataframe from `self.WCOTA_CITIES_TIME_CSV` as python's standard data structures
        '''        
        return WcotaCsv.df_to_data(self.generate_cities_time_df)

    @memorize(timeout=60 * 60 * 3)
    def cases_geojson(self):
        '''
        Shortcut for getting the dataframe from `self.WCOTA_CSV` as python's standard data structures 
        that when converted to JSON are compliant with geojson standards
        '''    
        return WcotaCsv.df_to_geojson_data(self.generate_df)
        
    @memorize(timeout=60 * 60 * 3)
    def cases_data(self):
        '''
        Shortcut for getting the dataframe from `self.WCOTA_CSV` as python's standard data structures
        ''' 
        return WcotaCsv.df_to_data(self.generate_df)
