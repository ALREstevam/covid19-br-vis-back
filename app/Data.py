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
        self.WCOTA_CSV = 'https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-cities-time_changesOnly.csv'
        self.WCOTA_CITIES_TIME_CSV = 'https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-cities-time.csv'
        self.IBGE_CSV = 'https://raw.githubusercontent.com/kelvins/Municipios-Brasileiros/master/csv/municipios.csv'
    
    @staticmethod
    def df(url):
        '''
        Downloads a csv file and convers it to a Pandas Dataframe
        '''
        print(f'DOWNLOADING {url}')
        return pd.read_csv(url)


    def ibge_data(self):
        '''
        Generates data related to IBGE's city id
        '''
        with open('./static/ufcapitalIBGEcode.json') as json_file:
            capitals = json.load(json_file)

        df_ibge = WcotaCsv.df(self.IBGE_CSV)
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



    def generate_cities_time_df(self):
        '''
        Generates a Datafraeme from `self.WCOTA_CSV`
        '''
        df_ibge, capitals = self.ibge_data()
        lat, long = self.lat_long_apply(df_ibge, capitals)

        df = WcotaCsv.df(self.WCOTA_CITIES_TIME_CSV)
        df = df [ df['state'] != 'TOTAL' ]
        df['date'] = df['date'].apply(dateutil.parser.parse)
        # df.loc[(df['city'].str.startswith('INDEFINIDA/')),'city'] = ''

        df['lat'] = df.apply(lat, axis=1) 
        df['long'] = df.apply(long, axis=1) 

        return df

    @memorize(timeout=60 * 60)
    def generate_df(self):
        '''
        Generates a Datafraeme from `self.WCOTA_CITIES_TIME_CSV`
        '''
        df_ibge, capitals = self.ibge_data()
        lat, long = self.lat_long_apply(df_ibge, capitals)
        
        df = WcotaCsv.df(self.WCOTA_CSV)
        df['date'] = df['date'].apply(dateutil.parser.parse)
        # df.loc[(df['city'].str.startswith('INDEFINIDA/')),'city'] = ''

        df['lat'] = df.apply(lat, axis=1) 
        df['long'] = df.apply(long, axis=1) 
        return df


    @memorize(timeout=60 * 60)
    def df_to_data(self, df_getter):
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

    @memorize(timeout=60 * 60)
    def df_to_geojson_data(self, df_getter):
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

    def daily_geojson(self):
        '''
        Shortcut for getting the dataframe from `self.WCOTA_CITIES_TIME_CSV` as python's standard data structures 
        that when converted to JSON are compliant with geojson standards
        '''        
        return self.df_to_geojson_data(self.generate_cities_time_df)
        
    def daily_data(self):
        '''
        Shortcut for getting the dataframe from `self.WCOTA_CITIES_TIME_CSV` as python's standard data structures
        '''        
        return self.df_to_data(self.generate_cities_time_df)

    def cases_geojson(self):
        '''
        Shortcut for getting the dataframe from `self.WCOTA_CSV` as python's standard data structures 
        that when converted to JSON are compliant with geojson standards
        '''    
        return self.df_to_geojson_data(self.generate_df)
        
    def cases_data(self):
        '''
        Shortcut for getting the dataframe from `self.WCOTA_CSV` as python's standard data structures
        ''' 
        return self.df_to_data(self.generate_df)
