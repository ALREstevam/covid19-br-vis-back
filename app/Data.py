import csv
import pandas as pd
import dateutil
import json
from datetime import timezone
import numpy as np
import time
from app.CsvDataManager import CsvDataManager, MemoryCache, df_data


#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', 100)


WCOTA_CHANGES_ONLY_URL = 'https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-cities-time_changesOnly.csv'
WCOTA_CHANGES_ONLY_FILE = './app/static/WCOTA_CITIES_TIME_CHANGES_ONLY_CSV.csv'

WCOTA_CITIES_TIME_URL = 'https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-cities-time.csv'
WCOTA_CITIES_TIME_FILE = './app/static/WCOTA_CITIES_TIME_CSV.csv'
        
IBGE_URL = 'https://raw.githubusercontent.com/kelvins/Municipios-Brasileiros/master/csv/municipios.csv'
IBGE_FILE = './app/static/municipios.csv'

IBGE_JSON = './app/static/ufcapitalIBGEcode.json'

cache = MemoryCache()

def get_cached(key):
    return cache.get(key)


def lat_long_apply(df_ibge, capitals):
    def code(row):
        if row['city'].startswith('CASO SEM LOCALIZAÇÃO DEFINIDA') or row['city'].startswith('INDEFINIDA/'):
            code_value = capitals[row['state']]['capitalIBGECode']
        else:
            code_value = row['ibgeID']
        return int(code_value)
    
    def lat(row):
        return df_ibge.loc[ code(row) ].latitude
    
    def long(row):
        return df_ibge.loc[ code(row) ].longitude
    return lat, long


def ibge_data():
        '''
        Generates data related to IBGE's city id
        '''
        return get_cached('IBGE_DF'), get_cached('CAPITALS')
        

def process_ibge_df(df):
    capitals = get_cached('CAPITALS')
    df.set_index('codigo_ibge', inplace=True)
    return df


def process_df(df):
        df_ibge, capitals = ibge_data()

        lat, long = lat_long_apply(df_ibge, capitals)

        df = df [ df['state'] != 'TOTAL' ]

        df.insert(len(df.columns), 'lat', np.nan)
        df.insert(len(df.columns), 'long', np.nan)

        df.loc[:, 'date'] = df.apply(lambda row: dateutil.parser.parse(row['date']), axis=1)
        df.loc[:, 'lat'] = df.apply(lat, axis=1)
        df.loc[:, 'long'] = df.apply(long, axis=1) 
        
        return df

def df_to_data(df):
        '''
        Runs a function that generates a Pandas Dataframe and converts it to standard python data structures
        that can be converted JSON
        '''

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

def df_to_geojson_data(df):
        '''
        Runs a function that generates a Pandas Dataframe and converts it to standard python data structures
        that when converted to JSON are compliant with geojson standards 
        '''
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



def daily_geojson(self):
    '''
    Shortcut for getting the dataframe from `self.WCOTA_CITIES_TIME_CSV` as python's standard data structures 
    that when converted to JSON are compliant with geojson standards
    '''        
    return df_to_geojson_data(self.generate_cities_time_df)
    

def daily_data(self):
    '''
    Shortcut for getting the dataframe from `self.WCOTA_CITIES_TIME_CSV` as python's standard data structures
    '''        
    return df_to_data(self.generate_cities_time_df)

def cases_geojson(self):
    '''
    Shortcut for getting the dataframe from `self.WCOTA_CSV` as python's standard data structures 
    that when converted to JSON are compliant with geojson standards
    '''    
    return df_to_geojson_data(self.generate_df)
    
def cases_data(self):
    '''
    Shortcut for getting the dataframe from `self.WCOTA_CSV` as python's standard data structures
    ''' 
    return df_to_data(self.generate_df)


with open(IBGE_JSON) as json_file:
    capitals = json.load(json_file)

cache.set('CAPITALS', capitals, 60*60*24*30)


dfs_data = [
    df_data('IBGE_DF', IBGE_URL, -1, process_ibge_df, lambda df: df),
    df_data('WCOTA_CHANGES_JSON', WCOTA_CHANGES_ONLY_URL, 60*60*5, process_df, df_to_data),
    df_data('WCOTA_CHANGES_GEOJSON', WCOTA_CHANGES_ONLY_URL, 60*60*5, process_df, df_to_geojson_data),
    df_data('WCOTA_TIME_JSON', WCOTA_CITIES_TIME_URL, 60*60*5, process_df, df_to_data),
    df_data('WCOTA_TIME_GEOJSON', WCOTA_CITIES_TIME_URL, 60*60*5, process_df, df_to_geojson_data),
]

thread = CsvDataManager(cache, update_cycle=10, cache_id='cache', dfs_data=dfs_data)
thread.start()