import csv
import pandas as pd
import dateutil
import json
from datetime import timezone
import numpy as np
import time
import shelve

class DataGenerator:


    def __init__(self, shelf='data', save_path='./'):

        self.IBGE_URLS = ['https://raw.githubusercontent.com/ALREstevam/covid19-br-vis-back/04d236a58efcd60efb6717cf8226ceb63419555d/app/static/municipios.csv',
                'https://raw.githubusercontent.com/kelvins/Municipios-Brasileiros/master/csv/municipios.csv'
            ]
        self.IBGE_JSON = './app/static/ufcapitalIBGEcode.json'
        self.shelf = shelf
        self.save_path = save_path

        self.loaded = {}


        self.WCOTA = {
            'CHANGES_ONLY': {
                'JSON':{
                    "URL": 'https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-cities-time_changesOnly.csv',
                    "KEY": 'CHANGES_JSON'
                },
                'GEOJSON':{
                    "URL": 'https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-cities-time_changesOnly.csv',
                    "KEY": 'CHANGES_GEO'
                }
            },
            'CITIES_TIME': {
                'JSON':{
                    "URL": 'https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-cities-time.csv',
                    "KEY": 'CITIES_JSON'
                },
                'GEOJSON':{
                    "URL": 'https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-cities-time.csv',
                    "KEY": 'CITIES_GEO'
                }
            },
        }


    def df_ibge_download(self, capitals):
        for url in self.IBGE_URLS:
            try:
                print(f'READING CITIES FROM {url}')
                return DataGenerator.process_ibge_df(
                    pd.read_csv(url), 
                    capitals
                )
            except:
                print('ERROR WHILE READING IBGE CSV')

    def generate_all(self, path='./app/static'):
        capitals = self.get_capitals()

        df_ibge = self.df_ibge_download(capitals)


        def gen(url, to_data):
            print(url)
            return DataGenerator.generate(url, DataGenerator.process_df, to_data, df_ibge, capitals)


        changes_json = gen(self.WCOTA['CHANGES_ONLY']['JSON']['URL'], DataGenerator.df_to_data)
        changes_geo = gen(self.WCOTA['CHANGES_ONLY']['GEOJSON']['URL'], DataGenerator.df_to_geojson_data)
        cities_json = gen(self.WCOTA['CITIES_TIME']['JSON']['URL'], DataGenerator.df_to_data)
        cities_geo = gen(self.WCOTA['CITIES_TIME']['GEOJSON']['URL'], DataGenerator.df_to_geojson_data)


        print('SAVING')
        DataGenerator.save(self.shelf, self.WCOTA['CHANGES_ONLY']['JSON']['KEY'], changes_json)
        DataGenerator.save(self.shelf, self.WCOTA['CHANGES_ONLY']['GEOJSON']['KEY'], changes_geo)
        DataGenerator.save(self.shelf, self.WCOTA['CITIES_TIME']['JSON']['KEY'], cities_json)
        DataGenerator.save(self.shelf, self.WCOTA['CITIES_TIME']['GEOJSON']['KEY'], cities_geo)

    def load(self, key):
        with shelve.open(self.save_path + '/' + self.shelf) as s:
            self.loaded[key] = s[key]
            return s[key]
            
    @staticmethod
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

    def get_capitals(self):
        print("GET CAPITALS")
        with open(self.IBGE_JSON) as json_file:
            return json.load(json_file)

    @staticmethod
    def process_ibge_df(df, capitals):
        print("PROCESSING IBGE_DF")
        df.set_index('codigo_ibge', inplace=True)
        return df

    @staticmethod
    def process_df(df, df_ibge, capitals):
        print("PROCESSING DF")
        lat, long = DataGenerator.lat_long_apply(df_ibge, capitals)
        df = df [ df['state'] != 'TOTAL' ]
        df.insert(len(df.columns), 'lat', np.nan)
        df.insert(len(df.columns), 'long', np.nan)
        df.loc[:, 'date'] = df.apply(lambda row: dateutil.parser.parse(row['date']), axis=1)
        df.loc[:, 'lat'] = df.apply(lat, axis=1)
        df.loc[:, 'long'] = df.apply(long, axis=1) 
        
        return df

    @staticmethod
    def df_to_data(df):
        '''
        Runs a function that generates a Pandas Dataframe and converts it to standard python data structures
        that can be converted JSON
        '''
        print('TO DATA JSON')
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
    def df_to_geojson_data(df):
        '''
        Runs a function that generates a Pandas Dataframe and converts it to standard python data structures
        that when converted to JSON are compliant with geojson standards 
        '''
        print('TO DATA GEOJSON')
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

    @staticmethod
    def generate(url, processor, to_data, ibge_df, capitals):
        def url2df(url):
            print(f'DOWNLOAD {url}')
            return pd.read_csv(url)

        return to_data(
            processor(
                url2df(url),
                ibge_df,
                capitals
            )
        )

    @staticmethod
    def save(shelf, key, data):
        with shelve.open(shelf) as s:
            s[key] = data


if __name__ == '__main__':
    DataGenerator('data', './app/static').generate_all()



