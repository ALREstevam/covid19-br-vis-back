from app.CsvConvert import CsvDownloader, CsvProcess, Data2Json
import json
import numpy as np
import dateutil
from datetime import timezone
import pandas as pd
import logging
import os
import shelve
from zipfile import ZipFile as zf, ZIP_DEFLATED

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
log = logging.getLogger(__name__)
pd.set_option('display.max_columns', None)

def load_capitals(ibge_json='./app/static/ufcapitalIBGEcode.json'):
    with open(ibge_json) as json_file:
        return json.load(json_file)


def df_saver(df, file_name, do_save=True):
    if do_save:
        log.info(f'SAVING DF {file_name} as CSV')
        df.to_csv(r'./app/static/df/' + file_name + '.csv', index=False, header=True, sep=',')
    return df
        

def ibge_df_process(df):
    log.info("PROCESSING IBGE DF")
    df.set_index('codigo_ibge', inplace=True)
    return df


def lat_long_apply(df_ibge):
    capitals = load_capitals()

    def code(row):
        if 'ibgeID' not in row\
                or row['city'].startswith('CASO SEM LOCALIZAÇÃO DEFINIDA')\
                or row['city'].startswith('INDEFINIDA/'):
            code_value = capitals[row['state']]['capitalIBGECode']
        else:
            code_value = row['ibgeID']
        return int(code_value)
    def lat(row):
        return df_ibge.loc[code(row)].latitude
    def long(row):
        return df_ibge.loc[code(row)].longitude
    return lat, long


def process_df(df, df_ibge):
    log.info("PROCESSING DF")
    lat, long = lat_long_apply(df_ibge)
    df = df[df['state'] != 'TOTAL']
    df.insert(len(df.columns), 'lat', np.nan)
    df.insert(len(df.columns), 'long', np.nan)
    df.loc[:, 'date'] = df.apply(lambda row: dateutil.parser.parse(row['date']), axis=1)
    df.loc[:, 'lat'] = df.apply(lat, axis=1)
    df.loc[:, 'long'] = df.apply(long, axis=1)
    df = df.where(pd.notnull(df), None)
    return df


def df2py(df):
    log.info('TO DATA JSON')
    result = []
    for index, row in df.iterrows():
        data = {
            'date': row['date'],
            'state': row['state'],
            'country': row['country'],
            'city': row['city'].split('/')[0],
            'newDeaths': row['newDeaths'],
            'deaths': row['deaths'],
            'newCases': row['newCases'],
            'totalCases': row['totalCases'],
            'ibgeID': str(row['ibgeID']) if 'ibgeID' in df else None,
            'deathsPer100kInhab': row['deaths_per_100k_inhabitants'] if 'deaths_per_100k_inhabitants' in df else None,
            'deathMSOfficial': row['deathsMS'] if 'deathsMS' in df else None,
            'totalCasesMSOfficial': row['totalCasesMS'] if 'totalCasesMS' in df else None,
            'totalCasesPer100kInhab': row[
                'totalCases_per_100k_inhabitants'] if 'totalCases_per_100k_inhabitants' in df else None,
            'deathOverTotal': row['deaths_by_totalCases'] if 'deaths_by_totalCases' in df else None,
            'location': {
                'lat': row['lat'],
                'long': row['long'],
            },
        }
        data = {k: v for k, v in data.items() if v is not None}
        result.append(data)
    return result


def df2pygeojson(df):
    log.info('TO DATA GEOJSON')
    features = []
    for index, row in df.iterrows():
        properties = {
            'date': row['date'],
            'country': row['country'],
            'timestamp': int(row['date'].replace(tzinfo=timezone.utc).timestamp()) * 1000,
            'state': row['state'],
            'city': row['city'].split('/')[0],
            'newDeaths': row['newDeaths'],
            'deaths': row['deaths'],
            'newCases': row['newCases'],
            'totalCases': row['totalCases'],
            'ibgeID': str(row['ibgeID']) if 'ibgeID' in df else None,
            'deathsPer100kInhab': row['deaths_per_100k_inhabitants'] if 'deaths_per_100k_inhabitants' in df else None,
            'deathMSOfficial': row['deathsMS'] if 'deathsMS' in df else None,
            'totalCasesMSOfficial': row['totalCasesMS'] if 'totalCasesMS' in df else None,
            'totalCasesPer100kInhab': row['totalCases_per_100k_inhabitants'] if 'totalCases_per_100k_inhabitants' in df else None,
            'deathOverTotal': row['deaths_by_totalCases'] if 'deaths_by_totalCases' in df else None,
        }

        properties = {k: v for k, v in properties.items() if v is not None}

        feature = {
            'type': 'Feature',
            'properties': properties,
            'geometry': {
                'type': 'Point',
                'coordinates': [row['long'], row['lat']],
            }
        }
        features.append(feature)

    return {
        'type': 'FeatureCollection',
        'features': features
    }


class DataGen:
    def __init__(self):
        self.process_steps = []
        self.shelf_path = "./app/static/shelf"

        self.sources = {
            'MUNICIPIOS': 'https://raw.githubusercontent.com/kelvins/Municipios-Brasileiros/master/csv/municipios.csv',
            'CASES-CITIES-TIME-CHANGESONLY': 'https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-cities-time_changesOnly.csv',
            'CASES-CITIES-TIME': 'https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-cities-time.csv',
            'CASES-STATES': 'https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-states.csv'
        }

        self.csv_files = {
            'MUNICIPIOS': r'./app/static/csv/municipios.csv',
            'CASES-CITIES-TIME-CHANGESONLY': r'./app/static/csv/cases-brazil-cities-time_changesOnly.csv',
            'CASES-CITIES-TIME': r'./app/static/csv/cases-brazil-cities-time.csv',
            'CASES-STATES': r'./app/static/csv/cases-brazil-states.csv',
        }

        self.processed = {}
        self.json_files = {}

    def json_saver(self, data, file_name, do_save=True):
        if do_save:
            log.info(f'SAVING {file_name} as JSON')
            json_str = Data2Json().as_json(data)
            
            zip_path = r'./app/static/zip_json/' + file_name + '.zip'
            json_file = r'content.json'

            with zf(zip_path, mode='w', compression=ZIP_DEFLATED) as file:
                file.writestr(json_file, json_str)

                
            self.json_files[file_name] = {
                'file_name': file_name,
                'path': zip_path,
            }

        return data

    def download(self):
        for key in self.sources.keys():
            source = self.sources[key]
            self.csv_files[key] = CsvDownloader().src(source).download()
        return self

    def register_process_step(self, key, step):
        self.process_steps.append((key, step ))
        return self
    
    def run(self):
        for count, (key, step) in enumerate(self.process_steps, start=1):
            log.info(f'>>> Processing "{key}"\t|\t  [{count}/{len(self.process_steps)}]')
            self.processed[key] = step(key)
        return self

    def register_default_steps(self, save_json_files=True, save_df_files=True):
        
        self.register_process_step('IBGE', 
            lambda key: CsvProcess().csv(self.csv_files['MUNICIPIOS'])\
                .register_df_transformer(transformer=ibge_df_process)\
                .run()
        )

        self.register_process_step('PYJSON-CASES-CITIES-TIME-CHANGESONLY',
            lambda key: CsvProcess().csv(self.csv_files['CASES-CITIES-TIME-CHANGESONLY'])\
                .register_df_transformer(lambda df: process_df(df, self.processed['IBGE']))\
                .register_storager(lambda df: df_saver(df, key, save_df_files))\
                .register_df_transformer(df2py)\
                .register_storager(lambda data: self.json_saver(data, key, save_json_files))\
                .run()
        )

        self.register_process_step('PYGEOJSON-CASES-CITIES-TIME-CHANGESONLY',
            lambda key: CsvProcess().csv(self.csv_files['CASES-CITIES-TIME-CHANGESONLY'])\
                .register_df_transformer(lambda df: process_df(df, self.processed['IBGE']))\
                .register_storager(lambda df: df_saver(df, key, save_df_files))\
                .register_df_transformer(df2pygeojson)\
                .register_storager(lambda data: self.json_saver(data, key, save_json_files))\
                .run()
        )

        self.register_process_step('PYJSON-CASES-CITIES-TIME',
            lambda key: CsvProcess().csv(self.csv_files['CASES-CITIES-TIME'])\
                .register_df_transformer(lambda df: process_df(df, self.processed['IBGE']))\
                .register_storager(lambda df: df_saver(df, key, save_df_files))\
                .register_df_transformer(df2py)\
                .register_storager(lambda data: self.json_saver(data, key, save_json_files))\
                .run()
        )

        self.register_process_step('PYGEOJSON-CASES-CITIES-TIME',
            lambda key: CsvProcess().csv(self.csv_files['CASES-CITIES-TIME'])\
                .register_df_transformer(lambda df: process_df(df, self.processed['IBGE']))\
                .register_storager(lambda df: df_saver(df, key, save_df_files))\
                .register_df_transformer(df2pygeojson)\
                .register_storager(lambda data: self.json_saver(data, key, save_json_files))\
                .run()
        )

        self.register_process_step('PYJSON-CASES-STATES',
            lambda key: CsvProcess().csv(self.csv_files['CASES-STATES'])\
                .register_df_transformer(lambda df: process_df(df, self.processed['IBGE']))\
                .register_storager(lambda df: df_saver(df, key, save_df_files))\
                .register_df_transformer(df2py)\
                .register_storager(lambda data: self.json_saver(data, key, save_json_files))\
                .run()
        )

        self.register_process_step('PYGEOJSON-CASES-STATES',
            lambda key: CsvProcess().csv(self.csv_files['CASES-STATES'])\
                .register_df_transformer(lambda df: process_df(df, self.processed['IBGE']))\
                .register_storager(lambda df: df_saver(df, key, save_df_files))\
                .register_df_transformer(df2pygeojson)\
                .register_storager(lambda data: self.json_saver(data, key, save_json_files))\
                .run()
        )

        return self


    def serialize_json_path(self):
        log.info('SERIALIZING')
        with shelve.open(self.shelf_path) as s:
            s['json_files'] = self.json_files
        return self


    def load_json_data(self):
        with shelve.open(self.shelf_path) as s:
            json_files = s['json_files']
            for key in json_files.keys():
                #log.info(f'READING "{key}"')
                finfo = json_files[key]
                print(f'READING "{key}"')

                if str(finfo['path']).endswith('.zip'):
                    self.processed[finfo['file_name']] = zf(finfo['path'])\
                        .read('content.json')\
                        .decode('UTF-8')
                if str(finfo['path']).endswith('.json'):
                    with open(finfo['path']) as file:
                        self.processed[finfo['file_name']] = file.read()
        return self

    def get_json(self, key):
        return self.processed[key]

