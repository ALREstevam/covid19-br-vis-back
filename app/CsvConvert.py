import csv
import pandas as pd
import dateutil
import json
import numpy as np
import time
import shelve
import requests
from tqdm import tqdm
from datetime import time, datetime, date, timezone
import logging
import os

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
log = logging.getLogger(__name__)


class CsvDownloader:
    '''
    > CsvDownloader()\
    > .src('https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-cities-time_changesOnly.csv')\
    > .download()
    '''
    def __init__(self):
        self.url = None
        self.filename = None
    
    def src(self, source_url):
        self.url = source_url
        return self

    def download(self):
        chunk_size = 1024
        req = requests.get(self.url, stream=True)
        total_size = int(req.headers['content-length'])
        self.filename = './app/static/csv/' + self.url.split('/')[-1]

        with open(self.filename, "wb") as file:
            log.info(f'[DOWNLOAD] {self.url}')
            log.info('\n')
            for data in tqdm(iterable=req.iter_content(chunk_size=chunk_size), total=total_size/chunk_size, unit='KB'):
                file.write(data.replace(b',\n', b'\n'))
        return self.filename


class CsvProcess:
    def __init__(self):
        self.filename = None
        self.df_transformers = []

    def csv(self, path):
        self.filename = path
        return self

    def register_df_transformer(self, transformer):
        self.df_transformers.append(transformer)
        return self

    def register_storager(self, storager):
        self.df_transformers.append(storager)
        return self

    def run(self):
        df = pd.read_csv(self.filename)
        for transf in self.df_transformers:
            result = transf(df)
            df = df if result is None else result
        return df

class CustomJSONEncoder(json.JSONEncoder):
    def __init__(self, **kwargs):
        kwargs['ensure_ascii'] = False
        super(CustomJSONEncoder, self).__init__(**kwargs)

    def default(self, obj):
        try:
            if isinstance(obj, date):
                return obj.isoformat()
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return json.JSONEncoder.default(self, obj)

class Data2Json:
    def __init__(self):
        pass

    def as_json(self, data):
        return CustomJSONEncoder().encode(data)
