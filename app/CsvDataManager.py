import requests
import pandas as pd
import shelve
from threading import Thread
import time
import random
import os

def file_exists(path):
    return os.path.isfile(path)

def df_data(key, csv_url, timeout_to_update, process_df, df_to_data):
    return {
        'key': key, 
        'csv_url':csv_url, 
        'df_map': process_df, 
        'df2data': df_to_data,
        'timeout_to_update': timeout_to_update,
    }


class MemoryCache:

    def __init__(self):
        self.cache = {}
        self.data_source = None

    def set(self, key, value, timeout):
        print(f'[SETTING] `{key}`')
        self.cache[key] = {
            'created': time.time(),
            'timeout': timeout,
            'value': value, 
        }

    def get(self, key):
        print(f'GETTING KEY {key}')
        print(f'KNOWN KEYS {list(self.cache.keys())}')
        if key in self.cache.keys():
            return self.cache[key]['value']
        return 'NOT FOUND'

    def bind(self, me):
        self.data_source = me

   


class CsvDataManager(Thread):
    '''    
    Auto updates CSV files from the web

    > memc = MemoryCache()
    > thread = CsvDataManager(memc, live_cycle=10, cache_id='test_shelf')
    > 
    > thread.start()
    > 
    > while True:
    >     print('CURRENT CACHE', memc.cache, thread.keys_to_update)
    >     time.sleep(1)
    >     if random.random() > .95:
    >         print('REQUESTING UPDATE A')
    >         thread.request_update('A')
    >     if random.random() > .95:
    >         print('REQUESTING UPDATE B')
    >         thread.request_update('B')
    '''
    def __init__(self, cache, cache_id, update_cycle, dfs_data=[]):
        Thread.__init__(self)

        self.state = "INIT"

        cache.bind(self)

        self.cache = cache
        self.cache_id = cache_id

        dfs = {}

        for item in dfs_data:
            dfs[item['key']] = item

        self.dfs_data = dfs
        self.load_keys = list(map(lambda el: el['key'], dfs_data))
        self.keys_to_update = [] #self.load_keys
        self.update_cycle = update_cycle

        print(f'[WILL UPDATE] {self.keys_to_update}')

        for key in self.load_keys:
            if key not in self.cache.cache.keys():
                self.request_update(key)

        self.state = "INIT_DONE"

    def run(self):
        #self.update()
        self.live()

    def get_state(self):
        if len(self.keys_to_update) > 0:
            if self.state == 'INIT_DONE':
                return 'CACHE_UPDATE'
            else:
                return 'CACHE_LOAD'
        else: 
            return self.state

    def live(self):
        print('[UPDATE QUEUE]', self.keys_to_update)
        print('[KNOWN KEYS]', self.cache.cache.keys())

        while True:
            if len(self.keys_to_update) > 0:
                self.update(self.keys_to_update[0])
            else:
                self.check_cache_timeout()

                # try:
                #     self.update(self.keys_to_update[0])
                #     #self.keys_to_update.remove(self.keys_to_update[0])
                #     #self.keys_to_update.pop(0)
                # except Exception as e:
                #     print(e)
                #     key = self.keys_to_update[0]
                #     self.keys_to_update.pop(0)
                #     self.request_update(key=key)


            time.sleep(self.update_cycle)

    def check_cache_timeout(self):
        #print('[TIMEOUT CHECK]')
        for key in self.cache.cache.keys():
            element = self.cache.cache[key]

            #print(f'> {key} {"INF" if element["timeout"] <= 0 else element["created"] + element["timeout"] - time.time()}secs remaining')

            if element['timeout'] > 0 and element['created'] + element['timeout'] - time.time() < 0:
                self.request_update(key=key)
                print('[TIMEOUT FOR KEY]', key)
            
    def request_update(self, key):
        if key not in self.keys_to_update:
            self.keys_to_update.append(key)

    def finish(self):
        self.join()

    def update(self, df_key):
        print("[UPDATE]", df_key)
        df_data = self.dfs_data[df_key]
        data = df_data['df2data'](
            df_data['df_map'](
                CsvDataManager.url2df(df_data['csv_url'])
            )
        )
        self.cache.set(df_key, data, df_data['timeout_to_update'])
        self.keys_to_update.remove(df_key)

    def update_(self, df_key):
        data = [random.randint(0,10), random.randint(0,10), random.randint(0,10), random.randint(0,10)]
        self.cache.set(df_key, data, 12)        

    @staticmethod
    def url2df(url):
        print(f'[PD_READ_CSV_ONLINE] {url}')
        return pd.read_csv(url)


