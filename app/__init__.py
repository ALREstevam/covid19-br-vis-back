from flask import Flask, jsonify, request
from flask.json import JSONEncoder
from flask_cors import CORS
from flask_caching import Cache
from datetime import time, datetime, date
import markdown

from app.Data2 import DataGenerator


def get_docs():
    md = markdown.Markdown(extensions=['extra', 'fenced_code', 'codehilite', 'nl2br'])
    with open('./app/static/docs.md') as doc_md:
        return md.convert(doc_md.read())

class CustomJSONEncoder(JSONEncoder):
    '''
    This custom JSON encoder will convert python's date objects as ISO strings (more suitable for use with JavaScript)
    '''
    def default(self, obj):
        try:
            if isinstance(obj, date):
                return obj.isoformat()
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self, obj)

app = Flask(__name__)


CORS(app)
app.json_encoder = CustomJSONEncoder
md = markdown.Markdown()
cache = Cache(app, config={'CACHE_TYPE': 'simple'})

# Solves city names without the correct string format due to the default accentuation representation
app.config['JSON_AS_ASCII'] = False

dg = DataGenerator(shelf='data', save_path='./app/static')

dg.load(dg.WCOTA['CHANGES_ONLY']['JSON']['KEY'])
dg.load(dg.WCOTA['CHANGES_ONLY']['GEOJSON']['KEY']) 
dg.load(dg.WCOTA['CITIES_TIME']['JSON']['KEY'])
dg.load(dg.WCOTA['CITIES_TIME']['GEOJSON']['KEY'])

@app.route("/", methods = ['GET'])

def main():
    return """
    <html>
    <p>Visit <code>/docs</code> for documentation.</p>
    </html>
    """

@app.route("/docs", methods = ['GET'])
def docs():
    return get_docs()

@app.route("/api/v1/br/cities.json", methods = ['GET'])
@cache.memoize(timeout=60*60*3)
def cities_cases_json():
    return jsonify( dg.loaded[dg.WCOTA['CHANGES_ONLY']['JSON']['KEY']] )

@app.route("/api/v1/br/cities.geojson", methods = ['GET'])
@cache.memoize(timeout=60*60*3)
def cities_cases_geojson():
    return jsonify( dg.loaded[dg.WCOTA['CHANGES_ONLY']['GEOJSON']['KEY']] )


@app.route("/api/v1/br/cities-daily.json", methods = ['GET'])
@cache.memoize(timeout=60*60*3)
def cities_daily_json():
    return jsonify( dg.loaded[dg.WCOTA['CITIES_TIME']['JSON']['KEY']] )

@app.route("/api/v1/br/cities-daily.geojson", methods = ['GET'])
@cache.memoize(timeout=60*60*3)
def cities_daily_geojson():
    return jsonify ( dg.loaded[dg.WCOTA['CITIES_TIME']['GEOJSON']['KEY']]  )


