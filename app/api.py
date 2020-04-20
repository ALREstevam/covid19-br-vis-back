from flask import Flask, jsonify, request
from flask.json import JSONEncoder

from datetime import time, datetime, date
from flask_cors import CORS

from app.Data import WcotaCsv

import markdown

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

flask_api = Flask(__name__)
CORS(flask_api)
flask_api.json_encoder = CustomJSONEncoder
md = markdown.Markdown()

# Solves city names without the correct string format due to the default accentuation representation
flask_api.config['JSON_AS_ASCII'] = False

wcota = WcotaCsv()

@flask_api.route("/", methods = ['GET'])
def main():
    return """
    <html>
    <p>Visit <code>/docs</code> for documentation.</p>
    </html>
    """

@flask_api.route("/docs", methods = ['GET'])
def docs():
    return get_docs()

@flask_api.route("/br/cities", methods = ['GET'])
def cities_cases():
    response_type = request.args.get('response_type')
    if response_type and response_type == 'geojson':
        return jsonify ( wcota.cases_geojson() )
    else:
        return jsonify( wcota.cases_data() )


@flask_api.route("/br/cities-daily", methods = ['GET'])
def cities_daily():
    response_type = request.args.get('response_type')
    if response_type and response_type=='geojson':
        return jsonify ( wcota.daily_geojson() )
    else:
        return jsonify( wcota.daily_data() )
