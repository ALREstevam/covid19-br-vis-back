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

app = Flask(__name__)
CORS(app)
app.json_encoder = CustomJSONEncoder
md = markdown.Markdown()

# Solves city names without the correct string format due to the default accentuation representation
app.config['JSON_AS_ASCII'] = False

wcota = WcotaCsv()

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

@app.route("/br/cities", methods = ['GET'])
def cities_cases():
    response_type = request.args.get('response_type')
    if response_type and response_type == 'geojson':
        return jsonify ( wcota.cases_geojson() )
    else:
        return jsonify( wcota.cases_data() )


@app.route("/br/cities-daily", methods = ['GET'])
def cities_daily():
    response_type = request.args.get('response_type')
    if response_type and response_type=='geojson':
        return jsonify ( wcota.daily_geojson() )
    else:
        return jsonify( wcota.daily_data() )