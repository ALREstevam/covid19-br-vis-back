from flask import Flask, jsonify, request
from flask.json import JSONEncoder
from flask_cors import CORS
from datetime import time, datetime, date
import markdown
from app.DataGen import DataGen

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
app.config['JSON_AS_ASCII'] = False
md = markdown.Markdown()
data = DataGen().load_json_data()

# Solves city names without the correct string format due to the default accentuation representation

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
def cities_cases_json():
    return app.response_class(response=data.get_json('PYJSON-CASES-CITIES-TIME'),
                              status=200,
                              mimetype='application/json'
                              )


@app.route("/api/v1/br/cities.geojson", methods=['GET'])
def cities_cases_geojson():
    return app.response_class(response=data.get_json('PYGEOJSON-CASES-CITIES-TIME'),
                              status=200,
                              mimetype='application/json'
                              )


@app.route("/api/v1/br/cities-daily.json", methods=['GET'])
def cities_daily_json():
    return app.response_class(response=data.get_json('PYJSON-CASES-CITIES-TIME-CHANGESONLY'),
                              status=200,
                              mimetype='application/json'
                              )


@app.route("/api/v1/br/cities-daily.geojson", methods=['GET'])
def cities_daily_geojson():
    return app.response_class(response=data.get_json('PYGEOJSON-CASES-CITIES-TIME-CHANGESONLY'),
                              status=200,
                              mimetype='application/json'
                              )


@app.route("/api/v1/br/states.json", methods=['GET'])
def states_json():
    return app.response_class(response=data.get_json('PYJSON-CASES-STATES'),
                              status=200,
                              mimetype='application/json'
                              )


@app.route("/api/v1/br/states.geojson", methods=['GET'])
def states_geojson():
    return app.response_class(response=data.get_json('PYGEOJSON-CASES-STATES'),
                              status=200,
                              mimetype='application/json'
                              )
