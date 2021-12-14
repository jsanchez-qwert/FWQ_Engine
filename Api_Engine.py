import flask 
import json
import os
import sys
import requests
import time


# a simple flask server to serve the API
app = flask.Flask(__name__)
api = requests.session()


# simple funcion that retrun the map from file
def get_map():
    with open('mapa.txt') as f:
        data = json.load(f)
    return data

@app.route('/map', methods=['GET'])
def get_map_api():
    return flask.jsonify(get_map())

if __name__ == "__main__":
    app.run(port=5000, debug=True)



