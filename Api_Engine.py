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

@app.route('/modulos', methods=['GET'])
def get_modulos_api():
    # temporal
    modulos = {'registro':'activo',
                'Engine':'activo',
                'Sensores':'activo',
                'Witing_server':'incativo',
                }
    return flask.jsonify(modulos)


if __name__ == "__main__":
    app.run(port=5000, debug=True)
    print("Test AAAAAAAAAAAAAAAAAAAaa")



