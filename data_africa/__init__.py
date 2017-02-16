import os
from flask import Flask, jsonify
from flask_cache import Cache
from flask_cors import CORS

app = Flask(__name__)
app.config.from_object('config')
cache = Cache(app)

from data_africa.attrs.views import mod as attrs_module
from data_africa.core.views import mod as core_module

app.register_blueprint(attrs_module)
app.register_blueprint(core_module)

CORS(app)

@app.errorhandler(500)
def error_page(err):
    return jsonify(error=str(err)), 500
