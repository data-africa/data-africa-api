# -*- coding: utf-8 -*-
import os

''' Base directory of where the site is held '''
basedir = os.path.abspath(os.path.dirname(__file__))

''' CSRF (cross site forgery) for signing POST requests to server '''
CSRF_EN = True

''' Secret key should be set in environment var '''
SECRET_KEY = os.environ.get("DATA_AFRICA_SECRET_KEY", "default-da-secret")

''' Default debugging to False '''
DEBUG = False
SQLALCHEMY_ECHO = True
SQLALCHEMY_TRACK_MODIFICATIONS = False
SQLALCHEMY_DATABASE_URI = "postgres://{0}:{1}@{2}/{3}".format(
    os.environ.get("DATA_AFRICA_DB_USER", "postgres"),
    os.environ.get("DATA_AFRICA_DB_PW", ""),
    os.environ.get("DATA_AFRICA_DB_HOST", "127.0.0.1"),
    os.environ.get("DATA_AFRICA_DB_NAME", "postgres"))

''' If an env var for production is set turn off all debugging support '''
if "DATA_AFRICA_PRODUCTION" in os.environ:
    SQLALCHEMY_ECHO = False
    DEBUG = False
    ERROR_EMAIL = True

JSONIFY_PRETTYPRINT_REGULAR = False

CACHE_TYPE = 'filesystem'
CACHE_DIR = os.path.join(basedir, 'cache/')
CACHE_DEFAULT_TIMEOUT = os.environ.get("CACHE_DEFAULT_TIMEOUT", 60 * 60 * 24 * 7 * 4) # 28 days
CACHE_THRESHOLD = 5000
