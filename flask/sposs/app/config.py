# Copyright 2023-2024 David Goddard.
#
# Except where otherwise noted, this software is licensed under the Apache
# License, Version 2.0 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at:
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# This file includes code based on boilerplate from Idris Rampurawala,
# originally under the MIT License. Original boilerplate code Copyright 2020
# by Idris Rampurawala. The full text of the MIT License for the original
# boilerplate code can be found in the accompanying file named
# 'LICENSE-MIT.txt' or at https://opensource.org/licenses/MIT.

import json

from os import environ, path

from dotenv import load_dotenv

basedir = path.abspath(path.join(path.dirname(__file__), '..'))
# loading env vars from .env file
load_dotenv()


class BaseConfig(object):
    ''' Base config class. '''

    APP_NAME = environ.get('APP_NAME') or 'sposs'

    api_auth_str = environ.get('API_AUTH', '{}')
    API_AUTH = json.loads(api_auth_str)

    ORIGINS = ['*']
    EMAIL_CHARSET = 'UTF-8'
    API_KEY = environ.get('API_KEY')
    BROKER_URL = environ.get('BROKER_URL') or 'redis://localhost:6379'
    RESULT_BACKEND = environ.get('RESULT_BACKEND') or 'redis://localhost:6379'

    LOG_INFO_FILE = environ.get('SPOSS_LOG') or path.join(basedir, 'log', 'info.log')
    LOG_CELERY_FILE = environ.get('SPOSS_CELERY_LOG') or path.join(basedir, 'log', 'celery.log')
    
    LOGGING = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '[%(asctime)s] - %(levelname)s - %(module)s - %(funcName)s - '
                '%(message)s',
                'datefmt': '%b %d %Y %H:%M:%S'
            },
            'aligned': {
                'format': '[%(asctime)s] [%(module)18s] [%(funcName)10s:%(lineno)s] --- %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S'
            },
            'simple': {
                'format': '%(module)s - %(funcName)s - %(message)s'
            },
        },
        'handlers': {
            'console': {
                'level': 'INFO',
                'class': 'logging.StreamHandler',
                'formatter': 'simple'
            },
            'log_info_file': {
                'level': 'INFO',
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': LOG_INFO_FILE,
                'maxBytes': 16777216,  # 16megabytes
                'formatter': 'aligned',
                'backupCount': 5
            },
        },
        'loggers': {
            '': {  # This is the root logger
                'handlers': ['console'],
                'level': 'WARN',
                'propagate': True
            },
            APP_NAME: {
                'level': 'DEBUG',
                'handlers': ['log_info_file'],
                'propagate': False
            },
            'domestique': {
                'handlers': ['log_info_file'],
                'level': 'INFO',
                'propagate': False
            },
        }
    }

    CELERY_LOGGING = {
        'format': '[%(asctime)s] - %(name)s - %(levelname)s - '
        '%(message)s',
        'datefmt': '%b %d %Y %H:%M:%S',
        'filename': LOG_CELERY_FILE,
        'maxBytes': 10000000,  # 10megabytes
        'backupCount': 5
    }

    SQLITE_DATABASE_FILE = environ.get('SQLITE_DATABASE_FILE') or '/data/awag/db/awag_sqlite.db'


class Development(BaseConfig):
    ''' Development config. '''

    DEBUG = True
    ENV = 'dev'


class Staging(BaseConfig):
    ''' Staging config. '''

    DEBUG = True
    ENV = 'staging'


class Production(BaseConfig):
    ''' Production config '''

    DEBUG = False
    ENV = 'production'


config = {
    'development': Development,
    'staging': Staging,
    'production': Production,
}
