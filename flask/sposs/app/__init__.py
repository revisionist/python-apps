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

import logging.config
from os import environ

#import sys
#import pkg_resources
#print(sys.path)
#print(list(pkg_resources.working_set))
#print(sys.modules.keys())

from celery import Celery
from dotenv import load_dotenv
from flask import Flask
from flask_cors import CORS

from domestique.db import sqlite


from .config import config as app_config

celery = Celery(__name__)


def create_app():

    load_dotenv()
    APPLICATION_ENV = get_environment()

    logging.config.dictConfig(app_config[APPLICATION_ENV].LOGGING)

    app = Flask(app_config[APPLICATION_ENV].APP_NAME)

    app.config["JSON_SORT_KEYS"] = False
    app.config.from_object(app_config[APPLICATION_ENV])
    #app.logger.addHandler(logging.handlers.RotatingFileHandler(app_config[APPLICATION_ENV].LOG_INFO_FILE, maxBytes=16777216, backupCount=5))

    db_file_path = app.config['SQLITE_DATABASE_FILE']
    sqlite.configure_db(db_file_path)

    CORS(app, resources={r'/svc/*': {'origins': '*'}})

    celery.config_from_object(app.config, force=True)
    # celery is not able to pick result_backend and hence using update
    celery.conf.update(result_backend=app.config['RESULT_BACKEND'])

    from .svc.v1 import svc_v1 as svc_v1_blueprint
    app.register_blueprint(
        svc_v1_blueprint,
        url_prefix='/svc/v1'
    )

    return app


def get_environment():
    return environ.get('APPLICATION_ENV') or 'development'
