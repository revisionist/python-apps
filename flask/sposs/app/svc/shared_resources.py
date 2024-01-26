# Copyright 2023-2024 David Goddard.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain a
# copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import sys
import types
import re
import sqlite3
import json
import os

from flask import current_app, make_response, g
from werkzeug.local import LocalProxy

from domestique.logging import log_exception
from domestique.flask.session import Session
from domestique.validation import Validator, NoiseLevel


logger = LocalProxy(lambda: current_app.logger)


VALID_NAME_REGEX = re.compile(r'^[a-zA-Z0-9:+\-_/~#]*$')


def init_route(request, calling_method_text=None):

    client_id = g.client_id
    resp = None
    conn = None
    
    # The value of validator_noise_level determines the log-level of emitted messages
    # - so passing DEBUG is quiter than INFO, because it will only emit debug-level
    # messages (unless it finds an error)
    #validator_noise_level=NoiseLevel.INFO
    validator_noise_level=NoiseLevel.DEBUG
    #validator_noise_level=NoiseLevel.SILENT

    session = Session(g.client_id, request, calling_method_text, validator_noise_level=validator_noise_level)
    session.log_info()

    return client_id, session


def is_valid_name_string(val):

    return VALID_NAME_REGEX.match(val) is not None


def get_valid_list_from_string(raw_string):

    logger.debug(f'Method get_valid_list_from_string: {raw_string}')

    if not raw_string:
        return None

    parsed_strings = None

    if isinstance(raw_string, str):
        # Check if the string is JSON formatted
        if raw_string.startswith('[') and raw_string.endswith(']'):
            try:
                json_to_parse = raw_string.replace("'", '"')
                parsed_strings = json.loads(json_to_parse)
            except json.JSONDecodeError:
                raise ValueError(f'Unable to parse JSON string: {raw_string}')
        else:
            # Handle as a comma-separated string
            parsed_strings = [s.strip() for s in raw_string.split(',') if s.strip()]

    elif isinstance(raw_string, list):
        parsed_strings = raw_string

    # Validate each string in the parsed list
    if parsed_strings is not None:
        for string in parsed_strings:
            if not is_valid_name_string(string):
                logger.error(f'Got bad string: {string} in strings input: {raw_string}')
                raise ValueError(f'Invalid string found: {string}')

    return parsed_strings

