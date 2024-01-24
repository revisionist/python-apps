# Copyright 2023-2024 David Goddard.

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

from functools import wraps

from flask import g, request, abort, current_app
from werkzeug.local import LocalProxy

logger = LocalProxy(lambda: current_app.logger)


def require_appkey(view_function):
    @wraps(view_function)
    # the new, post-decoration function. Note *args and **kwargs here.
    def decorated_function(*args, **kwargs):
        if request.headers.get('x-api-key') and request.headers.get(
                'x-api-key') == current_app.config['API_KEY']:
            return view_function(*args, **kwargs)
        else:
            abort(401)
    return decorated_function


def get_is_auth(client_id, client_token):

    api_auth = current_app.config['API_AUTH']
    #logger.debug('Auth data: ' + repr(api_auth))
    #logger.debug('Client ID: ' + repr(client_id))
    auth_val = api_auth.get(client_id)
    #logger.debug('Auth val: ' + repr(auth_val))
    return auth_val == client_token


def require_api_auth(view_function):
    @wraps(view_function)
    def decorated_function(*args, **kwargs):

        client_id = request.args.get('client_id') or request.headers.get('x-client-id')
        client_token = request.args.get('client_token') or request.headers.get('x-client-token')

        if client_id and client_token and get_is_auth(client_id, client_token):
            #logger.debug(f"Authenticated client for URL {request.url} : {client_id}")
            g.client_id = client_id  # Store the validated client_id in Flask's g object
            return view_function(*args, **kwargs)
        else:
            logger.debug(f"Client not authenticated for URL {request.url} : {repr(client_id)} / {repr(client_token)}")
            #logger.debug(f"Request URL: {request.url}")
            #logger.debug(f"Request Headers: {request.headers}")
            abort(401)
    
    return decorated_function

