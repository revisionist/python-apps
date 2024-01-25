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
import sys
import time
import types
import os
import shutil
import sqlite3
import requests
import uuid
import inspect

from domestique.flask.request import get_reqjson
from domestique.flask.response import ResponseWrapper
from domestique.db import conn_rollback, conn_close, concat_sql
from domestique.db.sqlite import get_db_conn
from domestique.json import get_list_from_json_string
from domestique.identifiers import generate_id, generate_simple_random_identifier
from domestique.text import truncate_string, tidy_and_truncate_string
from domestique.rfc2822 import parse_rfc_address
from domestique.convert import str_to_bool

from flask import Blueprint, current_app, g, jsonify, request

from werkzeug.local import LocalProxy
from datetime import datetime, timedelta

from jsondiff import diff

from authentication import require_api_auth

from .shared_resources import logger, init_route, is_valid_name_string, get_valid_list_from_string 


objectstore_routes = Blueprint('objectstore_routes', __name__)

TABLE_OBJECT_STORE = 'objects'
TABLE_OBJECT_STORE_TAGS = 'objects_tags'
TABLE_OBJECT_STORE_MAPPING = 'objects_mapping'

mapping_cache = {}


def get_mapping_identifier(conn, mapping_table_name, client_id, namespace_id):

    def mapping_identifier_exists( identifier):

        logger.debug(f"Checking if mapping identifier already exists: {identifier}")
        cursor.execute(f"SELECT 1 FROM {mapping_table_name} WHERE identifier_name=?", (identifier,))
        exists = cursor.fetchone() is not None
        logger.debug(f"Returning: {exists}")
        return exists

    def cache_identifier(client_id, namespace_id, identifier):
        mapping_cache[(client_id, namespace_id)] = identifier

    def get_cached_identifier(client_id, namespace_id):
        return mapping_cache.get((client_id, namespace_id))

    identifier = get_cached_identifier(client_id, namespace_id)
    
    if identifier:
        logger.debug(f"Using existing identifier for '{client_id}/{namespace_id}': {identifier}")
        return identifier

    cursor = conn.cursor()

    cursor.execute(f"SELECT identifier_name FROM {mapping_table_name} WHERE client_id=? AND namespace_id=?", (client_id, namespace_id))
    row = cursor.fetchone()

    if not row:
        while True:
            new_identifier = generate_simple_random_identifier(6)
            if not mapping_identifier_exists(new_identifier):
                break

        cursor.execute(f"INSERT INTO {mapping_table_name} (client_id, namespace_id, identifier_name, timestamp) VALUES (?, ?, ?, ?)",
                       (client_id, namespace_id, new_identifier, str(datetime.now())))
        conn.commit()
        identifier = new_identifier
    else:
        identifier = row[0]

    cache_identifier(client_id, namespace_id, identifier)

    return identifier


def init_db(client_id, namespace_id=None, response_wrapper=None):

    conn = get_db_conn()

    conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {TABLE_OBJECT_STORE_MAPPING} (
                    client_id TEXT NOT NULL,
                    namespace_id TEXT NOT NULL,
                    identifier_name TEXT NOT NULL,
                    timestamp DATETIME,
                    PRIMARY KEY(client_id, namespace_id)
                )
            ''')

    if not namespace_id:
        return conn, None, None

    mapping_identifier = get_mapping_identifier(conn, TABLE_OBJECT_STORE_MAPPING, client_id, namespace_id)

    table_object_store = TABLE_OBJECT_STORE + "_" + mapping_identifier
    logger.debug(f"Using TABLE_OBJECT_STORE: {table_object_store}")

    conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_object_store} (
                    client_id TEXT NOT NULL,
                    namespace_id TEXT NOT NULL,
                    object_id TEXT NOT NULL,
                    revision_id TEXT NOT NULL,
                    object_json JSON,
                    object_tags JSON,
                    timestamp DATETIME,
                    PRIMARY KEY(client_id, namespace_id, object_id, revision_id)
                )
            ''')

    table_object_store_tags = TABLE_OBJECT_STORE_TAGS + "_" + mapping_identifier
    logger.debug(f"Using TABLE_OBJECT_STORE_TAGS: {table_object_store_tags}")

    conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_object_store_tags} (
                    client_id TEXT NOT NULL,
                    namespace_id TEXT NOT NULL,
                    object_id TEXT NOT NULL,
                    object_tag TEXT,
                    timestamp DATETIME,
                    PRIMARY KEY(client_id, namespace_id, object_id, object_tag)
                )
            ''')

    if response_wrapper:
        response_wrapper.add_meta("_mid", mapping_identifier)

    return conn, table_object_store, table_object_store_tags


def update_main_table_tags(cursor, client_id, namespace_id, object_id):

    conn, table_object_store, table_object_store_tags = init_db(client_id, namespace_id)

    cursor.execute(
        f'SELECT object_tag FROM {table_object_store_tags} WHERE client_id=? AND namespace_id=? AND object_id=?',
        (client_id, namespace_id, object_id)
    )
    updated_tags = [row[0] for row in cursor.fetchall()]
    cursor.execute(
        f'UPDATE {table_object_store} SET object_tags=? WHERE client_id=? AND namespace_id=? AND object_id=?',
        (json.dumps(updated_tags), client_id, namespace_id, object_id)
    )


@objectstore_routes.route('/<namespace_id>/<object_id>', methods=['GET'])
@objectstore_routes.route('/<namespace_id>/<object_id>/<object_prop>', methods=['GET'])
@objectstore_routes.route('/retrieve/<namespace_id>/<object_id>', methods=['GET'])
@objectstore_routes.route('/retrieve/<namespace_id>/<object_id>/<object_prop>', methods=['GET'])
@require_api_auth
def object_retrieve(namespace_id, object_id, object_prop=None):

    CLIENT_ID, resp, conn = init_route(request)

    revision_id = request.args.get('revision_id')
    tag = request.args.get('tag')

    logger.debug(f"CLIENT_ID: {CLIENT_ID}, resp: {resp.get_id()}, namespace_id: {namespace_id}, object_id: {object_id}, revision_id: {revision_id}, tag: {tag}")

    conn = None

    try:

        conn, table_object_store, table_object_store_tags = init_db(CLIENT_ID, namespace_id, resp)

        cursor = conn.cursor()

        query_select = "SELECT o.client_id, o.namespace_id, o.object_id, o.revision_id, o.object_json, o.object_tags, o.timestamp"
        query_from = f"FROM {table_object_store} o"
        query_where = "WHERE o.client_id=? AND o.namespace_id=? AND o.object_id=?"
        query_params = [CLIENT_ID, namespace_id, object_id]
        query_order = "ORDER BY o.timestamp DESC"

        if revision_id:
            query_where += " AND o.revision_id=?"
            query_params.append(revision_id)

        if tag:
            query_from += f"""
                INNER JOIN {table_object_store_tags} t
                 ON o.client_id = t.client_id
                 AND o.namespace_id = t.namespace_id
                 AND o.object_id = t.object_id
                """
            query_where += " AND t.object_tag=?"
            query_params.append(tag)

        query_combined = concat_sql([query_select, query_from, query_where, query_order])
        #logger.debug(f'Query: {query_combined} ~ {repr(query_params)}')

        cursor.execute(query_combined, query_params)

        row = cursor.fetchone()

        if not row:
            if revision_id:
                response_text = f"Object '{namespace_id}/{object_id}' not found with revision: {revision_id}"
            else:
                response_text = f"Object not found: {namespace_id}/{object_id}"
            return resp.generate_response_with_data(response_text, 404)

        cursor.execute(
            f'SELECT client_id, namespace_id, object_id, revision_id, timestamp FROM {table_object_store} WHERE client_id=? AND namespace_id=? AND object_id=? ORDER BY timestamp DESC',
            (CLIENT_ID, namespace_id, object_id,)
        )

        response_json = {
                'status': 'OK',
                'client_id': row["client_id"],
                'namespace_id': row["namespace_id"],
                'object_id': row["object_id"],
                'revision_id': row["revision_id"],
                'object_tags': get_list_from_json_string(row["object_tags"]),
                'object_timestamp': row["timestamp"],
                'object': json.loads(row["object_json"])
            }

        if object_prop and object_prop == "revisions":
            rows_revisions = cursor.fetchall()
            if rows_revisions:
                revisions = [{'revision_id': row_revision['revision_id'], 'timestamp': row_revision['timestamp']} for row_revision in rows_revisions]
                response_json["revisions"] = revisions

        if object_prop:
            if object_prop in response_json:
                resp.set_plain(response_json[object_prop], 200)
            else:
                resp.set_error(f"Property '{object_prop}' not valid", 400)
            return resp.generate_response(False)

        return resp.generate_response_with_data(response_json, 200)

    except Exception as e:

        return resp.generate_response_with_exception(e)

    finally:

        conn_close(conn)


@objectstore_routes.route('/<namespace_id>/<object_id>', methods=['POST'])
@objectstore_routes.route('/store/<namespace_id>', methods=['POST'])
@require_api_auth
def object_store(namespace_id, object_id=None):

    CLIENT_ID, resp, conn = init_route(request)

    try:

        reqjson = get_reqjson(request)

        if not object_id:
            object_id = request.args.get('object_id')
        raw_tags = request.args.get('tags')

        logger.debug(f"CLIENT_ID: {CLIENT_ID}, namespace_id: {namespace_id}, object_id: {object_id}")
        tags = get_valid_list_from_string(raw_tags)
        if raw_tags:
            logger.debug(f"Tags (raw): {raw_tags}")
            logger.debug(f"Tags: {tags}")

        if object_id:
            object_id_use = object_id
        else:
            object_id_use = generate_id()
        #logger.debug(f"Object ID (use): {object_id_use)}")

        if not namespace_id:
            raise ValueError("Missing required parameter: namespace_id")
        if not reqjson:
            raise ValueError("Missing required parameter: reqjson")

        object_json = None

        if isinstance(reqjson, dict):
            object_json = json.dumps(reqjson)
        elif isinstance(reqjson, str):
            try:
                object_json = json.loads(reqjson)
                object_json = json.dumps(object_json)  # Convert back to string if it's valid JSON
            except json.JSONDecodeError:
                raise ValueError(f"Invalid JSON string: {reqjson}")
        else:
            raise TypeError(f"Invalid JSON document")

        conn, table_object_store, table_object_store_tags = init_db(CLIENT_ID, namespace_id, resp)

        timestamp = str(datetime.now())
        return_timestamp = timestamp

        cursor = conn.cursor()

        new_version = False
        message_text = 'OK'

        cursor.execute(
            f'SELECT object_json, revision_id, timestamp FROM {table_object_store} WHERE client_id=? AND namespace_id=? AND object_id=? ORDER BY timestamp DESC',
            (CLIENT_ID, namespace_id, object_id_use)
        )
        row = cursor.fetchone()

        if row:
            stored_object_json = json.loads(row["object_json"])
            reqjson_dict = json.loads(object_json)  # Parse reqjson to a dictionary
            #logger.debug(f"stored_object_json:\n{json.dumps(stored_object_json)}")
            #logger.debug(f"reqjson_dict:\n{json.dumps(reqjson_dict)}")
            if stored_object_json != reqjson_dict:
                logger.debug('Existing stored object DIFFERS')
                new_version = True
            else:
                logger.debug('Existing stored object MATCHES')
                new_version = False
        else:
            logger.debug('No existing stored object')
            new_version = True

        if new_version:
            revision_id = generate_id()
            message_text = f"Document stored: {namespace_id}/{object_id_use} with new revision_id: {revision_id}"
            cursor.execute(
                f'INSERT INTO {table_object_store} (client_id, namespace_id, object_id, object_json, revision_id, timestamp) VALUES (?, ?, ?, ?, ?, ?)',
                (CLIENT_ID, namespace_id, object_id_use, object_json, revision_id, timestamp)
            )
        else:
            revision_id = row["revision_id"]
            message_text = f"Document exists: {namespace_id}/{object_id} with revision_id: {revision_id}"
            return_timestamp = row["timestamp"]

        if tags:
            for tag in tags:
                cursor.execute(
                    f'INSERT OR IGNORE INTO {table_object_store_tags} (client_id, namespace_id, object_id, object_tag, timestamp) VALUES (?, ?, ?, ?, ?)',
                    (CLIENT_ID, namespace_id, object_id, tag, timestamp)
                )
        
        update_main_table_tags(cursor, CLIENT_ID, namespace_id, object_id)

        conn.commit()

        response_json = {
            'status': 'OK',
            'message': message_text,
            'client_id': CLIENT_ID,
            'namespace_id': namespace_id,
            'object_id': object_id_use,
            'revision_id': revision_id,
            'new_version': new_version,
            'tags': tags,
            'object_timestamp': return_timestamp
        }

        return resp.generate_response_with_data(response_json, 200)

    except Exception as e:

        conn_rollback(conn)
        return resp.generate_response_with_exception(e)
        
    finally:

        conn_close(conn)


@objectstore_routes.route('/<namespace_id>/<object_id>', methods=['DELETE'])
@objectstore_routes.route('/delete/<namespace_id>/<object_id>', methods=['DELETE'])
@require_api_auth
def object_delete(namespace_id, object_id):

    CLIENT_ID, resp, conn = init_route(request)

    try:

        revision_id = request.args.get('revision_id')

        logger.debug(f"CLIENT_ID: {CLIENT_ID}, namespace_id: {namespace_id}, object_id: {object_id}, revision_id: {revision_id}")

        if not namespace_id:
            raise ValueError("Missing required parameter: namespace_id")
        if not object_id:
            raise ValueError("Missing required parameter: object_id")

        timestamp = str(datetime.now())

        conn, table_object_store, table_object_store_tags = init_db(CLIENT_ID, namespace_id, resp)

        cursor = conn.cursor()

        if revision_id:
            cursor.execute(
                f'SELECT 1 FROM {table_object_store} WHERE object_id=? AND revision_id=?',
                (object_id, revision_id)
            )
        else:
            cursor.execute(
                f'SELECT 1 FROM {table_object_store} WHERE object_id=?',
                (object_id,)
            )

        exists = cursor.fetchone()
        if not exists:
            if revision_id:
                response_text = f"Object '{namespace_id}/{object_id}' not found with revision: {revision_id}"
            else:
                response_text = f"Object not found: {object_id}"
            return resp.generate_response_with_data(response_text, 404)

        if revision_id:
            message_text = f"Object '{namespace_id}/{object_id}' deleted revision: {revision_id}"
            cursor.execute(
                f'DELETE FROM {table_object_store} WHERE object_id=? AND revision_id=?',
                (object_id, revision_id)
            )
        else:
            message_text = f"Object '{namespace_id}/{object_id}' deleted"
            cursor.execute(f"DELETE FROM {table_object_store} WHERE object_id=?", (object_id,))

        cursor.execute(
            f'SELECT 1 FROM {table_object_store} WHERE client_id=? AND namespace_id=? AND object_id=?',
            (CLIENT_ID, namespace_id, object_id)
        )
        remaining_object = cursor.fetchone()

        if not remaining_object:
            cursor.execute(
                f'DELETE FROM {table_object_store_tags} WHERE client_id=? AND namespace_id=? AND object_id=?',
                (CLIENT_ID, namespace_id, object_id)
            )

        conn.commit()

        response_json = {
            'status': 'OK',
            'message': message_text,
            'client_id': CLIENT_ID,
            'namespace_id': namespace_id,
            'object_id': object_id,
            'revision_id': revision_id
        }

        return resp.generate_response_with_data(response_json, 200)

    except Exception as e:

        conn_rollback(conn)
        return resp.generate_response_with_exception(e)

    finally:

        conn_close(conn)


@objectstore_routes.route('/<namespace_id>/<object_id>/revisions', methods=['GET'])
@objectstore_routes.route('/query/<namespace_id>/<object_id>', methods=['GET'])
@require_api_auth
def object_query(namespace_id, object_id):

    CLIENT_ID, resp, conn = init_route(request)

    try:

        logger.debug(f"CLIENT_ID: {CLIENT_ID}, namespace_id: {namespace_id}, object_id: {object_id}")

        conn, table_object_store, table_object_store_tags = init_db(CLIENT_ID, namespace_id, resp)

        cursor = conn.cursor()

        cursor.execute(
            f'SELECT client_id, namespace_id, object_id, revision_id, timestamp FROM {table_object_store} WHERE client_id=? AND namespace_id=? AND object_id=? ORDER BY timestamp DESC',
            (CLIENT_ID, namespace_id, object_id,)
        )

        rows = cursor.fetchall()

        if not rows:
            return resp.generate_response_with_data(f"Object not found: {namespace_id}/{object_id}", 404)

        response_data = [{'revision_id': row['revision_id'], 'timestamp': row['timestamp']} for row in rows]

        response_json = {
                'status': 'OK',
                'client_id': CLIENT_ID,
                'namespace_id': namespace_id,
                'object_id': object_id,
                'revisions': response_data
            }

        return resp.generate_response_with_data(response_json, 200)

    except Exception as e:

        return resp.generate_response_with_exception(e)

    finally:

        conn_close(conn)


@objectstore_routes.route('/tags/<namespace_id>/<object_id>', methods=['PATCH'])
@objectstore_routes.route('/tags/<namespace_id>/<object_id>', methods=['PUT'])
@objectstore_routes.route('/tags/add/<namespace_id>/<object_id>', methods=['POST'])
@require_api_auth
def add_tags_to_object(namespace_id, object_id):

    CLIENT_ID, resp, conn = init_route(request)

    try:

        raw_tags = request.args.get('tags')

        logger.debug(f"CLIENT_ID: {CLIENT_ID}, namespace_id: {namespace_id}, object_id: {object_id}")
        tags = get_valid_list_from_string(raw_tags)
        if raw_tags:
            logger.debug(f"Tags (raw): {raw_tags}")
            logger.debug(f"Tags: {tags}")

        if not namespace_id:
            raise ValueError("Missing required parameter: namespace_id")
        if not object_id:
            raise ValueError("Missing required parameter: object_id")
        if not tags:
            raise ValueError("Missing required parameter: tags")

        is_replace_tags = False
        if request.method == 'PUT':
            logger.debug(f"This is a PUT request, so removing existing tags first")
            is_replace_tags = True

        conn, table_object_store, table_object_store_tags = init_db(CLIENT_ID, namespace_id, resp)

        cursor = conn.cursor()

        timestamp = str(datetime.now())

        cursor.execute(
            f'SELECT 1 FROM {table_object_store} WHERE client_id=? AND namespace_id=? AND object_id=?',
            (CLIENT_ID, namespace_id, object_id)
        )
        if not cursor.fetchone():
            return resp.generate_response_with_data(f"Object not found: {namespace_id}/{object_id}", 404)

        if is_replace_tags:
            cursor.execute(
                f'DELETE FROM {table_object_store_tags} WHERE client_id=? AND namespace_id=? AND object_id=?',
                (CLIENT_ID, namespace_id, object_id)
            )
            message = f"Tags replaced successfully: {tags}"
        else:
            message = f"Tags added successfully: {tags}"

        for tag in tags:
            cursor.execute(
                f'INSERT OR IGNORE INTO {table_object_store_tags} (client_id, namespace_id, object_id, object_tag, timestamp) VALUES (?, ?, ?, ?, ?)',
                (CLIENT_ID, namespace_id, object_id, tag, timestamp)
            )

        update_main_table_tags(cursor, CLIENT_ID, namespace_id, object_id)

        conn.commit()

        return resp.generate_response_with_data(message, 200)

    except Exception as e:

        conn_rollback(conn)
        return resp.generate_response_with_exception(e)

    finally:

        conn_close(conn)


@objectstore_routes.route('/tags/<namespace_id>/<object_id>', methods=['DELETE'])
@objectstore_routes.route('/tags/remove/<namespace_id>/<object_id>', methods=['POST'])
@require_api_auth
def remove_tags_from_object(namespace_id, object_id):

    CLIENT_ID, resp, conn = init_route(request)

    try:

        raw_tags = request.args.get('tags')

        logger.debug(f"CLIENT_ID: {CLIENT_ID}, namespace_id: {namespace_id}, object_id: {object_id}")
        tags = get_valid_list_from_string(raw_tags)
        if raw_tags:
            logger.debug(f"Tags (raw): {raw_tags}")
            logger.debug(f"Tags: {tags}")

        if not namespace_id:
            raise ValueError("Missing required parameter: namespace_id")
        if not object_id:
            raise ValueError("Missing required parameter: object_id")

        conn, table_object_store, table_object_store_tags = init_db(CLIENT_ID, namespace_id, resp)

        cursor = conn.cursor()

        cursor.execute(
            f'SELECT 1 FROM {table_object_store} WHERE client_id=? AND namespace_id=? AND object_id=?',
            (CLIENT_ID, namespace_id, object_id)
        )
        if not cursor.fetchone():
            return resp.generate_response_with_data(f"Object not found: {namespace_id}/{object_id}", 404)

        if tags:
            logger.debug(f"Deleting specified tags: {tags}")
            for tag in tags:
                cursor.execute(
                    f'DELETE FROM {table_object_store_tags} WHERE client_id=? AND namespace_id=? AND object_id=? AND object_tag=?',
                    (CLIENT_ID, namespace_id, object_id, tag)
                )
            message = f"Tags removed successfully from {object_id}: {tags}"
        else:
            logger.debug(f"Deleting ALL tags")
            cursor.execute(
                f'DELETE FROM {table_object_store_tags} WHERE client_id=? AND namespace_id=? AND object_id=?',
                (CLIENT_ID, namespace_id, object_id)
            )
            message = f"All tags removed successfully from {object_id}"

        update_main_table_tags(cursor, CLIENT_ID, namespace_id, object_id)

        conn.commit()

        return resp.generate_response_with_data(message, 200)

    except Exception as e:

        conn_rollback(conn)
        return resp.generate_response_with_exception(e)

    finally:

        conn_close(conn)


@objectstore_routes.route('/tags/<namespace_id>/<object_id>', methods=['GET'])
@objectstore_routes.route('/tags/get/<namespace_id>/<object_id>', methods=['GET'])
@require_api_auth
def get_tags_of_object(namespace_id, object_id):

    CLIENT_ID, resp, conn = init_route(request)

    try:
        if not namespace_id:
            raise ValueError("Missing required parameter: namespace_id")
        if not object_id:
            raise ValueError("Missing required parameter: object_id")

        conn, table_object_store, table_object_store_tags = init_db(CLIENT_ID, namespace_id, resp)

        cursor = conn.cursor()

        cursor.execute(
            f'SELECT 1 FROM {table_object_store} WHERE client_id=? AND namespace_id=? AND object_id=?',
            (CLIENT_ID, namespace_id, object_id)
        )
        if not cursor.fetchone():
            return resp.generate_response_with_data(f"Object not found: {namespace_id}/{object_id}", 404)

        cursor.execute(
            f'SELECT object_tag FROM {table_object_store_tags} WHERE client_id=? AND namespace_id=? AND object_id=?',
            (CLIENT_ID, namespace_id, object_id)
        )
        tags = [row[0] for row in cursor.fetchall()]

        response_json = {
            'status': 'OK',
            'client_id': CLIENT_ID,
            'namespace_id': namespace_id,
            'object_id': object_id,
            'tags': []
        }

        if tags:
            response_json["tags"] = tags

        return resp.generate_response_with_data(response_json, 200)

    except Exception as e:

        return resp.generate_response_with_exception(e)

    finally:

        conn_close(conn)


@objectstore_routes.route('/<namespace_id>', methods=['GET'])
@objectstore_routes.route('/query/<namespace_id>', methods=['GET'])
@require_api_auth
def namespace_query(namespace_id):

    CLIENT_ID, resp, conn = init_route(request)

    revision_id = request.args.get('revision_id')
    tag = request.args.get('tag')

    logger.debug(f"CLIENT_ID: {CLIENT_ID}, namespace_id: {namespace_id}, tag: {tag}")

    try:

        conn, table_object_store, table_object_store_tags = init_db(CLIENT_ID, namespace_id, resp)

        cursor = conn.cursor()

        query_select = "SELECT DISTINCT o.client_id, o.namespace_id, o.object_id, o.object_tags"
        query_from = f"FROM {table_object_store} o"
        query_where = "WHERE o.client_id=? AND o.namespace_id=?"
        query_params = [CLIENT_ID, namespace_id]

        if tag:
            query_from += f"""
                INNER JOIN {table_object_store_tags} t
                 ON o.client_id = t.client_id
                 AND o.namespace_id = t.namespace_id
                 AND o.object_id = t.object_id
                """
            query_where += " AND t.object_tag=?"
            query_params.append(tag)

        query_combined = concat_sql([query_select, query_from, query_where])
        #logger.debug(f"Query: {query_combined} ~ {repr(query_params)}")

        cursor.execute(query_combined, query_params)

        rows = cursor.fetchall()

        object_ids = [row['object_id'] for row in rows]

        response_json = {
            'status': 'OK',
            'client_id': CLIENT_ID,
            'namespace_id': namespace_id,
            'object_ids': object_ids
        }

        return resp.generate_response_with_data(response_json, 200)

    except Exception as e:

        return resp.generate_response_with_exception(e)

    finally:

        conn_close(conn)


@objectstore_routes.route('/<namespace_id>', methods=['DELETE'])
@objectstore_routes.route('/clear/<namespace_id>', methods=['DELETE'])
@require_api_auth
def clear_namespace(namespace_id):

    CLIENT_ID, resp, conn = init_route(request)

    try:

        raw_tags = request.args.get('tags')

        is_confirm = request.args.get('confirm', type=str_to_bool, default=False)

        logger.debug(f"CLIENT_ID: {CLIENT_ID}, namespace_id: {namespace_id}")
        tags = get_valid_list_from_string(raw_tags)
        if raw_tags:
            logger.debug(f"Tags (raw): {raw_tags}")
            logger.debug(f"Tags: {tags}")

        if not is_confirm:
            return resp.generate_response_with_data("Missing required parameter: confirm=true", 400)

        conn, table_object_store, table_object_store_tags = init_db(CLIENT_ID, namespace_id, resp)

        cursor = conn.cursor()

        query_del_main = f"DELETE FROM {table_object_store}"
        query_del_tags = f"DELETE FROM {table_object_store_tags}"
        query_where = f"WHERE client_id=? AND namespace_id=?"
        query_params = [CLIENT_ID, namespace_id]

        if tags:

            # Extend main SQL to delete from TABLE_OBJECT_STORE using inner join to limit records to matching tags
            query_where +="""
                AND rowid IN
                    (SELECT o.rowid FROM objects o INNER JOIN objects_tags t
                       ON o.client_id = t.client_id
                       AND o.namespace_id = t.namespace_id
                       WHERE o.client_id = ? AND o.namespace_id = ?
                """
            placeholders = ', '.join(['?' for _ in tags])
            query_where += " AND t.object_tag IN ({}) )".format(placeholders)  # Note the extra closing ) for subselect!
            query_params.extend([CLIENT_ID, namespace_id])
            query_params.extend(tags)

            # Delete relevant records from TABLE_OBJECT_STORE_TAGS
            for tag in tags:
                del_sql_tags = f'DELETE FROM {table_object_store_tags} WHERE client_id=? AND namespace_id=? AND object_tag=?'
                logger.debug('Executing SQL (tags): ' + del_sql_tags)
                cursor.execute(del_sql_tags, (CLIENT_ID, namespace_id, tag))

        else:
        
            # Delete all matching records from TABLE_OBJECT_STORE_TAGS
            del_sql_all_tags = f'DELETE FROM {table_object_store_tags} WHERE client_id=? AND namespace_id=?'
            logger.debug('Executing SQL (all tags): ' + del_sql_all_tags)
            cursor.execute(del_sql_all_tags, (CLIENT_ID, namespace_id))

        # Execute main deletion query (deletes from TABLE_OBJECT_STORE only)
        del_sql = concat_sql([query_del_main, query_where])
        logger.debug('Executing SQL (main): ' + del_sql)
        logger.debug('Executing with params (main): ' + str(query_params))
        cursor.execute(del_sql, query_params)

        conn.commit()

        if tags:
            message = f"Namespace '{namespace_id}' cleared for tags: {tags}"
        else:
            message = f"Namespace '{namespace_id}' cleared for all tags"

        return resp.generate_response_with_data(message, 200)

    except Exception as e:

        conn_rollback(conn)
        return resp.generate_response_with_exception(e)

    finally:

        conn_close(conn)


@objectstore_routes.route('/mappings', methods=['GET'])
@require_api_auth
def get_mappings():

    CLIENT_ID, resp, conn = init_route(request)

    namespace_id = request.args.get('namespace_id')

    logger.debug(f"CLIENT_ID: {CLIENT_ID}, resp: {resp.get_id()}, namespace_id: {namespace_id}")

    conn = None

    try:

        conn, _, _ = init_db(CLIENT_ID, namespace_id)

        cursor = conn.cursor()

        query_select = "SELECT client_id, namespace_id, identifier_name, timestamp"
        query_from = f"FROM {TABLE_OBJECT_STORE_MAPPING}"
        query_where = ""
        query_params = []
        query_order = "ORDER BY client_id, namespace_id ASC"

        if namespace_id:
            query_where = "WHERE namespace_id=?"
            query_params.append(namespace_id)

        query_combined = concat_sql([query_select, query_from, query_where, query_order])

        cursor.execute(query_combined, query_params)

        rows = cursor.fetchall()

        if not rows:
            return resp.generate_response_with_data("No mappings found", 404)

        response_data = []

        for row in rows:
            response_item = {
                "client_id": row["client_id"],
                "namespace_id": row["namespace_id"],
                "identifier_name": row["identifier_name"],
                "timestamp": row["timestamp"]
            }
            response_data.append(response_item)

        response_json = {
                'status': 'OK',
                'client_id': CLIENT_ID,
                'data': response_data
        }

        return resp.generate_response_with_data(response_json, 200)

    except Exception as e:

        return resp.generate_response_with_exception(e)

    finally:

        conn_close(conn)
