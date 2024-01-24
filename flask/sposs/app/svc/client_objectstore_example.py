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

import json
import requests

from domestique.json import get_json_str_from_dict_or_str
from domestique.flask.response import check_response_status
from domestique.text import truncate_string, tidy_and_truncate_string

from .shared_resources import logger


class ObjectstoreClient:

    def __init__(self, rest_base_url, client_id, client_token):

        logger.debug(f"Initialising new ObjectstoreClient with client_id '{client_id}' and URL: {rest_base_url}")

        if not rest_base_url:
            raise ValueError("rest_base_url required")
        if not client_id:
            raise ValueError("client_id required")
        if not client_token:
            raise ValueError("client_token required")

        self.rest_base_url = rest_base_url
        self.client_id = client_id
        self.client_token = client_token

        self.web_service_headers = {
            "Content-Type": "application/json",
            "x-client-id": client_id,
            "x-client-token": client_token,
        }


    def get_ws_url_objectstore_store(self, namespace):

        return f"{self.rest_base_url}/{namespace}"


    def get_ws_url_objectstore_retrieve(self, namespace):

        return f"{self.rest_base_url}/{namespace}"


    def get_ws_url_objectstore_query_namespace(self, namespace):

        return f"{self.rest_base_url}/{namespace}"


    def query_namespace(self, namespace_id, tag):

        logger.debug(f"Querying namespace '{namespace_id}' with tag: {tag}")

        if not namespace_id:
            raise Exception("Bad namespace passed to objectstore_query_namespace")

        params = {}
        if (tag):
            params["tag"] = tag
        url = f"{self.get_ws_url_objectstore_query_namespace(namespace_id)}"

        logger.debug(f"Calling: {url}")
        try:
            response = requests.get(url, headers=self.web_service_headers, params=params, timeout=30)
            #logger.debug(f"Object store response: \n{response.text}")
        except requests.exceptions.Timeout:
            raise Exception("Request timed out calling: " + url)
        check_response_status(response, url)

        response_json = response.json()
        response_namespace_id = response_json.get("namespace_id")
        response_object_ids = response_json.get("object_ids", [])
        logger.debug(f"Queried namespace '{namespace_id}' with tag '{tag}' and got objects: {response_object_ids}")

        return response_object_ids


    def store_object(self, namespace_id, object_id, tags, object_to_store):

        if object_id:
            logger.debug(f"Storing object in namespace '{namespace_id}' using object_id: {object_id}")
        else:
            logger.debug(f"Storing object in namespace '{namespace_id}' - no object_id")
        #logger.debug(f"Object is:\n{object_dict}")

        object_json = get_json_str_from_dict_or_str(object_to_store)

        params = {}
        if object_id:
            params['object_id'] = object_id
        if tags:
            params['tags'] = tags

        url = self.get_ws_url_objectstore_store(namespace_id)

        logger.debug(f"Calling: {url}")
        try:
            response = requests.post(url, headers=self.web_service_headers, params=params, json=object_json, timeout=30)
            #logger.debug(f"Object store response: \n{response.text}")
        except requests.exceptions.Timeout:
            raise Exception("Request timed out calling: " + url)
        check_response_status(response, url)

        response_json = response.json()
        response_object_id = response_json.get("object_id")
        response_revision_id = response_json.get("revision_id")
        response_new_version = response_json.get("new_version")
        response_object_timestamp = response_json.get("object_timestamp")
        if response_new_version:
            log_prefix = "Stored NEW/UPDATED object"
        else:
            log_prefix = "Found EXISTING object"
        logger.debug(f"{log_prefix} '{namespace_id}/{response_object_id}' with revision_id '{response_revision_id}' and timestamp: {response_object_timestamp}")

        return response_object_id


    def retrieve_object(self, namespace_id, object_id):

        logger.debug(f"Retrieving object '{object_id}' from namespace: {namespace_id}")

        if not namespace_id or not object_id:
            raise Exception("Bad namespace or object_id passed to retrieve_object")

        params = {}
        url = f"{self.get_ws_url_objectstore_retrieve(namespace_id)}/{object_id}"

        logger.debug(f"Calling: {url}")
        try:
            response = requests.get(url, headers=self.web_service_headers, params=params, timeout=30)
            #logger.debug(f"Object store response: \n{response.text}")
        except requests.exceptions.Timeout:
            raise Exception("Request timed out calling: " + url)
        check_response_status(response, url)

        response_json = response.json()
        response_object_id = response_json.get("object_id")
        response_revision_id = response_json.get("revision_id")
        response_object_timestamp = response_json.get("object_timestamp")
        response_object = response_json.get("object")
        logger.debug(f"Retrieved object '{namespace_id}/{response_object_id}' with revision_id '{response_revision_id}' and timestamp: {response_object_timestamp}")
        #logger.debug(f"Object: \n{response_object}")

        return response_object

