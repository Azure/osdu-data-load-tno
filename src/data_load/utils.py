#  Copyright © Microsoft Corporation
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
import os
import sys

from os import environ
from configparser import ConfigParser, RawConfigParser
from datetime import datetime
from json import loads
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

logger = logging.getLogger("Token manager")

class ClassProperty:
    """
    Decorator that allows get methods like class properties.
    """

    def __init__(self, fget):
        self.fget = fget

    def __get__(self, owner_self, owner_cls):
        return self.fget(owner_cls)



classproperty = ClassProperty # pylint: disable=invalid-name


class TokenManager:
    """
    Class for token managing.

    Simple usage:
    print(TokenManager.id_token)
    print(TokenManager.access_token)

    Requires dataload.ini with:
    [CONNECTION]
    token_endpoint = <token_endpoint_url>
    retries = <retries_count>

    Requires "REFRESH_TOKEN", "CLIENT_ID", "CLIENT_SECRET" in environment variables
    """
    _config = ConfigParser()
    _config.read("output/dataload.ini")
    expire_date = 0

    try:
        _retries = _config.getint("CONNECTION", "retries")
        _token_endpoint = _config["CONNECTION"]["token_endpoint"]
    except KeyError as e:
        logger.error('%s should be in dataload.ini', {e.args[0]})
        sys.exit(0)

    try:
        _client_id = os.environ["CLIENT_ID"]
        _client_secret = os.environ["CLIENT_SECRET"]
    except KeyError as e:
        logger.error('Environment should have variable %s', {e.args[0]})
        sys.exit(0)

    @classproperty
    def access_token(self):
        """
        Check expiration date and return access_token.
        """
        if datetime.now().timestamp() > self.expire_date:
            self.refresh()

        return self._access_token

    @classmethod
    def refresh(cls):
        """
        Refresh token and save them into class.
        """
        logger.info("Refreshing token.")

        for i in range(cls._retries):
            # try several times if there any error
            try:
                resp = cls.login_with_service_principal_creds(cls._token_endpoint, cls._client_id, cls._client_secret)

                if 'access_token' in resp:
                    break

            except HTTPError:
                if i == cls._retries - 1:
                    # too many errors, raise original exception
                    raise

        cls._access_token = resp["access_token"]
        cls.expire_date = datetime.now().timestamp() + int(resp["expires_in"])

        logger.info("Token is refreshed.")

    @staticmethod
    def login_with_service_principal_creds(url: str, client_id: str, client_secret: str) -> dict:
        """
        Retrieve Access Token using Service Principal.
        """


        logger.info("Using Service principal credentials")

        body = {
            "grant_type": "client_credentials",
            "scope": f"{client_id}/.default openid profile offline_access",
            "client_id": client_id,
            "client_secret": client_secret,
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        data = urlencode(body).encode("utf8")
        request = Request(url=url, data=data, headers=headers)
        try:
            response = urlopen(request)
            response_body = response.read()
            return loads(response_body)
        except HTTPError as err:
            code = err.code
            message = err.read().decode("utf8")
            logger.error("Login to fetch access token request failed. %d %s", {code}, {message})
            raise


    @staticmethod
    def refresh_request(url: str, refresh_token: str, client_id: str, client_secret: str) -> dict:
        """
        Retrieve Access Token using Refresh.
        """

        logger.info("Refresh Token")

        body = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = urlencode(body).encode("utf8")
        request = Request(url=url, data=data, headers=headers)
        try:
            response = urlopen(request)
            response_body = response.read()
            return loads(response_body)
        except HTTPError as err:
            code = err.code
            message = err.read().decode("utf8")
            logger.error("Refresh token request failed. %d %s", {code}, {message})
            raise


def get_headers(config: RawConfigParser) -> dict:
    """
    Get request headers.

    :param RawConfigParser config: config that is used in calling module
    :return: dictionary with headers required for requests
    :rtype: dict
    """

    timestamp = datetime.now().strftime('%m%d-%H%M%S')
    correlation_id = f'workflow-create-{timestamp}'

    return {
        "Content-Type": "application/json",
        "data-partition-id": config.get("CONNECTION", "data-partition-id"),
        "Authorization": f"Bearer {TokenManager.access_token}",
        "correlation-id": correlation_id
    }
