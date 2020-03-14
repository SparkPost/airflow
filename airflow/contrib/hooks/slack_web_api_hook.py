# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from slack import WebClient
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException


class SlackWebAPIHook(BaseHook):
    """
       Interact with Slack, using slackclient library.
    """

    def __init__(self, token=None, slack_conn_id=None, *args, **kwargs):
        """
        Takes both Slack API token directly and connection that has Slack API token.

        If both supplied, Slack API token will be used.

        :param token: Slack API token
        :type token: str
        :param slack_conn_id: connection that has Slack API token in the password field
        :type slack_conn_id: str
        """
        super(SlackWebAPIHook, self).__init__(*args, **kwargs)

        self.token = self.__get_token(token, slack_conn_id)

    def __get_token(self, token, slack_conn_id):
        if token is not None:
            self.log.info(f"Token: {token}")
            return token
        elif slack_conn_id is not None:
            conn = self.get_connection(slack_conn_id)

            if not getattr(conn, 'password', None):
                raise AirflowException('Missing token(password) in Slack connection')
            return conn.password
        else:
            raise AirflowException('Cannot get token: '
                                   'No valid Slack token nor slack_conn_id supplied.')

    def post_message(self, channel_id, text=None, blocks=None):
        """
        Post a Slack message.

        Either one of `text` or `blocks` must be provided.

        :param channel_id: Slack channel ID e.g. C1234567890
        :type channel_id: str
        :param text: Message text. (Optional)
        :type text: str
        :param blocks: Dictionary of blocks. (Optional)
        :type blocks: list
        """
        sc = WebClient(token=self.token)
        rc = sc.chat_postMessage(text=text, blocks=blocks, channel=channel_id)

        if not rc['ok']:
            msg = "Slack API call failed ({})".format(rc['error'])
            raise AirflowException(msg)

    def post_file(self, file_path, channel_id, title=None, comment=None):
        """
        Post a file to a Slack channel.

        :param file_path: Local path to file.
        :type file_path: str
        :param channel_id: Slack channel ID e.g. C1234567890
        :type channel_id: str
        :param title: File title (Optional)
        :type title: str
        :param comment: Associated message text. (Optional)
        :type comment: str
        """
        sc = WebClient(token=self.token)
        rc = sc.files_upload(file=file_path, title=title, initial_comment=comment, channels=channel_id)

        if not rc['ok']:
            msg = "Slack API call failed ({})".format(rc['error'])
            raise AirflowException(msg)

    def call(self, method, api_params):
        """ Directly make a call to Slacks Web API """
        sc = WebClient(token=self.token)
        rc = sc.api_call(method, **api_params)

        if not rc['ok']:
            msg = "Slack API call failed ({})".format(rc['error'])
            raise AirflowException(msg)
