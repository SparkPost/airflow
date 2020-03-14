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
#
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.slack_web_api_hook import SlackWebAPIHook


class SlackWebAPIFileUploadOperator(BaseOperator):
    """
    This operator allows you to post a file to Slack using the Slack API.
    Takes both Slack API token directly and connection that has Slack API token.

    :param slack_conn_id: connection that has Slack API token in the extra field
    :type slack_conn_id: str
    :param api_token: Slack API token
    :type api_token: str
    :param file_path: Local path to file
    :type file_path: str
    :param title: File title (Optional)
    :type title: str
    :param channel: The channel the message should be posted to
    :type channel: str
    :param comment: Associated message text (Optional)
    :type comment: str
    """

    template_fields = ['slack_conn_id', 'api_token', 'file_path',
                       'title', 'channel', 'comment', 'source']

    @apply_defaults
    def __init__(self,
                 slack_conn_id=None,
                 api_token=None,
                 file_path="",
                 title="",
                 channel=None,
                 comment="",
                 source=None,
                 *args, **kwargs):
        super(SlackWebAPIFileUploadOperator, self).__init__(*args, **kwargs)
        self.slack_conn_id = slack_conn_id
        self.api_token = api_token
        self.file_path = file_path
        self.title = title
        self.channel = channel
        self.comment = comment
        self.source = source
        self.hook = None

    def execute(self, context):
        """
        Call the SlackWebAPIHook to post the provided Slack message
        """
        self.hook = SlackWebAPIHook(
            token=self.api_token,
            slack_conn_id=self.slack_conn_id,
            source=self.source
        )
        self.hook.post_file(channel_id=self.channel,
                            file_path=self.file_path,
                            title=self.title,
                            comment=self.comment)
