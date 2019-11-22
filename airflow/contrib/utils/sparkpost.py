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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import base64
import mimetypes
import os

from sparkpost import SparkPost
from sparkpost.exceptions import SparkPostAPIException

from airflow.utils.email import get_email_address_list
from airflow.utils.log.logging_mixin import LoggingMixin


def send_email(to, subject, html_content, files=None,
               dryrun=False, cc=None, bcc=None,
               mime_subtype='mixed', **kwargs):
    """
    Send an email with html content using SparkPost.

    To use this plugin:
    0. include then SparkPost subpackage as part of your Airflow installation, e.g.,
    pip install 'apache-airflow[sparkpost]'
    1. update [email] backend in airflow.cfg, i.e.,
    [email]
    email_backend = airflow.contrib.utils.sparkpost.send_email
    2. configure SparkPost specific environment variables at all Airflow instances:
    SPARKPOST_FROM_EMAIL={your-mail-from}
    SPARKPOST_API_KEY={your-sparkpost-api-key}.
    """
    mail = SparkPost()
    log = LoggingMixin().log

    from_email = kwargs.get('from_email') or os.environ.get('SPARKPOST_FROM_EMAIL')
    if not from_email:
        log.warning('SPARKPOST_FROM_EMAIL is not set in environment. Sending email will fail.')
    from_name = kwargs.get('from_name') or os.environ.get('SPARKPOST_FROM_NAME')
    from_email_name = "{} <{}>".format(from_name or "", from_email).strip()

    # Add the recipient list of to emails.
    to = get_email_address_list(to)
    if cc:
        cc = get_email_address_list(cc)
    if bcc:
        bcc = get_email_address_list(bcc)

    attachment_list = []
    # Add email attachment.
    for fname in files or []:
        basename = os.path.basename(fname)
        with open(fname, "rb") as f:
            attachment_data = str(base64.b64encode(f.read()), 'utf-8')
            attachment_type = mimetypes.guess_type(basename)[0] or "application/octet-stream"
            attachment_name = basename
            attachment = dict(
                data=attachment_data,
                type=attachment_type,
                name=attachment_name,

            )
            attachment_list.append(attachment)

    try:
        result = mail.transmission.send(recipients=to,
                                        cc=cc,
                                        bcc=bcc,
                                        html=html_content,
                                        subject=subject,
                                        from_email=from_email_name,
                                        return_path=from_email,
                                        attachments=attachment_list
                                        )
        log.info('Email with subject {} and transmission_id {} successfully sent to recipients: {}'.format(subject,
                                                                                                           result["id"],
                                                                                                           to))
    except SparkPostAPIException as e:
        log.warning('Failed to send out email with subject {}. Status code: {}'.format(subject, e))
