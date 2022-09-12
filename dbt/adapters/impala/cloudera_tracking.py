# Copyright 2022 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import requests
import threading

from decouple import config

from dbt.events import AdapterLogger

# global logger
logger = AdapterLogger("Tracker")

# global switch to trun on/off the usage tracking
usage_tracking: bool = True


def track_usage(tracking_payload):
    """
    usage tracking code - Cloudera specific
    @param tracking_payload - list of key value pair of tracking data. Example:
            payload = {}
            payload["id"] = "dbt_impala_open"
            payload["unique_hash"] = hashlib.md5(credentials.host.encode()).hexdigest()
            payload["auth"] = auth_type
            payload["connection_state"] = connection.state
    """

    global usage_tracking

    logger.debug(f"Usage tracking flag {usage_tracking}")

    # if usage_tracking is disabled, quit
    if not usage_tracking:
        return

    # inject other static payload to tracking_payload

    # form the tracking data
    tracking_data = {}
    tracking_data["data"] = tracking_payload

    # inner function which actually calls the endpoint
    def _tracking_func(data):
        global usage_tracking

        try:
            SNOWPLOW_ENDPOINT = config("SNOWPLOW_ENDPOINT")
            SNOWPLOW_TIMEOUT = int(config("SNOWPLOW_TIMEOUT"))  # 10 seconds
            SNOWPLOW_API_KEY = config("SNOWPLOW_API_KEY")
            SNOWPLOW_ENV = config("SNOWPLOW_ENV")
        except Exception as err:
            logger.debug(f"Error reading tracking config. {err}")
            logger.debug("Disabling usage tracking due to error.")
            usage_tracking = False

        # prod creds
        headers = {
            "x-api-key": SNOWPLOW_API_KEY,
            "x-datacoral-environment": SNOWPLOW_ENV,
            "x-datacoral-passthrough": "true",
        }

        data = json.dumps([data])

        res = None

        try:
            logger.debug(f"Sending Event {data}")
            res = requests.post(
                SNOWPLOW_ENDPOINT, data=data, headers=headers, timeout=SNOWPLOW_TIMEOUT
            )
        except Exception as err:
            logger.debug(f"Usage tracking error. {err}")
            logger.debug("Disabling usage tracking due to error.")
            usage_tracking = False

        return res

    # call the tracking function in a Thread
    the_track_thread = threading.Thread(
        target=_tracking_func, kwargs={"data": tracking_data}
    )
    the_track_thread.start()
