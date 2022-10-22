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

import requests
from airflow.exceptions import AirflowException

from airflow.providers.apache.druid.hooks.druid import DruidHook


class CustomDruidReportHook(DruidHook):
    """
    [ CustomDruidReportHook ]
    : Connection to Druid Overlord for the ingestion task report
      - Gets the result of the druid task using druid task id and status.

    :param ingest_task_id: The task id of the ingestion task submitted to druid
    :param ingest_task_status: The task status of the ingestion task submitted to druid
    :param druid_conn_id: The connection id to the Druid overlord machine
                                 which gets the task report

    :author itzel.choi
    :date 2022-09-29
    """

    def __init__(
        self,
        ingest_task_id: str,
        ingest_task_status: str,
        druid_conn_id: str = 'druid_ingest_default',
    ) -> None:

        super().__init__()
        self.ingest_task_id = ingest_task_id
        self.ingest_task_status = ingest_task_status
        self.druid_conn_id = druid_conn_id
        self.header = {'content-type': 'application/json'}

    def get_conn_url(self) -> str:
        """
        Get Druid connection url for druid task report.

        e.g: druid://localhost:8082/druid/indexer/v1/task/{task_id}/reports
        cc. https://druid.apache.org/docs/latest/operations/api-reference.html#tasks
        """
        conn = self.get_connection(self.druid_conn_id)
        host = conn.host
        port = conn.port
        conn_type = 'http' if not conn.conn_type else conn.conn_type
        endpoint = conn.extra_dejson.get('endpoint', '/druid/indexer/v1/task')
        return f"{conn_type}://{host}:{port}/{endpoint}/{self.ingest_task_id}/reports"

    def get_task_report(self):
        """
        Get Druid Task Report
        """
        url = self.get_conn_url()

        req_report = requests.get(url, headers=self.header, auth=self.get_auth())
        if req_report.status_code != 200:
            raise AirflowException(f'Did not get 200 when getting the Druid task report to {url}')

        # Parse the druid task's report
        payload = req_report.json()['ingestionStatsAndErrors']['payload']
        error_msg = payload['errorMsg']

        processed_data = payload['rowStats']['buildSegments']['processed']
        processed_with_error = payload['rowStats']['buildSegments']['processedWithError']
        thrown_away_data = payload['rowStats']['buildSegments']['thrownAway']
        unparseable_data = payload['rowStats']['buildSegments']['unparseable']

        # Logging the druid task's report
        self.log.info("Druid indexing task-id: %s", self.ingest_task_id)
        self.log.info("Druid indexing task status: %s", self.ingest_task_status)
        if self.ingest_task_status == 'SUCCESS':
            self.log.info("============================= REPORT =============================")
            self.log.info(f"processed data: {processed_data} rows")
            self.log.info(f"processed with error: {processed_with_error} rows")
            self.log.info(f"thrown away data: {thrown_away_data} rows")
            self.log.info(f"unparseable data: {unparseable_data} rows")
            self.log.info("==================================================================")

            if processed_data == 0:
                self.log.warning("Druid ingest task is SUCCESS, but there is no data processed")
            else:
                self.log.warning(f"Druid ingest task is SUCCESS, {processed_data} rows data processed")

            if processed_with_error > 0:
                self.log.warning(f"Druid ingest task is SUCCESS, but Failed to process {processed_with_error} rows data with error")
            if unparseable_data > 0:
                self.log.warning(f"Druid ingest task is SUCCESS, but Failed to parse {unparseable_data} rows data")
        elif self.ingest_task_status == 'FAILED':
            # Logging task error message
            self.log.error(f"Druid ingest task is FAILED, check error message:\n {error_msg}")
        else:
            raise AirflowException(f'Could not get status of the job, got {self.ingest_task_status}')
