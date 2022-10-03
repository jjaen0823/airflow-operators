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

import time
from typing import Any, Dict, Optional, Union

import requests
from airflow.exceptions import AirflowException
from airflow.providers.apache.druid.hooks.druid import DruidHook


class CustomDruidHook(DruidHook):
    """
    [ CustomDruidHook ]
    1. Submit Druid ingestion job and Save Druid task id in airflow Xcom.
      - Save the druid task id in airflow Xcom to receive the result information of the task.
      - The druid task id is used by the CustomDruidTaskReportOperator.
    2. Connection to Druid Overlord for the ingestion task report
      - Gets the result of the druid task using druid task id and status.

    :param druid_ingest_conn_id: The connection id to the Druid overlord machine
                                 which accepts index jobs
    :param timeout: The interval between polling
                    the Druid job for the status of the ingestion job.
                    Must be greater than or equal to 1
    :param max_ingestion_time: The maximum ingestion time before assuming the job failed

    :author itzel.choi
    :date 2022-09-29
    """

    def __init__(
        self,
        druid_ingest_conn_id: str = 'druid_ingest_default',
        timeout: int = 1,
        max_ingestion_time: Optional[int] = None,
    ) -> None:
        super().__init__(druid_ingest_conn_id, timeout, max_ingestion_time)
        self.druid_ingest_conn_id = druid_ingest_conn_id
        self.timeout = timeout
        self.max_ingestion_time = max_ingestion_time
        self.header = {'content-type': 'application/json'}

        if self.timeout < 1:
            raise ValueError("Druid timeout should be equal or greater than 1")

    def submit_indexing_job(self, json_index_spec: Union[Dict[str, Any], str], **kwargs) -> None:
        """Submit Druid ingestion job"""
        url = self.get_conn_url()

        self.log.info("Druid ingestion spec: %s", json_index_spec)
        req_index = requests.post(url, data=json_index_spec, headers=self.header, auth=self.get_auth())
        if req_index.status_code != 200:
            raise AirflowException(f'Did not get 200 when submitting the Druid job to {url}')

        req_json = req_index.json()
        # Wait until the job is completed
        druid_task_id = req_json['task']
        kwargs['ti'].xcom_push("druid_task_id", druid_task_id)
        self.log.info("Druid indexing task-id: %s", druid_task_id)

        running = True

        sec = 0
        while running:
            req_status = requests.get(f"{url}/{druid_task_id}/status", auth=self.get_auth())

            self.log.info("Job still running for %s seconds...", sec)

            if self.max_ingestion_time and sec > self.max_ingestion_time:
                # ensure that the job gets killed if the max ingestion time is exceeded
                requests.post(f"{url}/{druid_task_id}/shutdown", auth=self.get_auth())
                raise AirflowException(f'Druid ingestion took more than {self.max_ingestion_time} seconds')

            time.sleep(self.timeout)

            sec += self.timeout

            status = req_status.json()['status']['status']
            kwargs['ti'].xcom_push("druid_task_status", status)
            if status == 'RUNNING':
                running = True
            elif status == 'SUCCESS':
                running = False  # Great success!
            elif status == 'FAILED':
                raise AirflowException('Druid indexing job failed, check "druid_task_report" for more error message')
            else:
                raise AirflowException(f'Could not get status of the job, got {status}')

        self.log.info('Successful index')

    def get_task_report(self, ingest_task_id, ingest_task_status):
        """
        Get Druid Task Report
        """
        url = self.get_conn_url() + f"/{ingest_task_id}/reports"

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
        self.log.info("Druid indexing task-id: %s", ingest_task_id)
        self.log.info("Druid indexing task status: %s", ingest_task_status)
        if ingest_task_status == 'SUCCESS':
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
                self.log.warning(
                    f"Druid ingest task is SUCCESS, but Failed to process {processed_with_error} rows data with error")
            if unparseable_data > 0:
                self.log.warning(f"Druid ingest task is SUCCESS, but Failed to parse {unparseable_data} rows data")
        elif ingest_task_status == 'FAILED':
            # Logging task error message
            self.log.error(f"Druid ingest task is FAILED, check error message:\n {error_msg}")
        else:
            raise AirflowException(f'Could not get status of the job, got {ingest_task_status}')
