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

from typing import TYPE_CHECKING, Any

from airflow.models import BaseOperator
from airflow.utils.trigger_rule import TriggerRule

from background.library.hooks.druid.druid_hook import CustomDruidHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class CustomDruidTaskReportOperator(BaseOperator):
    """
    [ CustomDruidTaskReportOperator ]
    : Connection to Druid Overlord for the ingestion task report
      - Gets the result of the druid task using druid task id and status.
    Allows to get a task report to druid with the ingestion task id submitted to druid

    :param druid_conn_id: The connection id to the Druid overlord machine
                                 which gets the task report

    :author itzel.choi
    :date date 2022-09-29
    """

    def __init__(
            self,
            *,
            druid_conn_id: str = 'druid_ingest_default',
            **kwargs: Any,
    ) -> None:
        super().__init__(trigger_rule=TriggerRule.ALWAYS, **kwargs)
        self.conn_id = druid_conn_id

    def execute(self, context: "Context") -> None:
        ingest_task_id = context['ti'].xcom_pull(key='druid_task_id')
        ingest_task_status = context['ti'].xcom_pull(key='druid_task_status')

        hook = CustomDruidHook(
            druid_ingest_conn_id=self.conn_id,
        )
        hook.get_task_report(ingest_task_id, ingest_task_status)
