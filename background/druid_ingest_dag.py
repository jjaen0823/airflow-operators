import json
import os
from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from background.library.operators.druid.druid_operator import CustomDruidOperator
from background.library.operators.druid.druid_task_report_operator import CustomDruidTaskReportOperator


def fn_change_intervals(spec: dict, **kwargs):
    try:
        data_interval_start_UTC, data_interval_end_UTC = kwargs['data_interval_start'], kwargs['data_interval_end']
        # TODO timezone 정책 논의
        # data_interval_start_KST = data_interval_start_UTC + timedelta(hours=9)
        # data_interval_end_KST = data_interval_end_UTC + timedelta(hours=9)

        if args["schedule_interval"] != "@once":
            new_intervals = [f"{data_interval_start_UTC}/{data_interval_end_UTC}"]
        else:
            new_intervals = []
            spec["spec"]["ioConfig"]["dropExisting"] = False

        spec["spec"]["dataSchema"]["timestampSpec"]["missingValue"] = f"{data_interval_start_UTC}"
        spec["spec"]["dataSchema"]["granularitySpec"]["intervals"] = new_intervals
        print(f"new intervals={new_intervals}")

        kwargs['ti'].xcom_push("new_spec", json.dumps(spec))

    except KeyError:
        print("Failed change intervals")


# arguments.json
arguments_file_path = f"{os.path.dirname(os.path.realpath(__file__))}/arguments.json"
with open(arguments_file_path, "r") as file:
    args = json.load(file)


# change start_date(str) to datetime
args["default_args"]["start_date"] = pendulum.parse(args["default_args"]["start_date"])

with DAG(
    dag_id=args["dag_id"],
    description=args["description"],
    schedule_interval=args["schedule_interval"],
    concurrency=args["concurrency"],
    max_active_tasks=args["max_active_tasks"],
    max_active_runs=args["max_active_runs"],
    catchup=args["catchup"],
    default_args=args["default_args"],
    tags=args["tags"],
    user_defined_macros=args["user_defined_macros"],
) as dag:
    # spec.json
    spec_file_path = f"{os.path.dirname(os.path.realpath(__file__))}/spec.json"
    with open(spec_file_path, "r") as file:
        spec = json.load(file)

    change_intervals = PythonOperator(
        task_id="change_intervals",
        python_callable=fn_change_intervals,
        op_kwargs={"spec": spec}
    )

    druid_ingest_operator = CustomDruidOperator(
        task_id="druid_ingest",
        json_index_file="{{ task_instance.xcom_pull(task_ids='change_intervals', key='new_spec') }}",
        druid_ingest_conn_id=dag.user_defined_macros["druid_ingest_conn_id"],
    )

    druid_task_report_operator = CustomDruidTaskReportOperator(
        task_id="druid_task_report",
        druid_conn_id=dag.user_defined_macros["druid_ingest_conn_id"]
    )

    [] >> change_intervals
    [change_intervals] >> druid_ingest_operator
    [druid_ingest_operator] >> druid_task_report_operator
