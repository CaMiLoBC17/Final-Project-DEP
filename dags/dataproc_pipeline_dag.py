from __future__ import annotations
import os
import requests
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocDeleteClusterOperator, DataprocSubmitJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.email import send_email

PROJECT_ID = Variable.get("PROJECT_ID")
REGION = Variable.get("REGION", default_var="us-central1")
GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_DATASET = Variable.get("BQ_DATASET", default_var="curso_ds")
EMAIL_TO = Variable.get("EMAIL_TO")
DRIVE_FILE_ID = Variable.get("DRIVE_FILE_ID", default_var="1UZmU8RSJryGVtj6GZi7L9jNRB2kp8dV_")
DATAPROC_CLUSTER_NAME_PREFIX = Variable.get("DP_CLUSTER_PREFIX", default_var="curso-dp")
GCS_STAGING_PREFIX = Variable.get("GCS_STAGING_PREFIX", default_var="staging/")
GCS_JOBS_PREFIX = Variable.get("GCS_JOBS_PREFIX", default_var="jobs/")
GCS_OUTPUT_PREFIX = Variable.get("GCS_OUTPUT_PREFIX", default_var="output/")
DATASET_BASENAME = Variable.get("DATASET_BASENAME", default_var="salaries.csv")
PYSPARK_JOB_FILENAME = Variable.get("PYSPARK_JOB_FILENAME", default_var="pyspark_job.py")
BQ_JAR = "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.36.1.jar"
execution_date = "{{ ds_nodash }}"
CLUSTER_NAME = f"{DATAPROC_CLUSTER_NAME_PREFIX}-{execution_date}"
GCS_DATA_URI = f"gs://{GCS_BUCKET}/{GCS_STAGING_PREFIX}{DATASET_BASENAME}"
GCS_JOB_URI = f"gs://{GCS_BUCKET}/{GCS_JOBS_PREFIX}{PYSPARK_JOB_FILENAME}"

def download_drive_to_gcs(**context):
    url = f"https://drive.google.com/uc?export=download&id={DRIVE_FILE_ID}"
    resp = requests.get(url, allow_redirects=True)
    resp.raise_for_status()
    gcs_hook = GCSHook()
    gcs_hook.upload(bucket_name=GCS_BUCKET, object_name=f"{GCS_STAGING_PREFIX}{DATASET_BASENAME}", data=resp.content, mime_type="text/csv")

def upload_job_to_gcs(**context):
    possible_paths = [
        f"/home/airflow/gcs/jobs/{PYSPARK_JOB_FILENAME}",
        f"/opt/airflow/dags/jobs/{PYSPARK_JOB_FILENAME}",
        f"/home/airflow/gcs/dags/jobs/{PYSPARK_JOB_FILENAME}",
    ]
    local_path = None
    for p in possible_paths:
        if os.path.exists(p):
            local_path = p
            break
    if local_path is None:
        raise FileNotFoundError(f"No se encontró {PYSPARK_JOB_FILENAME}")
    gcs_hook = GCSHook()
    gcs_hook.upload(bucket_name=GCS_BUCKET, object_name=f"{GCS_JOBS_PREFIX}{PYSPARK_JOB_FILENAME}", filename=local_path)

def build_email_html(client, query):
    job = client.query(query)
    rows = list(job.result())
    if not rows:
        return "<p>Sin resultados.</p>"
    headers = rows[0].keys()
    html = ["<table border='1' cellpadding='6' cellspacing='0'>"]
    html.append("<thead><tr>" + "".join(f"<th>{h}</th>" for h in headers) + "</tr></thead>")
    html.append("<tbody>")
    for r in rows:
        html.append("<tr>" + "".join(f"<td>{r[h]}</td>" for h in headers) + "</tr>")
    html.append("</tbody></table>")
    return "\n".join(html)

def email_report(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    q1 = f"SELECT experience_level, AVG_USD AS avg_salary_usd FROM `{PROJECT_ID}.{BQ_DATASET}.salaries_by_experience` ORDER BY avg_salary_usd DESC LIMIT 10"
    q2 = f"SELECT job_title, AVG_USD AS avg_salary_usd FROM `{PROJECT_ID}.{BQ_DATASET}.salaries_by_job` ORDER BY avg_salary_usd DESC LIMIT 10"
    q3 = f"SELECT * FROM `{PROJECT_ID}.{BQ_DATASET}.job_salary_extremes` ORDER BY metric DESC"
    q4 = f"SELECT * FROM `{PROJECT_ID}.{BQ_DATASET}.top10_salaries` ORDER BY salary_in_usd DESC LIMIT 10"
    html_parts = [
        "<h3>Top 10 — Salario promedio por experience_level</h3>", build_email_html(client, q1),
        "<h3>Top 10 — Salario promedio por job_title</h3>", build_email_html(client, q2),
        "<h3>Máximo y mínimo por cargo</h3>", build_email_html(client, q3),
        "<h3>Top 10 — Salarios más altos</h3>", build_email_html(client, q4),
    ]
    send_email(to=EMAIL_TO, subject="Reporte Top‑10 Proyecto Final", html_content="\n".join(html_parts))

with DAG(
    dag_id="dataproc_pipeline_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={"owner": "data-eng", "retries": 0},
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config={
            "master_config": {"num_instances": 1, "machine_type_uri": "e2-standard-4"},
            "worker_config": {"num_instances": 2, "machine_type_uri": "e2-standard-4"},
            "software_config": {"image_version": "2.1-debian12"},
        },
    )
    download_dataset = PythonOperator(task_id="download_dataset", python_callable=download_drive_to_gcs)
    upload_job = PythonOperator(task_id="upload_job", python_callable=upload_job_to_gcs)
    submit_pyspark = DataprocSubmitJobOperator(
        task_id="submit_pyspark",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": GCS_JOB_URI,
                "args": [
                    f"--input={GCS_DATA_URI}",
                    f"--gcs_output=gs://{GCS_BUCKET}/{GCS_OUTPUT_PREFIX}",
                    f"--bq_dataset={BQ_DATASET}",
                    f"--project_id={PROJECT_ID}",
                ],
                "jar_file_uris": [BQ_JAR],
            },
        },
    )
    send_email_task = PythonOperator(task_id="send_email_report", python_callable=email_report)
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    create_cluster >> [download_dataset, upload_job] >> submit_pyspark >> send_email_task >> delete_cluster
