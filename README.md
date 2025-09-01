# Proyecto Final — GCP Dataproc + PySpark + BigQuery + Airflow (Composer)

Este proyecto contiene:

- DAG de Airflow que crea un clúster Dataproc, procesa datos y lo elimina al final.
- Script PySpark que limpia un dataset de salarios, genera agregados y guarda resultados en BigQuery.
- Archivo requirements.txt con dependencias mínimas.
- Este README con las instrucciones de despliegue.

## Prerrequisitos

1. Proyecto GCP con facturación activa.
2. Habilitar APIs: Composer, Dataproc, BigQuery, Storage, IAM.
3. Crear un bucket en GCS y un dataset en BigQuery.
4. Configurar variables en Composer (PROJECT_ID, REGION, GCS_BUCKET, BQ_DATASET, EMAIL_TO, etc.).

## Despliegue

1. Subir `dags/dataproc_pipeline_dag.py` al entorno Composer.
2. Subir `jobs/pyspark_job.py` al entorno Composer en la carpeta `dags/jobs/`.
3. Configurar dependencias desde `requirements.txt`.
4. Ejecutar el DAG en la UI de Airflow.

## Resultados esperados

- Tablas creadas en BigQuery: `salaries_clean`, `salaries_by_experience`, `salaries_by_job`, `job_salary_extremes`, `top10_salaries`.
- Correo recibido con tablas Top‑10 en HTML.
