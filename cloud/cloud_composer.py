import logging
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from google.cloud import storage
import requests

# Konfigurationen
PROJECT_ID = '<your-project-id>'
BUCKET_NAME = '<your-bucket-name>'
DATASET_NAME = '<your-dataset-name>'
TABLE_NAME = '<your-table-name>'
DATA_PROC_CLUSTER = '<your-cluster-name>'
REGION = '<your-region>'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

logging.basicConfig(level=logging.INFO)


def download_and_upload_to_gcs(url: str, headers: dict, gcs_path: str, decode: str = 'utf-8',
                               skip_lines: int = 0) -> str:
    """
    Lädt Daten von der angegebenen URL herunter, überspringt optional die ersten Zeilen 
    und lädt den Inhalt anschließend in einen GCS-Bucket hoch.
    """
    logging.info("Starte Download von URL: %s", url)
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to download file: {response.status_code}")
    logging.info("Download erfolgreich abgeschlossen")

    content = response.content.decode(decode, errors='replace')
    if skip_lines:
        lines = content.split('\n')[skip_lines:]
        content = '\n'.join(lines)

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)

    logging.info("Lade %d Bytes zu gs://%s/%s hoch", len(content), BUCKET_NAME, gcs_path)
    blob.upload_from_string(content, content_type='text/csv')

    return f"gs://{BUCKET_NAME}/{gcs_path}"


def extract_data_from_deutschland_api() -> str:
    """
    Extrahiert Daten aus der Deutschland API, überspringt die ersten 10 Zeilen und lädt sie in GCS hoch.
    """
    url = ('https://www.bundesnetzagentur.de/SharedDocs/Downloads/DE/Sachgebiete/Energie/Unternehmen_Institutionen/'
           'E_Mobilitaet/Ladesaeulenregister_CSV.csv?__blob=publicationFile&v=42')
    headers = {'Content-Type': 'text/csv; charset=UTF-8'}
    return download_and_upload_to_gcs(url, headers, 'deutschland_api/deutschland_api_ladesaulen.csv',
                                      decode='ISO-8859-1', skip_lines=10)


def extract_data_from_open_charge_map() -> str:
    """
    Extrahiert Daten aus der OpenChargeMap API und lädt sie in GCS hoch.
    """
    url = ('https://api.openchargemap.io/v3/poi/?output=csv&countrycode=DE&maxresults=20000000'
           '&key=7cc8dce0-c8a6-4302-a88c-2d167b6898f7')
    headers = {'Content-Type': 'text/csv; charset=UTF-8'}
    return download_and_upload_to_gcs(url, headers, 'open_charge_api/open_charge_ladesaulen.csv')


def extract_data_from_open_data() -> str:
    """
    Extrahiert Daten aus der Open Data Quelle und lädt sie in GCS hoch.
    """
    url = ("https://opendata.rhein-kreis-neuss.de/api/explore/v2.1/catalog/datasets/"
           "rhein-kreis-neuss-ladesaulen-in-deutschland/exports/csv?delimiter=%3B&list_separator=%2C"
           "&quote_all=false&with_bom=true")
    headers = {'Content-Type': 'text/csv; charset=UTF-8'}
    return download_and_upload_to_gcs(url, headers, 'OpenDataAPI-raw/ladesaulen.csv')


with DAG(
        'cloud_composer_pipeline_ladesaulen',
        default_args=default_args,
        description='Orchestrate data pipeline for ladesaulen data',
        schedule_interval='@daily',
        start_date=days_ago(1),
        catchup=False,
) as dag:
    extract_data_deutschland_api_job = PythonOperator(
        task_id='extract_data_deutschland_api_job',
        python_callable=extract_data_from_deutschland_api,
    )

    extract_data_open_charge_job = PythonOperator(
        task_id='extract_data_open_charge_job',
        python_callable=extract_data_from_open_charge_map,
    )

    extract_data_open_data_job = PythonOperator(
        task_id='extract_data_open_data_job',
        python_callable=extract_data_from_open_data,
    )

    transform_deutschland_api_job = DataprocSubmitJobOperator(
        task_id='transform_deutschland_api_job',
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": DATA_PROC_CLUSTER},
            "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET_NAME}/scripts/data_proc_deutschland_api.py"},
        },
    )

    transform_open_charge_map_job = DataprocSubmitJobOperator(
        task_id='transform_open_charge_map_api_job',
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": DATA_PROC_CLUSTER},
            "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET_NAME}/scripts/data_proc_open_charge_api.py"},
        },
    )

    transform_open_data_job = DataprocSubmitJobOperator(
        task_id='transform_open_data_job',
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": DATA_PROC_CLUSTER},
            "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET_NAME}/scripts/data_proc_open_data_api.py"},
        },
    )

    merge_data_job = DataprocSubmitJobOperator(
        task_id='merge_data_job',
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": DATA_PROC_CLUSTER},
            "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET_NAME}/scripts/merge_data_source.py"},
        },
    )

    load_data_to_big_query_job = GCSToBigQueryOperator(
        task_id='load_data_to_big_query',
        bucket=BUCKET_NAME,
        source_objects=["bigquery/part-*.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}",
        write_disposition='WRITE_TRUNCATE',
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        max_bad_records=100,
    )

    extract_data_deutschland_api_job >> extract_data_open_charge_job >> extract_data_open_data_job >> \
    transform_deutschland_api_job >> transform_open_charge_map_job >> transform_open_data_job >> \
    merge_data_job >> load_data_to_big_query_job
