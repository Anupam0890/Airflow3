from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python import PythonOperator
import json
import google.auth.transport.requests
import google.oauth2.id_token
import requests
import logging

GCP_CONN_ID = "google_cloud_default"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": pendulum.duration(minutes=5),
    "gcp_conn_id": GCP_CONN_ID,
    }


def invoke_cloud_run(**kwargs):
    """
    Invokes a Google Cloud Run service with IAM authentication.
    """
    service_url = kwargs['service_url']
    method = kwargs['method']
    payload = kwargs.get('payload', None) # Use .get for optional payload

    logging.info(f"Attempting to invoke Cloud Run service: {service_url}")
    logging.info(f"Method: {method}")
    if payload:
        logging.info(f"Payload: {json.dumps(payload)}") # Log payload safely

    try:
        # 1. Get authentication token
        auth_req = google.auth.transport.requests.Request()
        id_token = google.oauth2.id_token.fetch_id_token(auth_req, service_url)
        headers = {
            "Authorization": f"Bearer {id_token}",
            "Content-Type": "application/json" # Adjust if your service expects a different content type
        }
        logging.info("Successfully fetched authentication token.")

        # 2. Make the HTTP Request
        logging.info(f"Sending {method} request to {service_url}...")
        response = None
        if method.upper() == "POST":
            response = requests.post(service_url, headers=headers, json=payload, timeout=300) # 5 min timeout
        elif method.upper() == "GET":
            response = requests.get(service_url, headers=headers, timeout=300)
        # Add other methods (DELETE, PATCH, etc.) as needed
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        # 3. Handle Response
        logging.info(f"Cloud Run service responded with status code: {response.status_code}")
        response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)

        try:
            # Try to log response body as JSON, fallback to text
            response_json = response.json()
            logging.info(f"Response Body (JSON): {json.dumps(response_json)}")
        except json.JSONDecodeError:
            logging.info(f"Response Body (Text): {response.text}")

        logging.info("Cloud Run invocation successful.")

    except google.auth.exceptions.DefaultCredentialsError as e:
        logging.error(f"Authentication failed. Could not find default credentials. "
                      f"Ensure your Airflow environment is configured correctly for GCP access. Error: {e}")
        raise
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request failed: {e}")
        if e.response is not None:
            logging.error(f"Response Status Code: {e.response.status_code}")
            logging.error(f"Response Body: {e.response.text}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise

with DAG(
    dag_id="pipeline-ecommerce-bq-dbt",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="Dag to run the Ecommerce Pipeline using BigQuery, Cloud Run and DBT"
) as dag:

    GCP_PROJECT_ID = "your-gcp-project-id"
    BQ_DATASET = "your_bq_dataset"
    BQ_DESTINATION_TABLE = "your_destination_table_name"
    BQ_LOCATION = "US"
    list_files = [
                    'olist_customers_dataset',
                    'olist_geolocation_dataset',
                    'olist_order_items_dataset',
                    'olist_order_payments_dataset',
                    # 'olist_order_reviews_dataset',
                    'olist_orders_dataset',
                    'olist_products_dataset',
                    'olist_sellers_dataset'
                ]


    # print_token = bash.BashOperator(
    #         task_id='print_token', 
    #         bash_command='gcloud auth print-identity-token' # The end point of the deployed Cloud Run container
    #     ) 

    # token = "{{ task_instance.xcom_pull(task_ids='print_token') }}" # gets output from 'print_token' task

    run_cloud_service_dbt = PythonOperator(
        task_id='run_cloud_service_dbt',
        python_callable=invoke_cloud_run,
        op_kwargs={
            'service_url': "https://pipeline-ecommerce-bq-dbt-131772725043.us-central1.run.app/run_transformation",
            'method': "POST",
            'payload': {},
            # Add any other parameters your function might need
        },
    )



    for file in list_files:
        SQL_QUERY = f"""
            LOAD DATA OVERWRITE ecommerce_raw.{file}
            FROM FILES (
            format = 'CSV',
            uris = ['gs://eighth-feat-467302-g3-tf-bucket/{file}.csv']);
        """

        run_bq_query_job = BigQueryExecuteQueryOperator(
            task_id=f"create_raw_table_{file}",
            sql=SQL_QUERY,
            use_legacy_sql=False,
            location=BQ_LOCATION,
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            allow_large_results=True,
        )

        run_bq_query_job #>> run_cloud_service_dbt
