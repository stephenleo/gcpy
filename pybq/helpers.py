from google.cloud import bigquery
import logging
import time

logger = logging.getLogger(__name__)

def execute_query(sql_query_file: str,
                  query_params: dict,
                  target_table: str,
                  client):
    """Helper function to exectue a query
    
    Arguments:
    - sql_query_file: Full path to the .sql file that contains the query. 
        Any query parameters should be within {}. eg: WHERE col_name<{some_key}. See query_params argument.
    - query_params: Query Parameters dictionary. 
        The sql query in .sql is treated as a string with parameters inside {key} replaced their corresponding values.
        eg: {'some_key':10} will change the sql: WHERE col_name<{some_key} --> WHERE col_name<10
    - target_table: The Big Query Table to store the data in. This could be a temporary holding area before pushing to GCS.
    - client: The BigQuery Client.
    """

    job_config = bigquery.QueryJobConfig(
        destination=target_table,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

    # Read the sql string
    with open(sql_query_file) as f:
        sql = f.read()

    # Set Params to the query
    sql = sql.format(**query_params)

    # Start the query, passing in the extra configuration.
    s = time.time()
    query_job = client.query(sql, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.
    e = time.time()
    logger.info(f'Query Execution time: {e - s}')

def bq_to_gcs(project: str,
              source_dataset: str,
              source_tablename: str,
              gcs_path: str,
              location: str,
              client):
    """Helper function to export a BQ Table to GCS bucket
    
    Arguments:
    - project: GCP Project name
    - source_dataset: BQ dataset containing the "source_tablename" table to export
    - source_tablename: BQ table to export
    - gcs_path: GCS path to store the sharded csvs
    - location: location of the GCS bucket.
    - client: The BigQuery Client.
    """

    dataset = client.dataset(source_dataset, project=project)
    table_ref = dataset.table(source_tablename)

    s = time.time()
    extract_job = client.extract_table(
        table_ref,
        gcs_path,
        location=location,
    )  # API request
    extract_job.result()  # Waits for job to complete.
    e = time.time()

    logger.info(f'Exported {project}.{source_dataset}.{source_tablename} to {gcs_path}')
    logger.info(f'Time elapsed: {e - s} seconds')