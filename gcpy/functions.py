from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage
from . import helpers
import logging
import os
import pandas as pd
import time
from tqdm import tqdm

CLIENT = bigquery.Client()

# Define log settings
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')


####### GCS related #######
def query_to_gcs(sql_query_file: str,
                 target_gcs_path: str,
                 query_params: dict = {},
                 project: str = CLIENT.project,
                 dataset: str = 'gcpy',
                 del_temp_bq_table: bool = True,
                 location: str = 'US',
                 client=CLIENT) -> str:
    """Query all the necessary data and save as sharded csvs in GCS

    Arguments:
    - sql_query_file: Full path to the .sql file that contains the query. 
        Any query parameters should be within {}. eg: WHERE col_name<{some_key}. See query_params argument.
    - target_gcs_path: GCS bucket path to store the output of the query in sharded csvs
    - query_params: Query Parameters dictionary. 
        The sql query in .sql is treated as a string with parameters inside {key} replaced their corresponding values.
        eg: {'some_key':10} will change the sql: WHERE col_name<{some_key} --> WHERE col_name<10
        Default: {}
    - project: GCP Project name. Default: CLIENT.project
    - dataset: Big Query Dataset to temporarily store the results before moving to GCS. 
        It is the string that appears inbetween the project name and table name in Big Query.
        Default: 'gcpy'
    - del_temp_bq_table: Switch to delete the temporary BQ table. Default: True.
        If set to False, the query results are saved in BQ table f'{project}.{dataset}.{sql_query_file.split("/")[-1].split(".")[0]}_{current_date_time}'
    - location: location of the GCS bucket. Default: 'US'
    - client: The BigQuery Client. Default: CLIENT

    Returns:
    The string path to the stored sharded csvs
    """

    # Current datetime
    current_date_time = datetime.now().strftime("%Y%m%d%H%M%S")

    # Create BQ dataset for temporary data storage
    bq_dataset = bigquery.Dataset(f'{project}.{dataset}')
    bq_dataset.location = location
    bq_dataset = client.create_dataset(bq_dataset, timeout=30,
                                       exists_ok=True)  # Make an API request.

    # Query from BQ and store in temporary BQ table.
    logging.info('Executing query')
    query_data = sql_query_file.split("/")[-1].split(".")[0]
    target_table = f'{project}.{dataset}.{query_data}_{current_date_time}'
    helpers.execute_query(sql_query_file, query_params, target_table, client)

    # Export from temporary BQ table to sharded csvs in GCS
    logging.info('Write to Sharded CSVs in GCS')
    gcs_path = f'{target_gcs_path}/{current_date_time}/{query_data}_*.csv'
    tablename = target_table.split('.')[-1]
    helpers.bq_to_gcs(project,
                      dataset,
                      tablename,
                      gcs_path,
                      location=location,
                      client=client)

    if del_temp_bq_table:
        client.delete_table(target_table, not_found_ok=True)
        logging.info(f'Deleted {target_table} temporary BQ Table')

    return gcs_path


def gcs_to_bq(source_gcs_path: str,
              target_bq_dataset: str,
              target_bq_tablename: str,
              target_table_schema='auto',
              num_header_rows: int = 1,
              project: str = CLIENT.project,
              client=CLIENT):
    """Import GCS Table into BQ

    Arguments:
    - source_gcs_path: source GCS path containing the files to load to BQ
    - target_bq_dataset: BQ dataset containing the "target_bq_tablename" table to import into
    - target_bq_tablename: BQ table to import into
    - target_table_schema: 'auto' or dictionary of the form {'col_name': 'dtype'}. Default: 'auto'
    - num_header_rows: Number of header rows to skip while data upload. Default: 1 will skip the first row (column header)
    - project: GCP Project name. Default: CLIENT.project
    - client: The BigQuery Client. Default: CLIENT
    """
    dataset = client.dataset(target_bq_dataset, project=project)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        skip_leading_rows=num_header_rows)

    if target_table_schema == 'auto':
        job_config.autodetect = True
    else:
        job_config.schema = [
            bigquery.SchemaField(key, value)
            for key, value in target_table_schema.items()
        ]

    s = time.time()
    load_job = client.load_table_from_uri(source_gcs_path,
                                          dataset.table(target_bq_tablename),
                                          job_config=job_config)  # API request
    logging.info(f'Starting job {load_job.job_id}')

    load_job.result()  # Waits for table load to complete.
    e = time.time()

    logging.info(f'Job finished. Time elapsed: {e - s} seconds')

    target_table = client.get_table(dataset.table(target_bq_tablename))
    logging.info(f'Loaded {target_table.num_rows} rows')


####### Pandas related #######
def pd_to_bq(source_df,
             target_dataset: str,
             target_tablename: str,
             project: str = CLIENT.project,
             client=CLIENT) -> str:
    """Pandas Dataframe to BigQuery
    
    Arguments:
    - source_df: The DataFrame to load into BQ
    - target_dataset: BQ dataset containing the "target_tablename" table to import into
    - target_tablename: BQ table to import into
    - project: GCP Project name. Default: CLIENT.project
    - client: The BigQuery Client. Default: CLIENT

    Returns:
    The string BQ table name where source_df has been uploaded to
    """
    bq_table = project + '.' + target_dataset + '.' + target_tablename

    job = client.load_table_from_dataframe(source_df, bq_table)

    # Wait for the load job to complete.
    logging.info(job.result())
    return bq_table


def sharded_gcs_csv_to_pd(source_gcs_path: str, file_prefix: str):
    """GCS to Pandas Dataframe
    
    Arguments:
    - source_gcs_path: source GCS path containing the sharded csv files to load to BQ
    - file_prefix: A file name prefix to select only the files of interest
    """

    bucketName = source_gcs_path.split('/')[2]
    prefix = os.path.join(*source_gcs_path.split('/')[3:-1], file_prefix)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucketName)

    # List the files in a GCS directory
    files = bucket.list_blobs(prefix=prefix)
    file_list = [
        os.path.join("gs://", bucketName, file.name) for file in files
        if '.' in file.name
    ]

    # Load all the csv files into a single dataframe if you want to (limited by RAM size). usecols is used to only read the columns of interest.
    df_list = []
    for file in tqdm(file_list):
        df = pd.read_csv(file)
        df_list.append(df)

    unsharded_df = pd.concat(df_list, ignore_index=True)

    return unsharded_df
