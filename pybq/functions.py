from google.cloud import bigquery
from . import helpers
import logging
import os
import time

CLIENT = bigquery.Client()

# Define log settings
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

####### GCS related #######
def query_to_gcs(sql_file: str,
                 query_params: dict,
                 current_date: str,
                 data_path: str,
                 project: str,
                 dataset: str,
                 location: str = "US",
                 client=CLIENT) -> str:
    """Query all the necessary data and save as sharded csvs in GCS

    Arguments:
    - sql_file: Full path to the .sql file that contains the query. 
                Any query parameters should be within {}. eg: WHERE col_name<{some_key}
    - query_params: Query Parameters dictionary. 
                    The sql query in .sql is treated as a string with parameters inside {key} replaced their corresponding values.
                    eg: {'some_key':10} will change the sql: WHERE col_name<{some_key} --> WHERE col_name<10
    - current_date: Current date string. Used as a suffix for the Big Query temporary table and directory in GCS.
    - data_path: GCS bucket path to store the sharded csvs
    - project: GCP Project name
    - dataset: Big Query Dataset to temporarily store the results before moving to GCS. 
               It is the string that appears inbetween the project name and table name in Big Query
    - location: location of the GCS bucket. Default: "US"
    - client: The BigQuery Client. Default: CLIENT

    Returns:
    The string path to the stored sharded csvs
    """

    # Query from BQ and store in temporary BQ table.
    logging.info('Executing query')
    query_data = sql_file.split("/")[-1].split(".")[0]
    output_table = '{}.{}.{}_{}'.format(project, dataset, query_data,
                                        current_date)
    helpers.execute_query(sql_file, query_params, output_table, client)

    # Export from temporary BQ table to sharded csvs in GCS
    logging.info('Write to Sharded CSVs in GCS')
    gcs_path = '{}/{}/{}_*.csv'.format(data_path, current_date, query_data)
    tablename = output_table.split('.')[-1]
    helpers.bq_to_gcs(project, dataset, tablename, gcs_path, location="US", client=client)

    return gcs_path


def gcs_to_bq(project: str, dataset: str, tablename: str, gcs_path: str, client=CLIENT):
    """Import GCS Table into BQ

    Arguments:
    - project: GCP Project name
    - dataset: BQ dataset containing the "tablename" table to import into
    - tablename: BQ table to import into
    - gcs_path: GCS path to store the sharded csvs
    - client: The BigQuery Client. Default: CLIENT
    """
    dataset_ref = client.dataset(dataset, project=project)

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

    s = time.time()
    load_job = client.load_table_from_uri(gcs_path,
                                          dataset_ref.table(tablename),
                                          job_config=job_config)  # API request
    logging.info("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    e = time.time()

    logging.info("Job finished. Time elapsed: {} seconds".format(e - s))

    destination_table = client.get_table(dataset_ref.table(tablename))
    logging.info("Loaded {} rows.".format(destination_table.num_rows))


####### Pandas related #######
def pd_to_bq(df, project: str, dataset: str, tablename: str, client=CLIENT):
    """Pandas Dataframe to BigQuery
    
    Arguments:
    - df: The DataFrame to load into BQ
    - project: GCP Project name
    - dataset: BQ dataset containing the "tablename" table to import into
    - tablename: BQ table to import into
    - client: The BigQuery Client. Default: CLIENT

    Returns:
    The string BQ table name where df has been uploaded to
    """
    bq_table = project + '.' + dataset + '.' + tablename

    job = client.load_table_from_dataframe(df, bq_table)

    # Wait for the load job to complete.
    logging.info(job.result())
    return bq_table

def sharded_gcs_csv_to_pd(gcs_path, file_prefix):
    """GCS to Pandas Dataframe"""
    bucketName = gcs_path.split('/')[2]
    prefix = os.path.join(*gcs_path.split('/')[3:-1], file_prefix)

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
    for file in file_list:
        df = pd.read_csv(file)
        df_list.append(df)

    unsharded_df = pd.concat(df_list, ignore_index=True)

    return unsharded_df

def main():
    query_params = {'date_key_start': start_date, 'date_key_end': end_date}
    query_data = 'shop_data'  #name of the SQL file
    shop_data_filepath = query_to_gcs(client, query_params, query_data,
                                      current_date, end_date, data_path)
