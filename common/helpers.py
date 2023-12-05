from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import col,when,lit
import  great_expectations as ge
import os
import lakefs_client
from lakefs_client import models
from lakefs_client.client import LakeFSClient
from lakefs_client import configuration
from pyspark.dbutils import DBUtils

def file_exists(
      spark: SparkSession,
      dir: str
    ) -> bool: 
    """
    Check if a file or folder exists on destination location.
    
    Parameters
    ----------
    spark: SparkSession 
       The entry point to spark on the remote cluster.
    dir: str
       The path of the file or directory.
       The path should be the absolute DBFS path (/absolute/path/).
    
    Raises
    ------
    RuntimeError
       if the user does not have permission to check the target file or directory.
    
    Returns
    -------
    bool
       True: if the path does exist.
       False: if the path doesn't exist or current permissions are insufficient.
    """

    try:
        dbutils = DBUtils(spark)
        dbutils.fs.ls(dir)
    except Exception as e:
       if 'java.nio.file.AccessDeniedException' in str(e):
          raise
       return False
    
    return True

def read_delta_table(
        spark,
        path: str = '',
        columns: list = ['*']
    ) -> DataFrame:
    """
    Read delta table using a path and select the list of columns in an output dataframe.
    """
    raw_data = (
        spark
        .read
        .format('delta')
        .load(path)
    )
    output_df = raw_data.select(*columns)
    return output_df
   
def check_data_quality(
        spark: SparkSession,
        repo: str, 
        branch: str, 
        lakefs_delta_path: str, 
        target_table_name: str, 
        log_table_full_name: str,
        expectation_suite_path: str,
        job_id: str,
        current_timestamp: str
    ) -> None:
    """
    Apply data quality checks on the current delta table.
    """
    
    # build the expectation conf file path for the current table <target_table_name>
    filename = target_table_name.lower() +'.data.expectations.json'
    expectation_suite_file = os.path.join(expectation_suite_path, filename)
    try:
        # check if the file exists 
        if file_exists(spark,expectation_suite_file.removeprefix('/dbfs')):
            # read the delta table using the lakefs path <lakefs://{repo}/{branch}/target_table_name>
            df_lakefs = spark.read.format('delta').load(lakefs_delta_path)
            # validate the expectations
            df_expectations = ge.dataset.SparkDFDataset(df_lakefs)
            test_results = df_expectations.validate(expectation_suite=expectation_suite_file)
            # save the expectation results and status in a unity catalog table or print them on screen
            success = test_results['success']
            results = test_results['results']
        else: 
            success = 'false'
            results = f'Cannot find the current expectation conf file: {expectation_suite_file}'
    except Exception as e:
        success = 'false'
        results = f'An error occured during expectation validation: {str(e)}'

    if log_table_full_name: 
        spark.sql(f"INSERT INTO {log_table_full_name} VALUES ('{job_id}','{current_timestamp}','{repo}','{branch}','{target_table_name}','{success}','{results}')")
    else:
        print(test_results)


def sync_lakefs_to_uc(
        spark: SparkSession,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        lakefs_delta_path: str,
        table_clone_path: str
    ) -> None:
    """
    The goal of this function is to sync a lake-fs table into unity catalog
    To do that we will follow the work arround three steps bellow: 
    1. Create a table on the hive-metastore using the lake-fs path
    2. Clone the recently created table 
    3. Sync the clone to UC

    -> The clone step is an intermediary and a mandatory step to sync the table with UC.
    """

    # create the current table <table_name> in hive_metastore using the lakefs path <lakefs://{repo}/{branch}/table_name>
    spark.sql(f"CREATE TABLE IF NOT EXISTS hive_metastore.{schema_name}.{table_name} USING DELTA LOCATION '{lakefs_delta_path}';")
    # create a clone of the recently created table (in the previous step) 
    # this clone will be used later to sync the <table_name> into UC
    spark.sql(f"CREATE OR REPLACE TABLE hive_metastore.{schema_name}_shallow_clone.{table_name} SHALLOW CLONE hive_metastore.{schema_name}.{table_name} LOCATION '{table_clone_path}';")
    # sync the recently created clone to unity catalog
    spark.sql(f'SYNC TABLE {catalog_name}.{schema_name}.{table_name} FROM hive_metastore.{schema_name}_shallow_clone.{table_name};')


def init_lakefs_client(
        spark: SparkSession
    ) -> LakeFSClient:
    """
    Init the lakeFS client
    """
    # lakeFS endpoint and keys
    dbutils = DBUtils(spark)
    lakefsEndPoint =  dbutils.secrets.get(scope="scope-for-databricks-lakefs",key="lakefsendpoint")
    lakefsAccessKey = dbutils.secrets.get(scope="scope-for-databricks-lakefs",key="lakefsaccesskey")
    lakefsSecretKey = dbutils.secrets.get(scope="scope-for-databricks-lakefs",key="lakefssecretkey")
    # lakeFS credentials and endpoint
    configuration = lakefs_client.Configuration()
    configuration.username = lakefsAccessKey
    configuration.password = lakefsSecretKey
    configuration.host = lakefsEndPoint
    client = LakeFSClient(configuration)
    return client

def get_lakefs_path(
        repo: str,
        branch: str,
        kind: str,
        table_name: str
    ) -> str:
    """
    Return the table path using lakefs endpoint
    """
    return f'lakefs://{repo}/{branch}/taxiservice/{kind}/{table_name}'

def get_cloud_storage_path(
        zone: str,
        kind:str,
        table_name: str
    ) -> str:
    """
    Return the table path, from the cloud storage
    """
    return  f'/mnt/main/{zone}/delta/taxiservice/{kind}/{table_name}'
