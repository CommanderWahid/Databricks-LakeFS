# Databricks notebook source
# MAGIC %md
# MAGIC #### Imports

# COMMAND ----------

import datetime
from common.helpers import (check_data_quality,sync_lakefs_to_uc,init_lakefs_client,get_lakefs_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Lakefs client setup

# COMMAND ----------

client = init_lakefs_client(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Widgets initialization

# COMMAND ----------

# get repos
repositories = (
    client
    .repositories
    .list_repositories()
    .get('results')
)
repo_names = [r.get('id') for r in repositories]
dbutils.widgets.dropdown('Repo',repo_names[0], repo_names, 'Repo')
repo = dbutils.widgets.get("Repo")

# get branchs for the chosen repo
branches = (
    client
    .branches
    .list_branches(repository=repo)
    .get('results')
)
branche_names = [b.get('id') for b in branches]

dbutils.widgets.dropdown('Branch', branche_names[0], branche_names, 'Branch')
branch = dbutils.widgets.get('Branch')

# get notebooks 
notebook_names = ['DimTaxiTripRateCode','DimTaxiTripMode','DimTaxiTripPaymentType','DimTaxiTripType','DimTaxiTripZone', 'FacTaxiTrip']
dbutils.widgets.multiselect('Notebook', 'All', notebook_names + ['All'], 'Notebook')
notebooks = dbutils.widgets.get('Notebook').split(',')

# get job id
dbutils.widgets.text("JobId", "")
job_id = dbutils.widgets.get('JobId')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Global Variables

# COMMAND ----------

now = datetime.datetime.now()
current_timestamp = now.strftime('%Y-%m-%d %H:%M:%S')

if not job_id:
    job_id = now.strftime('%Y%m%d%H%M%S%f')

catalog_name = 'taxi_service_checks_catalog'
schema_name  =  ('data_quality_' + repo + '_' + branch).replace('-','0')
data_quality_results_table_name = 'data_quality_results'
shallow_clone_path = '/mnt/main/lab/clones/'
expectation_suite_path = f'/dbfs/mnt/main/lab/data_quality_conf/{branch}/'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create catalogs/schemas if not exist

# COMMAND ----------

# create: 
# 1. A new unity catalog & schema for the current repo/branch
# 2. Two schemas in hive_metastore to sync tables into unity catalog 
if repo.lower() != 'none':
    spark.sql(f'CREATE CATALOG IF NOT EXISTS {catalog_name};')
    spark.sql(f'CREATE TABLE IF NOT EXISTS {catalog_name}.default.{data_quality_results_table_name}\
                                                        (job_id STRING,current_timestamp TIMESTAMP,lakefs_Repo STRING,lakefs_Branch STRING,\
                                                        table_name STRING,expectation_success STRING,expectation_result STRING)') 
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {catalog_name}.`{schema_name}`;')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS hive_metastore.`{schema_name}`;')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS hive_metastore.`{schema_name}_shallow_clone`;')


# COMMAND ----------

# MAGIC %md
# MAGIC #### Apply data quality checks and sync tables/logs to UC

# COMMAND ----------

# check if all is selected
if 'All' in notebooks:
    notebooks = notebook_names
# loop through the notebooks and apply data quality checks
for notebook_name in notebooks:
    # apply data quality check on target table 
    table_name = notebook_name
    try:
        # get lakefs uri path
        if table_name.lower().startswith('dim'):
            lakefs_delta_path = get_lakefs_path(repo=repo,branch=branch,kind='referencedata',table_name=table_name)
        elif table_name.lower().startswith('fac'):
            lakefs_delta_path = get_lakefs_path(repo=repo,branch=branch,kind='transactionaldata',table_name=table_name)
        else:
            raise(f'Cannot generate lakefs path for table: {table_name}')
        
        log_table_full_name = f'{catalog_name}.default.{data_quality_results_table_name}'
        check_data_quality(spark,repo,branch,lakefs_delta_path,table_name,log_table_full_name,expectation_suite_path,job_id,current_timestamp)
        # sync lakefs table to unity catalog 
        table_clone_path = shallow_clone_path + table_name
        sync_lakefs_to_uc(spark,catalog_name,schema_name,table_name,lakefs_delta_path,table_clone_path)

    except Exception as e:
        print(e)
        continue