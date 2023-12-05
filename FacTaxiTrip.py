# Databricks notebook source
# MAGIC %md
# MAGIC #### Imports

# COMMAND ----------

from delta.tables import DeltaTable
import itertools
import pyspark
from functools import reduce
from pyspark.sql.functions import row_number,desc
from pyspark.sql.window import Window
from common.helpers import (init_lakefs_client,read_delta_table,get_lakefs_path,get_cloud_storage_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Lake FS client

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
repo_names = [r.get('id') for r in repositories] + ['None']
dbutils.widgets.dropdown('Repo', 'None', repo_names, 'Repo')
repo = dbutils.widgets.get("Repo")

# get branchs for the chosen repo
if repo.lower() == 'none':
    branche_names = ['None'] 
else:
    branches = (
        client
        .branches
        .list_branches(repository=repo)
        .get('results')
    )
    branche_names = [b.get('id') for b in branches] + ['None']

dbutils.widgets.dropdown('Branch', 'None', branche_names, 'Branch')
branch = dbutils.widgets.get("Branch")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Global Variables

# COMMAND ----------

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
table_name = (notebook_path.rsplit('/', 1)[-1])

if repo.lower() == 'none':
    target_delta_path = get_cloud_storage_path(zone='curatedzone',kind='transactionaldata',table_name=table_name)
else:
    target_delta_path = get_lakefs_path(repo=repo,branch=branch,kind='transactionaldata',table_name=table_name)

print(f'Target delta path: {target_delta_path}')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Source dataframe

# COMMAND ----------

path_table_green = get_cloud_storage_path(zone='standardizedzone',kind='transactionaldata',table_name='taxigreentrip')
path_table_yellow = get_cloud_storage_path(zone='standardizedzone',kind='transactionaldata',table_name='taxiyellowtrip')
paths = [path_table_green,
         path_table_yellow]
cols = '*'

df_trip_lst = list(
        map(
            read_delta_table,
            itertools.repeat(spark),
            paths,
            itertools.repeat(cols)
        )
    )

df_trip = reduce(pyspark.sql.dataframe.DataFrame.unionByName, df_trip_lst)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Remove duplicates

# COMMAND ----------

cols = ['VendorID','PickupDate','DropOffDate','PickupLocationID','DropOffLocationID','TripTypeID','RateCodeID','PaymentTypeID']
partition=Window.partitionBy(cols).orderBy(desc('PickupDate'))

df_trip = (
    df_trip
    .withColumn('RowNum',row_number().over(partition))
    .where('RowNum = 1')
    .drop('RowNum')
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Merge into target

# COMMAND ----------

target_delta_table = DeltaTable.forPath(spark, target_delta_path)
(
    target_delta_table.alias('trg')
    .merge(
        source = df_trip.alias('src'),
        condition = 'src.VendorID = trg.VendorID and src.PickupDate = trg.PickupDate and src.DropOffDate = trg.DropOffDate and src.PickupLocationID = trg.PickupLocationID and src.DropOffLocationID = trg.DropOffLocationID and src.TripTypeID = trg.TripTypeID and src.RateCodeID = trg.RateCodeID and src.PaymentTypeID = trg.PaymentTypeID'
    )
    .whenMatchedUpdateAll(
        condition = 'src.PassengerCount <> trg.PassengerCount or src.TripDistance <> trg.TripDistance or src.TotalAmount <> trg.TotalAmount or src.TripTimeMn <> trg.TripTimeMn'
    )
    .whenNotMatchedInsertAll()
    .execute()
)