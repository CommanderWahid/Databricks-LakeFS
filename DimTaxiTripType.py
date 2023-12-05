# Databricks notebook source
# MAGIC %md
# MAGIC #### Imports

# COMMAND ----------

from delta.tables import DeltaTable
from common.helpers import (init_lakefs_client,get_lakefs_path,get_cloud_storage_path,read_delta_table)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Lake FS client

# COMMAND ----------

client = init_lakefs_client(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Widgets

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
# MAGIC #### Global variables

# COMMAND ----------

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
table_name = (notebook_path.rsplit('/', 1)[-1])

if repo.lower() == 'none':
    target_delta_path = get_cloud_storage_path(zone='curatedzone',kind='referencedata',table_name=table_name)
else:
    target_delta_path = get_lakefs_path(repo=repo,branch=branch,kind='referencedata',table_name=table_name)

print(f'Target delta path: {target_delta_path}')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Source dataframe

# COMMAND ----------

path = get_cloud_storage_path(zone='standardizedzone',kind='referencedata',table_name='taxitriptype')
df_trip_type = read_delta_table(spark,path,'*')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Merge into target

# COMMAND ----------

target_delta_table = DeltaTable.forPath(spark, target_delta_path)
(
    target_delta_table.alias('trg')
    .merge(
        source=df_trip_type.alias('src'),
        condition = 'src.TripTypeID = trg.TripTypeID'
    )
    .whenMatchedUpdateAll(
        condition = 'src.TripType <> trg.TripType'
    )
    .whenNotMatchedInsertAll()
    .execute()
)