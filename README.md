# Dynamic DAG
## Objective 📝
Load a table in BigQuery from Teradata database.

## Functionality ⚙️
A single task called homebanking queries a table from Teradata which contains information about client segmentation for a target product. This information is ingested by a BigQuery table through a pandas function.

The process is simple: from Teradata, runs a Select query saving the data into a Dataframe variable and then its loads at BigQuery passing a table_schema for fields with date data type so it can be respected the original format.

## Graph & Grid from Airflow UI 📊
![graph from airflow ui](https://github.com/Skjalda7/carga_teradata_bigquery/blob/main/homebanking.png)
