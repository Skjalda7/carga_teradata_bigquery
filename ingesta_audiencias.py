from datetime import timedelta, datetime
import pandas as pd
import teradatasql
from google.oauth2 import service_account

from airflow.decorators import task, dag
from airflow.hooks.base_hook import BaseHook


# ------- DAG instantiation -------

DEFAULT_ARGS = {
	'owner': 'airflow',
	'depends_on_past': False,
	'retries': 1,
	'retry_delay': timedelta(minutes=5)
	}

doc_md = """
### Ingesta_Audiencias
#### Proposito
Carga diariamente la tabla BANCO.Audiencias desde Teradata a BigQuery.
#### Notas
- La carga reemplaza los datos existentes en BigQuery.
- [Documentacion](https://gpdya.atlassian.net/)
"""

# ------- DAG builder -------

@dag(dag_id='Ingesta_Audiencias',
     doc_md = doc_md,
     description='Carga de tabla BANCO.Audiencias a BigQuery',
     default_args=DEFAULT_ARGS,
     start_date=datetime(2022, 11, 1),
     catchup=False,
     schedule_interval='@daily',
     tags=['GCP', 'campañas', 'bigquery'])

def my_dag():

    @task
    def homebanking():
        conn = BaseHook.get_connection(f'Teradata')
        con_tera = teradatasql.connect(None,
                                   host=f"localhost",
                                   user=f"{conn.login}",
                                   password=f"{conn.password}",
                                   column_name='false')

        tsql = con_tera.cursor()

        query = ('SELECT * FROM BANCO.Audiencias')
        tsql.execute(query)
        col_headers = [i[0] for i in tsql.description]
        rows = [list(i) for i in tsql.fetchall()]
        data = pd.DataFrame(rows, columns=col_headers)
        tsql.close()

        destination_table = 'banco.audiencias'
        project_id = 'my-project-id'
        file_json = 'credenciales.json'
        path_file = '/home/airflow/airflow/dags/ingesta_audiencias/files/'

        credentials = service_account.Credentials.from_service_account_file(f"{path_file}{file_json}")

        data.to_gbq(destination_table=destination_table,
                    project_id=project_id,
                    if_exists='replace',
                    table_schema=[{'name': 'fecha_carga', 'type': 'DATE'},
                                  {'name': 'inicio_campania', 'type': 'DATE'},
                                  {'name': 'fin_campania', 'type': 'DATE'}],
                    credentials=credentials)

        return print("Cargada {} filas en {}: {}.".format(data.shape[0], project_id, destination_table))

    audiencias()

carga_datos_gcp = my_dag()
