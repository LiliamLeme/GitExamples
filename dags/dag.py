from airflow.operators.python import PythonOperator
from airflow.operators.generic_transfer import GenericTransfer
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
from airflow import DAG

with  DAG(dag_id='Generic_Transfer_Operator', 
        start_date=datetime(2024, 1, 23),
        schedule_interval="@daily",
        catchup=False) as dag :

        load_upload_data=GenericTransfer(
                        task_id='load_upload_data' ,
                        sql=r"""        select* from sys.tables;        """,
                        destination_table =r"""sys.tables;        """ ,
                        source_conn_id="sqllitle_FabricWH"  ,
                        destination_conn_id="sqllitle_FabricWH" ,
                        dag=dag

                    )


load_upload_data
