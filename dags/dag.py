from airflow.operators.python import PythonOperator
from airflow.operators.generic_transfer import GenericTransfer
from airflow import DAG

with  DAG(dag_id='Generic Transfer Operator', 
        schedule_interval=None ,
        start_date=datetime(2023,2,2) ,
        catchup=False) as dag :

        load_upload_data=GenericTransfer(
                        task_id='load_upload_data' ,
                        sql="select * from datbricksOnelake.FactInternetSales_Consold"
                        destination_table ="datbricksOnelake.FactInternetSales_Consold"  ,
                        source_conn_id="GenericLAkehouse"  ,
                        destination_conn_id="GenericLAkehouse" 
                        dag=dag

                    )

load_upload_data
