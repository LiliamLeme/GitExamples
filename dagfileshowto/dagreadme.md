## Step by step for Fabric

1) Create the Airflow IR

![image](https://github.com/LiliamLeme/GitExamples/assets/62876278/29740187-c029-40df-ad8f-661dc08ccf71)



2. Create a Service Principal with a secret and add as contributor in Microsoft Fabric Workspace.

   Take note of the Application ID, Tenant ID and the Secret.

![image](https://github.com/LiliamLeme/GitExamples/assets/62876278/3b131cf2-2e15-4e9a-8e3d-c8971a3b3f15)


   

3. Click in view airflow connections and create a new connection for Azure Data Factory 

Add a new connection

![image](https://github.com/LiliamLeme/GitExamples/assets/62876278/1942eade-7cdf-4ca4-9342-c9bb11d69e69)


Name the connection in connection id, type will be Azure Data Factory. Fill with the Client ID ( SP ID), secret, Tenant ID and also add the subscription id

![image-20240124165658882](C:\Users\lilem\AppData\Roaming\Typora\typora-user-images\image-20240124165658882.png)

4. Click in Configure Airflow, add the requirement:

   apache-airflow-providers-microsoft-azure

5. From File Storage, you will need a gihub repository with a folder called dags. I use personal token for authentication

![image](https://github.com/LiliamLeme/GitExamples/assets/62876278/d06eee4e-ecf9-48a1-b9da-c14f30507367)



Here is my github:

![image](https://github.com/LiliamLeme/GitExamples/assets/62876278/1da09d52-a321-4e0b-9ed8-4b4353eda04b)


6. For the code:

   `

   ```
   from datetime import datetime, timedelta
   
   from airflow.models import DAG
   from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
   
   #try:
   #    from airflow.operators.empty import EmptyOperator
   #except ModuleNotFoundError:
   #    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore
   #from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
   #from airflow.providers.microsoft.azure.sensors.data_factory import AzureDataFactoryPipelineRunStatusSensor
   #from airflow.utils.edgemodifier import Label
   
   
   with DAG(
       dag_id="example_adf_run_pipeline",
       start_date=datetime(2024, 1, 23),
       schedule_interval="@daily",
       catchup=False,
       default_args={
           "retries": 1,
           "retry_delay": timedelta(minutes=3),
           "azure_data_factory_conn_id": "Azure_data_factory", #This is a connection created on Airflow UI
           #"factory_name": "NextSynapse12",  # This can also be specified in the ADF connection.
           #"resource_group_name": "<ResourceGroupName>",  # This can also be specified in the ADF connection.
       },
       default_view="graph",
   ) as dag:
   
   
       run_adf_pipeline = AzureDataFactoryRunPipelineOperator( 
           task_id="run_adf_pipeline", 
           pipeline_name="pipeline_test_1",  
           parameters={"justaparameter": "1"}, 
       ) 
   
   
   run_adf_pipeline
   
      
   ```

7. This is the Pipeline from Fabric I am trying to run:

   
![image](https://github.com/LiliamLeme/GitExamples/assets/62876278/f92c99cf-cd4e-4aa0-919f-1139cb93198e)


8. Once you save the configuration inside Github and the files and folder are in place, open Monitoring Airflow and click in refresh. It may take a few seconds to appear.

   
![image](https://github.com/LiliamLeme/GitExamples/assets/62876278/035358be-830b-4c11-9ad6-4851278721fa)
