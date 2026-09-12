from airflow.sdk import dag,task

from airflow_utils.DBtoS3 import AccessS3 as s3
from airflow_utils.DBtoS3 import AccessDatabase as db

@dag(dag_id = "DbToS3")
def DbToS3():

    @task.python
    def dbTask(**kwargs):
        ti = kwargs['ti']        
        ora_file = db.process_oracle_data()
        ti.xcom_push(key='return_value' , value = ora_file)

    @task.python
    def moveToS3(**kwargs):
        ti = kwargs['ti']
        ora_file = ti.xcom_pull(task_ids="dbTask")
        result = s3.upload_file_to_bucket( file_path = ora_file)
        print ( result)

    dbTask = dbTask()
    moveToS3 = moveToS3()

    dbTask  >> moveToS3

DbToS3()        