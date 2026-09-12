from airflow.sdk import dag,task

@dag(dag_id = "xcom_dag")
def xcom_dag():

    @task.python
    def first_task():
        return { "name" : "Adilkhan", "age" : 25}

    @task.python
    def second_task(data : dict):
        print ( f"Name is {data["name"]}")

    first = first_task()
    second = second_task(first)

#Initialize dag
xcom_dag()    

