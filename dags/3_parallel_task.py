from airflow.sdk import dag,task

@dag ( dag_id = "parallel_dag")
def parallel_dag():

    @task.python
    def first_task(**kwargs):
        ti = kwargs['ti']
        data = { "name" : "Adilkhan", "age" : 25}
        ti.xcom_push( key = "return_value" , value = data)

    @task.python
    def second_task(**kwargs):
        ti = kwargs['ti']
        pulled_data = ti.xcom_pull(task_ids ="first_task")
        pulled_data["name"] = pulled_data["name"].upper()
        ti.xcom_push(key = "return_value" , value = pulled_data)

    @task.python
    def third_task(**kwargs):
        ti = kwargs['ti']
        pulled_data = ti.xcom_pull(task_ids ="first_task")
        pulled_data["mobile"] = "8686858576"
        ti.xcom_push(key = "return_value" , value = pulled_data)        


    @task.python
    def final_task(**kwargs):
        ti = kwargs['ti']
        task1_data = ti.xcom_pull(task_ids = "second_task")
        task2_data = ti.xcom_pull(task_ids = "third_task")
        print(f'{task1_data} :: {task2_data}')

    first_task = first_task()
    second_task = second_task()
    third_task = third_task()
    final_task = final_task()

    first_task >> [second_task , third_task] >> final_task

#Initiating Dag
parallel_dag()    


