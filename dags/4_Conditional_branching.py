from airflow.sdk import dag,task

import json 

@dag ( dag_id = "branching_dag")
def branching_dag():

    @task.python
    def master_data_task(**kwargs):
        ti = kwargs['ti']
        data = '''{
                    "Name" : "Adilkhan Pattan",
                    "Age" : 33,
                    "Joining_Date" : "2017-03-06",
                    "Department" : "Sales"
                    }'''
        ti.xcom_push(key ="return_value" , value = data)

    @task.python
    def salary_task(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids = "master_data_task")
        data = json.loads(data)
        data["salary"] = "10000"
        ti.xcom_push( key = "return_value" , value = json.dumps(data))

    @task.python
    def commission_task(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids = "master_data_task")
        data = json.loads(data)
        data["commission"] = "10000"
        ti.xcom_push( key = "return_value" , value = json.dumps(data))

    @task.branch
    def decider_task(**kwargs):
        ti = kwargs['ti']
        dept = ti.xcom_pull(task_ids = "master_data_task")
        data = json.loads(dept)
        dept = data["Department"]

        if dept == "Sales":
            data["task"] = "commission_task"
            ti.xcom_push(key = "return_value" , value = json.dumps(data))
            return "commission_task"
        else :
            data["task"] = "salary_task"
            ti.xcom_push(key = "return_value" , value = json.dumps(data))            
            return "salary_task"

    @task.python(trigger_rule="none_failed_min_one_success")
    def result_task(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids = "decider_task")
        data = json.loads(data)
        if data["task"] == "commission_task":
            data = ti.xcom_pull(task_ids = "commission_task")
        else :
            data = ti.xcom_pull(task_ids = "salary_task")
        print ( f'The final output is :: {data}')

    #Initialize the dependencies
    master_data_task = master_data_task()
    salary_task = salary_task()
    commission_task = commission_task()
    decider_task = decider_task()
    result_task = result_task()

    master_data_task >> decider_task >> [salary_task , commission_task] >> result_task

branching_dag()        
