from airflow.sdk import dag, task

@dag
def first_dag():

    @task
    def first_task():
        print("This is my first task")

    @task
    def second_task():
        print("This is my second task")

    @task
    def third_task():
        print('This is my third task')

    first_task() >> second_task() >> third_task()    

# Instantiate the dag
first_dag()