import oracledb as ora
import pandas as pd
import sqlalchemy

username="hr"
password="hr"
dsn="host.docker.internal:1521/orcl"

def process_oracle_data():

    connection = ora.connect(user=username,password=password,dsn=dsn)

    cursor = connection.cursor()

    cursor.execute("select * from employees")

    df=pd.DataFrame(cursor.fetchall())
    cursor.close()
    connection.close()
    file_path = '/opt/airflow/output/FromDatabaseEmployees.csv'
    df.to_csv(file_path,index=False)
    return file_path