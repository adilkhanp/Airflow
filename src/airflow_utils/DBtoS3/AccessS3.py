### step 1 ---- install boto3
import boto3
from dotenv import load_dotenv
import os
load_dotenv()

def access_s3():
    s3_client = boto3.client('s3','eu-north-1',aws_access_key_id = os.environ.get("aws_access_key_id"),
                         aws_secret_access_key = os.environ.get("aws_access_key_password"))

    return s3_client

    # buckets = s3_client.list_buckets()["Buckets"]


    # for buck_list in buckets:
    #     print(f"{buck_list['Name']}")

def upload_file_to_bucket(file_path):
    s3_client = access_s3()
    s3_client.upload_file(file_path,'adilkhanp-s3-bucket','EMPLOYEE_DETAILS.CSV')
    return "Successfully Uploaded"



