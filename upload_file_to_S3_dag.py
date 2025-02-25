from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3

# AWS Configuration
AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""
S3_BUCKET_NAME = "spotiibucket"
LOCAL_FILE_PATH = "/home/vaibhav/s3filefolder/user_data.csv"
S3_FILE_PATH = "data_files/"  # Path inside S3 bucket
# S3_FILE_PATH = "folder-in-s3/file.csv"  # Path inside S3 bucket

def upload_all_files_to_s3():
    """Uploads all files from a local directory to an S3 bucket."""
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    # Iterate through all files in the local directory
    for file_name in os.listdir(LOCAL_DIRECTORY):
        local_file_path = os.path.join(LOCAL_DIRECTORY, file_name)

        if os.path.isfile(local_file_path):  # Ensure it's a file, not a directory
            s3_file_path = f"{S3_FOLDER}{file_name}"  # Destination in S3
            with open(local_file_path, "rb") as file:
                s3_client.upload_fileobj(file, S3_BUCKET_NAME, s3_file_path)
            print(f"Uploaded {local_file_path} to S3://{S3_BUCKET_NAME}/{s3_file_path}")

# Define Airflow DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 24),
    "retries": 1
}

dag = DAG(
    "upload_csv_to_s3_26",
    default_args=default_args,
    schedule_interval=None,  # Manually triggered
    catchup=False
)

upload_task = PythonOperator(
    task_id="upload_csv",
    python_callable=upload_all_files_to_s3,
    dag=dag
)

upload_task

