import sys
from pyspark.sql import SparkSession
import boto3
 
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Load S3 to Snowflake") \
    .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.14,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.1") \
    .getOrCreate()
 
# Create a Secrets Manager client
secrets_client = boto3.client('secretsmanager')
 
# Specify the name of your secret in Secrets Manager
secret_name = "snowconnection"  # Replace with your secret name
 
response = secrets_client.get_secret_value(SecretId=secret_name)
secret = eval(response['SecretString'])  # Evaluate the JSON string to a dictionary
 
# Snowflake connection properties
snowflake_options = {
    "sfURL": secret['sfURL'],
    "sfDatabase": secret['sfDatabase'],
    "sfSchema":secret['sfSchema'] ,
    "sfWarehouse": secret['sfWarehouse'],
    "sfRole": secret['sfRole'],
    "sfuser": secret['sfuser'],
    "sfpassword": secret['sfpassword']
}
 
# Load CSV from S3 into DataFrame
s3_path = "s3://spotiibucket/spotii/"
df = spark.read.option("header", "true").csv(s3_path)
 
# Write DataFrame to Snowflake
df.write \
    .format("snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", "spoitifydata") \
    .mode("overwrite") \
    .save()
 
print("Data successfully loaded into Snowflake!")
