1. Add your snowflake credentials in Secret manager:
       {
        "sfURL": "https://nk40936.ap-south-1.aws.snowflakecomputing.com",
        "sfWarehouse": "",
        "sfDatabase": "",
        "sfSchema": "",
        "sfRole" : "",
        "sfuser" : "",
        "sfpassword" : ""
      }
2. create IAM role with required permission:

3. create s3 bucket:

4. create glue job with code provided in file (spotifygluejob.py)
5. create AWS lambda and event bridge with provided lambda script(AWSlambdafunction.py):

7. 
