import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1739468857116 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://sssample/spotifysource/spotify_history.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1739468857116")

# Script generated for node Snowflake
Snowflake_node1739468876505 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1739468857116, connection_type="snowflake", connection_options={"autopushdown": "on", "dbtable": "spotable", "connectionName": "Snowflake connection", "preactions": "CREATE TABLE IF NOT EXISTS yami.spotable (spotify_track_uri string, ts string, platform string, ms_played string, track_name string, artist_name string, album_name string, reason_start string, reason_end string, shuffle string, skipped string);", "sfDatabase": "aloy", "sfSchema": "yami"}, transformation_ctx="Snowflake_node1739468876505")

job.commit()
