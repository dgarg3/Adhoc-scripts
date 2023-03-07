'''glue etl script to process stream from kinesis'''
import sys
import datetime
import base64
import boto3
from pyspark.context import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['PROCESS_NAME', 'S3_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['PROCESS_NAME'], args)

# S3 sink locations
output_path = args['S3_PATH']

s3_target = output_path + "inventory_kpi/"
checkpoint_location = output_path + "cl/"
temp_path = output_path + "temp/"


def process_batch(data_frame, batchid):
    '''function to  to process batch'''
    if data_frame.count() > 0:

        dynamic_frame = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")

        dynamic_frame.printSchema()

        s3sink = glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            connection_options={"path": s3_target},
            format="parquet")



# Source catalog table which was created manually on the top of kinesis stream
sourceData = glueContext.create_data_frame.from_catalog(
    database="inventory-db",
    table_name="inventory-stream",
    transformation_ctx="",
    additional_options={"startingPosition": "TRIM_HORIZON", "inferSchema": "true"})

sourceData.printSchema()

glueContext.forEachBatch(frame=sourceData,
                         batch_function=process_batch,
                         options={"windowSize": "100 seconds",
                                  "checkpointLocation": checkpoint_location}
                         )
job.commit()
