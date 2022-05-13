import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from CommentAggregation.Executor.Executor import Executor
import os


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kinesis-asl_2.12:3.1.1 pyspark-shell'

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

window_size = 1*60

path_to_save = 's3://mbucket111111/Agg'

limit = 3
executor = Executor()

executor.execute(path_to_save, limit, spark, window_size)

job.commit()


