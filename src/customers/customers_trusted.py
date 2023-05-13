import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("Customers Trusted Zone") \
        .getOrCreate()

customers = spark.read.json("s3://data-lake-udacity-524095156763/customers/landing/")

customers_agreed_research = customers.where(customers.shareWithResearchAsOfDate.isNotNull())

customers_agreed_research.write.parquet("s3://data-lake-udacity-524095156763/customers/trusted/")
