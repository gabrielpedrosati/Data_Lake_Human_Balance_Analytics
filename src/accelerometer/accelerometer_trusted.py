import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job

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
        .appName("Accelerometer Trusted") \
        .getOrCreate()


# Accelerometer Landing Zone
accelerometer = spark.read.json("s3://data-lake-udacity-524095156763/accelerometer/landing/")

# Customers Trusted Zone - não precisa filtrar por research purposes
customers = spark.read.parquet("s3://data-lake-udacity-524095156763/customers/trusted/")

customers_unique_values = customers.dropDuplicates(["email"])

accelerometer_unique_values = accelerometer.dropDuplicates(accelerometer.columns)

accelerometer_join_customers = accelerometer_unique_values.join(customers_unique_values,
                                 accelerometer_unique_values.user == customers_unique_values.email,
                                 "inner")

accelerometer_drop_customer_columns = accelerometer_join_customers.drop(*customers.columns)

# save to accelerometer_trusted
accelerometer_drop_customer_columns.write.mode("overwrite").parquet("s3://data-lake-udacity-524095156763/accelerometer/trusted/")