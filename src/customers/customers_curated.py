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
        .appName("Customers Curated Transformation") \
        .getOrCreate()


# Read from customers trusted
customers = spark.read.parquet("s3://data-lake-udacity-524095156763/accelerometer/trusted/")

# Accelerometer from trusted
accelerometer = spark.read.parquet("s3://data-lake-udacity-524095156763/accelerometer/trusted/")

# Step Trainer from landing
step_trainer = spark.read.json("s3://data-lake-udacity-524095156763/accelerometer/landing/")

customer_unique_serial_number = customers.dropDuplicates(["serialNumber"])

step_trainer  = step_trainer.withColumnRenamed("serialNumber", "st_serialNumber")

step_trainer_unique_serial_number = step_trainer.dropDuplicates(["st_serialNumber"])

# Customers JOIN Step Trainer
customers_join_step_trainer = customer_unique_serial_number.join(step_trainer_unique_serial_number,
                                            customer_unique_serial_number.serialNumber == step_trainer_unique_serial_number.st_serialNumber,
                                            "inner")

accelerometer_unique_users = accelerometer.dropDuplicates(["user"])

# Customers x Accelerometer
customer_join_accelerometer = customers_join_step_trainer.join(accelerometer_unique_users,
               customers_join_step_trainer.email == accelerometer_unique_users.user,
               "inner")

customer_drop_step_trainer = customer_join_accelerometer.drop(*step_trainer.columns)

customer_drop_accelerometer = customer_drop_step_trainer.drop(*accelerometer.columns)

customer_drop_accelerometer.write.mode("overwrite").parquet("s3://data-lake-udacity-524095156763/accelerometer/curated/")
