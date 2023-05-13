# Data Lake

## Introduction

In this project, a data lake solution will be developed for sensor data that trains machine learning algorithms.

AWS infrastructure will be used to create storage zones (landing, trusted and curated), data catalog, data transformations between zones and queries in semi-structured data.

## Project Details

The STEDI Team has been hard at work developing a hardware **STEDI** **Step Trainer** that:

- trains the user to do a STEDI balance exercise;
- and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
- has a companion mobile app that collects customer data and interacts with the device sensors.

STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. *Privacy will be a primary consideration in deciding what data can be used.*

Some of the early adopters have agreed to share their data for research purposes. **Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model**.

## Requirements

### Customers Data

- **Trusted Zone**: only store the Customer Records who agreed to share their data for research purposes.

- **Curated Zone**: only includes customers who have accelerometer data *and* have agreed to share their data for research.

### Accelerometer Data

- **Trusted Zone**: only store Accelerometer Readings from customers who agreed to share their data for research purposes.

### Step Trainer Data

- **Trusted Zone**: contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (customers_curated).

### Machine Learning Data

- **Curated Zone**: aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data.

## Data

**1. Customer Records (from fulfillment and the STEDI website):**

JSON file contains the following fields:

![](/imgs/customers_json.svg)

**2. Step Trainer Records (data from the motion sensor):**

JSON file contains the following fields:

![](/imgs/step_trainer_json.svg)

**3. Accelerometer Records (from the mobile app):**

JSON file contains the following fields:

![](/imgs/accelerometer_json.svg)

## Error reading JSON files

The files are not read by Spark, as they are not a single JSON object, they are several JSON objects together, the solution to the problem is handling the files by adding keys at the beginning and end and separating them with a comma. The script I used to handle the files is at:[transform_to_json.py]().

You can read more at [JSON only reading the first object in spark](https://stackoverflow.com/questions/47458463/read-json-only-reading-the-first-object-in-spark).

## Architecture

![](/imgs/Data%20Lake.svg)

### Data Lake Zones

| Zone    | Description                      |
| ------- | -------------------------------- |
| Landing | New files landing zone. |
| Trusted | Treated and cleaned files.      |
| Curated | Files with business rules and ready for consumption.  |

## Services

| Service         | Description                                                                     |
| --------------- | ------------------------------------------------------------------------------- |
| Pyspark         | Script to clean and transform the data.                    |
| Glue        | Service that allows performing the ETL process between storage zones. |
| Glue Tables | Run a crawler that connects to one or more data stores, determines the data structures, and writes tables into the Data Catalog.                                   |
| Athena      | Service to perform queries on data stored in S3.               |
| S3          | Service to create the zones that store the data.                           |

### Preparing Glue Service Role

Create a role for Glue that allows access to S3 buckets.

[Create an IAM Role](https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html)

## Preparing files

**Important:**  Command below will ingest the data into the Landing Zone, for this it is necessary to create the bucket for the Data Lakehouse and after downloading the data through AWS Cloud Shell:

```shell
# Execute o comando no AWS Cloud Shell
./s3-sync-all.sh
```

## AWS Glue Scripts

The scripts used to process the data are in the `src/` folder.

File handling flow:

![](/imgs/Data%20Lineage.svg)

## AWS Glue Tables

### Accelerometer and Customers Landing

The SQL file used to create the Glue tables can be found here: [customers_landing.sql](./sql/customer_landing.sql) and [accelerometer_landing.sql](/sql/accelerometer_landing.sql).

## Athena

### Customers and Accelerometer Landing

Customers

![](/imgs/customer_landing.png)

Accelerometer

### ![](/imgs/accelerometer_landing.png)

### Customers Curated

![](/imgs/customers_curated.png)
