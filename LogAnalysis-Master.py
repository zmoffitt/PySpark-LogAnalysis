#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Converted from .ipynb (Jupyter Notebook) to .py (Python script)
# Written/executed on Microsoft Azure HDInsight cluster [Spark 2.1 on Linux (HDI 3.6)]

__author__ = "Zachary Moffitt"
__version__ = "1.0.1"


%%configure
{
    "name":"LogAnalysis",
    "executorMemory": "4G",
    "executorCores": 8,
    "numExecutors": 32,
    "driverMemory": "8G",
    "driverCores": 8
}

from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json
import re
import requests

# Set the environment to recursively read directories
sc._jsc.hadoopConfiguration().set('mapreduce.input.fileinputformat.input.dir.recursive', 'true')
blobPath = "wasbs://insights-logs-dataplanerequests@<redacted>.blob.core.windows.net/processed/"

# Find set of json files in blobPath (WASBS connection) for Group 1
input0 = sc.textFile(blobPath + 'm=03/d=21/')
input1 = sc.textFile(blobPath + 'm=03/d=22/')
input2 = sc.textFile(blobPath + 'm=03/d=23/')
input3 = sc.textFile(blobPath + 'm=03/d=24/')
input4 = sc.textFile(blobPath + 'm=03/d=25/')

# Find set of json files in blobPath (WASBS connection) for Group 2
input5 = sc.textFile(blobPath + 'm=03/d=26/')
input6 = sc.textFile(blobPath + 'm=03/d=27/')
input7 = sc.textFile(blobPath + 'm=03/d=28/')
input8 = sc.textFile(blobPath + 'm=03/d=29/')
input9 = sc.textFile(blobPath + 'm=03/d=30/')

# Find set of json files in blobPath (WASBS connection) for Group 3
inputA = sc.textFile(blobPath + 'm=03/d=31/')
inputB = sc.textFile(blobPath + 'm=04/d=01/')
inputC = sc.textFile(blobPath + 'm=04/d=02/')
inputD = sc.textFile(blobPath + 'm=04/d=03/')

# Create RDD union for groups 1/2/3
inGrp0 = input0.union(input1.union(input2.union(input3.union(input4))))
inGrp1 = input5.union(input6.union(input7.union(input8.union(input9))))
inGrp2 = inputA.union(inputB.union(inputC.union(inputD)))

# Merge all RDDs together and read as JSON
rddAll = inGrp0.union(inGrp1.union(inGrp2))

# Merge the resulting RDD merge, read as JSON and set variable to continue
jsonData = spark.read.json(rddAll)

# Fetch the global inferred schema
jsonData.printSchema()

# Construct the array into multiple sets in table format to process with SQL-like commands
thisList = jsonData.select('category','operationName','resourceId','time', 'properties.*')

# Create temp table "Master" that will hold all the records for further processing
thisList.registerTempTable("Master")
allRecords = spark.sql("SELECT * from Master")

# Print the total number of rows that exist in the 'Master' temp table
print('Loaded {} records into "Master" table.'.format(allRecords.count()))

#spark.sql("SELECT operationName,collectionRid,duration,partitionId from Master limit 1").show()
allRecords.groupBy("operationName", "collectionRid", "partitionId").count()\
    .coalesce(1).repartition(1)\
    .write.mode("overwrite")\
    .csv('wasbs://insights-logs-dataplanerequests@<redacted>.blob.core.windows.net/hdifiles/GroupedwPartition.csv', header='true')

# Create grouped ops output with aggregated requestCharge (sum), avg duration, and avg request charge per op
from pyspark.sql import functions as func
thisList.groupBy("operationName").agg(func.sum("requestCharge"), func.avg("duration"), func.avg("requestCharge"))\
    .coalesce(1).repartition(1)\
    .write.mode("overwrite")\
    .csv('wasbs://insights-logs-dataplanerequests@<redacted>.blob.core.windows.net/hdifiles/Averages.csv', header='true')

# Transform the date for grouping
dateTransformed = spark.sql("select to_date(time) as date, operationName, partitionId, statusCode, collectionRid from Master")
dateTransformed.registerTempTable("TransformedDateTable")

# Read "TransformedDateTable" table into totals val, use RDD (df) op to count total operations by distinct ops
totals = dateTransformed.groupBy("date","operationName", "collectionRid").count()

# Write the resulting DFF back to blob storage
totals.coalesce(1).repartition(1).write.mode("overwrite")\
    .csv('wasbs://insights-logs-dataplanerequests@<redacted>.blob.core.windows.net/hdifiles/totalsByDay.csv', header='true')

print('Successfully aggregated transformed data with {} final results.'.format(totals.count()))

# Create Ops count by group [count by activityId-unique]
results = allRecords.groupBy("operationName").agg(count("activityId").alias("count"))
results.coalesce(1).repartition(1).write.mode("overwrite")\
    .csv('wasbs://insights-logs-dataplanerequests@<redacted>.blob.core.windows.net/hdifiles/countByOpName.csv', header='true')

# Write all of the records into a giant 5-partition CSV
spark.sql("SELECT activityId, time, operationName, collectionRid, partitionId,\
            region, clientIpAddress, requestLength, requestResourceId, userAgent,\
            statusCode, requestCharge, duration from Master")\
    .coalesce(1).repartition(5).write.mode("overwrite")\
    .csv('wasbs://insights-logs-dataplanerequests@<redacted>.blob.core.windows.net/hdifiles/Major.csv', header='true')
