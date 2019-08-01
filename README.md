# PySpark-LogAnalysis

#### Cosmos DB \(DocumentDB\) Insights Log Processor

The **pyspark-loganalysis** .py script helps to analyze large amounts of operational log data written by Cosmos DB \(DocumentDB\) and Application Insights. When a large amount of transactions are executed in a short timeframe, the log file \(PT5M.json\) can be &gt; 1GB which prevents quick analysis on data that has been collected for several days, weeks, or months.

To prepare these files, the `doTransformations.sh` script uses `jq` to explode the array so that we can import the JSON files on a line-by-line basis.

### Prerequisites

From the command line, verify/install [jq](https://stedolan.github.io/jq/):

```text
$ jq

jq - commandline JSON processor [version 1.5-1-a5b5cbe]
Usage: jq [options] <jq filter> [file...]
```

## Usage

### Run JSON transformation on insights-logs-dataplanerequests files

These files were available on the HDInsight Cluster already since the Azure Storage Blob was made available by scripting the command on each head node. Update the `fileCount` and `filePaths` variables in the `doTransformations.sh` script before executing:

```text
$ sudo sh doTransformation.sh
```

and there will be an output similar to:

```text
Enumerating the objects, please wait...

There are 1402 JSON files to be transformed

Press any key to continue...
```

### \(Optional\) Delete the untransformed files

Update `fileCount` and `filePaths` in the `doDelete.sh` file to match the path of `doTransformations.sh`: $ sudo sh doDelete.sh

```text
Enumerating the objects, please wait...

There are 1402 unparsed JSON files to be deleted

Press any key to continue...
```

### Process data on Spark Cluster

In the Jupyter \(.ipynb\) Notebook, there are 18 cells that seperate the execution stages. To run from terminal:

```text
$ ./bin/spark-submit --files LogAnalysis-Master.py
```

or

```text
$ ./bin/pyspark LogAnalysis-Master.py
```

