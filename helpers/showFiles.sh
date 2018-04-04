#!/bin/bash

fileCount0=$(find /hdfs:/process/m=03/ -iname '*.processed.json' | wc -l)
filePaths=$(find /hdfs:/process/m=03/ -iname '*.processed.json')
echo -n "There are $fileCount0 parsed JSON files"
printf "\n\n"

for file in ${filePaths}
do
  echo ${file}
done
