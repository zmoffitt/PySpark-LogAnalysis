#!/bin/bash
bold=$(tput bold)
normal=$(tput sgr0)

printf "Enumerating the objects, please wait...\n\n"
fileCount=$(find /hdfs:/process/processed/m=03/ -iname '*.json' | wc -l)
filePaths=$(find /hdfs:/process/processed/m=03/ -iname '*.json')
echo -n "There are ${bold}$fileCount ${normal} JSON files to process"
printf "\n\n"
read -n 1 -s -r -p "Press any key to continue..."
printf "\n-------------------------\n"
printf "\nCalling JSON line sum loops...\n"

count=1
totalLines=0
for file in ${filePaths}
do
  echo "${bold}Counting file ${count} of ${fileCount}:${normal} ${file}"
  thisFileCount=$(cat ${file} | wc -l)
  printf "\t"
  echo "Found ${thisFileCount} lines."
  printf "\n"
  totalLines=$[$totalLines +${thisFileCount}]
  echo "There are now ${bold}${totalLines}${normal} lines in the count"
  printf "\n"
  count=$[$count +1]
done

printf "\n-------------------------\n"
echo "Total Lines: ${totalLines}"
printf "\n-------------------------\n"
