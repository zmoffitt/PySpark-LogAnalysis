#!/bin/bash
bold=$(tput bold)
normal=$(tput sgr0)

printf "Enumerating the objects, please wait...\n\n"
fileCount=$(find /hdfs:/process/ -iname 'PT5M.json' | wc -l)
filePaths=$(find /hdfs:/process/ -iname 'PT5M.json')
echo -n "There are ${bold}$fileCount ${normal} unparsed JSON files to be deleted"
printf "\n\n"
read -n 1 -s -r -p "Press any key to continue..."
printf "\n-------------------------\n"
printf "\nCalling JSON removal loops...\n"

count=1
for file in ${filePaths}
do
  echo "${bold}Removing ${count}:${normal} ${file}"
  printf "\t"
  rm -fv ${file}
  count=$[$count +1]
done
