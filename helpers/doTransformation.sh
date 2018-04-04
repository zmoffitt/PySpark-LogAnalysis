#!/bin/bash
bold=$(tput bold)
normal=$(tput sgr0)

printf "Enumerating the objects, please wait...\n\n"
fileCount0=$(find /hdfs:/process/m=03/ -iname '*.json' | wc -l)
filePaths=$(find /hdfs:/process/m=03/ -iname '*.json')
echo -n "There are ${bold}$fileCount0 ${normal} JSON files to be transformed"
printf "\n\n"
read -n 1 -s -r -p "Press any key to continue..."
printf "\n-------------------------\n"
printf "\nCalling JSON transformation loops...\n"

for file in ${filePaths}
do
  thisFilePath="/hdfs:/process/${file:15:-9}"
  thisFileTemp="${thisFilePath}PT5M.temp.json"
  thisParseRes="${thisFilePath}PT5M.processed.json"
  echo "Processing ${bold}${file}${normal}"
  printf "\t"
  echo "${bold}Stage 1:${normal} Running  jq '.records[]' '${file}' > '${thisFileTemp}' command"
  touch ${thisFileTemp}
  jq '.records[]' ${file} > ${thisFileTemp}
  printf "\t"
  echo "${bold}Stage 2:${normal} Running jq -c . < ${thisFileTemp} > ${thisParseRes}"
  touch ${thisParseRes}
  jq -c . < ${thisFileTemp} > ${thisParseRes}
  printf "\t"
  echo "${bold}Stage 3:${normal} Cleaning up ${thisFileTemp}"
  printf "\n"
  rm -f ${thisFileTemp}
done


