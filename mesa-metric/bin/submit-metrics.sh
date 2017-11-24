#!/usr/bin/env bash


mesa_path=$(cd $(dirname $0); pwd)

readonly json_files=${mesa_path}/../docs/metrics
readonly url="http://localhost:8009/mesa/execute"


echo "all json files $json_files"

for file in `ls ${json_files}`
do

 echo "\nsubmit file -> $file"
 echo " curl -H \"Content-Type: application/json\" -X POST  --data  @${file}  ${url}"
 curl -H "Content-Type: application/json" -X POST  --data  @docs/metrics/${file}  ${url}

done



