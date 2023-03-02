#!/bin/bash
# https://stackoverflow.com/questions/28185913/how-to-read-and-parse-the-json-file-and-add-it-into-the-shell-script-variable
#echo "'$1'"
cat connections/connections.json
conn=`cat connections/connections.json`
echo $conn

sudo docker-compose -f docker-compose-dev.yaml --compatibility down -v &&
sudo docker system prune -a -f &&
sudo docker-compose -f docker-compose-dev.yaml --compatibility up -d --build

sudo docker exec openweather_airflow-worker_1 \
airflow connections add 'azure_connection' \
    --conn-type 'generic' \
    --conn-extra "$conn" &> /dev/null;
sudo docker exec openweather_airflow-worker_1 \
az login --use-device-code;
#az account set --subscription aa7754bb-f1e5-4dd6-aab8-ee8b88c8b4fa