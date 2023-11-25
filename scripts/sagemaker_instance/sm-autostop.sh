#!/bin/bash
set -e

# PARAMETERS
IDLE_TIME=3600

echo "Fetching the autostop script"
wget -O autostop.py s3://scripts-infrastructure-ml-pred-taxi/sagemaker/notebook/autostop.py

echo "Starting the SageMaker autostop script in cron"
(crontab -l 2>/dev/null; echo "*/5 * * * * /bin/bash -c '/usr/bin/python3 $DIR/autostop.py --time ${IDLE_TIME} | tee -a /home/ec2-user/SageMaker/auto-stop-idle.log'") | crontab -

echo "Changing cloudwatch configuration"
curl s3://scripts-infrastructure-ml-pred-taxi/sagemaker/notebook/on-start.sh | sudo bash -s auto-stop-idle /home/ec2-user/SageMaker/auto-stop-idle.log

