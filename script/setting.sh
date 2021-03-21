#!/bin/bash
airflow db init
airflow users create -r ${AIRFLOW_RULE} \
		     -u ${AIRFLOW_USER} \
		     -e ${AIRFLOW_EMAIL} \
		     -f ${AIRFLOW_FNAME} \
		     -l ${AIRFLOW_LNAME} \
		     -p ${AIRFLOW_PASSWORD}

airflow connections add gcp_project_1 \
	--conn-type=google_cloud_platform \
	--conn-extra='{"extra__google_cloud_platform__key_path":"/opt/airflow/config/.keys/gcp_project_1.json","extra__google_cloud_platform__project":"kd003staging","extra__google_cloud_platform__scope":"https://www.googleapis.com/auth/cloud-platform"}'


airflow variables import ${AIRFLOW_HOME}/config/variables/gcp_project_1.json
