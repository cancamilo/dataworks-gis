init_env:
	poetry shell

build_deployment_extract_flows:
	prefect deployment build flows/web_to_gcs.py:web_to_gcs_data_range_flow -n "web to gcs data range ETL" \
	-o flows/deployments/web_to_gcs_data_range_deployment.yaml \
	&& prefect deployment build flows/web_to_gcs.py:web_to_gcs_flow -n "web to gcs dt ETL" \
	-o flows/deployments/web_to_gcs_deployment.yaml

build_deployment_deploy_load_flow:
	prefect deployment build flows/gcs_to_bq.py:gcs_to_bq_data_range_flow -n "gcs to bq daily range ETL" \
	-o flows/deployments/gcs_to_bq_data_range_deployment.yaml \
	&& prefect deployment build flows/gcs_to_bq.py:gcs_to_bq_flow -n "gcs to bq daily ETL" \
	-o flows/deployments/gcs_to_bq_deployment.yaml

build_deployments:
	@make build_deployment_extract_flows && make build_deployment_deploy_load_flow

apply_deployments:
	prefect deployment apply flows/deployments/web_to_gcs_data_range_deployment.yaml \
	&& prefect deployment apply flows/deployments/web_to_gcs_deployment.yaml \
	&& prefect deployment apply flows/deployments/gcs_to_bq_data_range_deployment.yaml \
	&& prefect deployment apply flows/deployments/gcs_to_bq_deployment.yaml \

start_agent:
	prefect agent start -q 'default'

ssh-vm:
	gcloud compute ssh --zone "europe-north1-a" "instance-1" --project "dataworks-gis"

activate-service-account:
	gcloud auth activate-service-account dataworks-gis-super@dataworks-gis.iam.gserviceaccount.com --key-file="gc_keys/dataworks-gis.json

set-default-project:
	gcloud config set project dataworks-gis




