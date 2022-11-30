import argparse
import time
import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from databricks_cli.runs.api import RunsApi
from databricks_cli.sdk import ApiClient

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("--databricks-host", type=str)
parser.add_argument("--notebook_path", type=str)
parser.add_argument("--keyvault-url", type=str)
parser.add_argument("--keyvault-secret-name")
parser.add_argument("--num-workers", type=int)
parser.add_argument("--node-type-id", type=str)
parser.add_argument("--spark-version", type=str)

args = parser.parse_args()

#   generate the output path. AzureML pipelines use the default datastore
#   to store the intermediate outputs. The form of the path that is used
#   is azureml/<RUN-ID>/<NAME_OF_OUTPUT>. The name of the output in the component
#   is output_path, which is specified in the component yaml file:
#   outputs:
#    output_path:
#      type: uri_folder
#      mode: direct

OUTPUT_PATH = os.path.join('azureml', os.getenv('AZUREML_RUN_ID'), 'output_path')

# Get the secret from Key Vault
print("getting secret from key vault...", end=" ")
credential = DefaultAzureCredential()
client = SecretClient(vault_url=args.keyvault_url, credential=credential)
secret = client.get_secret(args.keyvault_secret_name)
print("done")

# Connect to Databricks using ApiClient
print("creating databricks api client...", end=" ")
api_client = ApiClient(host=args.databricks_host, token=secret.value)
print("done")

#   Create the Databricks Jobs specification according to 
#   https://docs.databricks.com/dev-tools/api/latest/jobs.html
job_spec = {
    "name": os.getenv('AZUREML_RUN_ID'),
    # create a new (on-demand) cluster to run the job
    "new_cluster": {
        "spark_version": args.spark_version, 
        "node_type_id": args.node_type_id, 
        "num_workers": args.num_workers, 
        # passthrough the AzureML MLFLow details so that tracking is done
        # in AzureML
        "spark_env_vars":{
            "MLFLOW_TRACKING_URI": os.getenv('MLFLOW_TRACKING_URI'),
            "AZUREML_RUN_ID": os.getenv('AZUREML_RUN_ID'),
            "MLFLOW_EXPERIMENT_ID": os.getenv('MLFLOW_EXPERIMENT_ID'),
            "MLFLOW_TRACKING_TOKEN": os.getenv('MLFLOW_TRACKING_TOKEN'),
            "MLFLOW_RUN_ID": os.getenv('MLFLOW_RUN_ID'),
            "MLFLOW_EXPERIMENT_NAME": os.getenv('MLFLOW_EXPERIMENT_NAME')
        }
    },
    # Run a notebook - it is possible to run other task types too, for 
    # example a python script/jar/SQL/dbt task.
    "notebook_task": {
        "notebook_path": args.notebook_path,
        "base_parameters": {
            # resolves to azureml/<RUN-ID>/output_path
            "output_folder": OUTPUT_PATH
        }
    },
    # libraries to install on the cluster. Here we install the azureml-mlflow
    # library so that we can use the AzureML MLFlow tracking store.
    "libraries": [
        {
            "pypi": {
                "package": "azureml-mlflow"
            }
        }
    ]
}

# Submit the Run
print("submitting databricks run...", end=" ")
runs_api = RunsApi(api_client)
run_id = runs_api.submit_run(job_spec)["run_id"]
print("done")

# Wait for the run to complete
my_run = runs_api.get_run(run_id)
while my_run["state"]["life_cycle_state"] != "TERMINATED":
    my_run = runs_api.get_run(run_id)
    print(f'state of run {run_id} is {my_run["state"]["life_cycle_state"]}')
    time.sleep(10)

