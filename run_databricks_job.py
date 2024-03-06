""" 
This script demonstrates a proof-of-concept (POC) for running Databricks orchestration workflow jobs via YAML 
configuration using the Python SDK. It provides an alternative for Data Science teams to avoid dependency 
on specific orchestration tools like Airflow, enabling them to make changes directly in the configuration 
and execute the workflow job. This approach can be further integrated into CI/CD pipelines and automation processes.

Features:
1. Imports necessary Databricks SDK modules to run Databricks workflow.
2. Utilizes YAML and JSON to parse the configuration.
3. Creates a workflow job in Databricks based on the provided configuration input.
4. Triggers the job with given parameters in the config.
5. Optionally deletes the job once completed (Commented code).

Pre-requisites:
%pip install --upgrade databricks-sdk

Databricks Profile Setup at ~/.databrickscfg
Refer: https://docs.databricks.com/en/dev-tools/cli/profiles.html

"""
import time
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, compute
import json

DATABRICK_PROFILE = "dev"  # Can be set as an environment variable

w = WorkspaceClient(profile=DATABRICK_PROFILE)

def config_parser(config_file: str) -> dict:
    """
    Parses the YAML configuration file containing details about the Databricks job to be executed,
    including source code and runtime parameters.

    Args:
    - config_file (str): Path to the YAML configuration file.

    Returns:
    - dict: Key-value mappings of the Job and tasks.
    """
    try:
        with open(config_file) as f:
            configuration = yaml.safe_load(f)
            output = {"job": {}}
            for k, v in configuration["job"].items():
                output["job"][k] = configuration["job"][k]
            return output
    except Exception as ex:
        raise ValueError(
            "Failed to initialize ApplicationConfiguration, couldn't load YAML config!"
        ) from ex


def create_tasks(tasks_list_input: list) -> list:
    """
    Dynamically creates task objects based on the provided configurations.

    Args:
    - tasks_list_input (list): List containing task details.

    Returns:
    - list: A list of Databricks task objects.
    """
    tasks_list_output = []
    for tasks_dict in tasks_list_input:
        task = jobs.Task(
            description=tasks_dict["description"],
            job_cluster_key="default_cluster",
            spark_python_task=jobs.SparkPythonTask(
                python_file=tasks_dict["python_file"],
                source=jobs.Source.GIT,
                parameters=tasks_dict["parameters"],
            ),
            task_key=tasks_dict["task_key"],
            timeout_seconds=0,
            depends_on=[
                jobs.TaskDependency(task_key=i) for i in tasks_dict.get("depends_on", [])
            ],
        )
        tasks_list_output.append(task)
    return tasks_list_output


config = config_parser("src/configuration.yaml")
job = config["job"]
tasks = job["tasks"]

task_list = [task["task_key"] for task in tasks]
task_names = ",".join(task_list)

job_tasks = create_tasks(tasks)

# Create the Job with tasks and configuration from config_file
created_job = w.jobs.create(
    name=f"{job['name']}-{time.time_ns()}",
    tags=job["tags"],
    git_source=jobs.GitSource(
        git_url=job["git_url"],
        git_provider=jobs.GitProvider.gitHub,
        git_branch=job["git_branch"],
    ),
    job_clusters=[
        jobs.JobCluster(
            job_cluster_key="default_cluster",
            new_cluster=compute.ClusterSpec(
                spark_version=w.clusters.select_spark_version(long_term_support=True),
                node_type_id="Standard_DS3_v2",
                num_workers=1,
            ),
        )
    ],
    tasks=job_tasks,
)

# Run the Created Job
run_by_id = w.jobs.run_now(job_id=created_job.job_id).result()

# Cleanup - Uncomment to delete the job once complete.
# w.jobs.delete(job_id=created_job.job_id)
