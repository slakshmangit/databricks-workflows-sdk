""" 
This is a minimal POC to run databricks orchestration workflow jobs via YAML 
configuration using Python SDK. This will be helpful for Data Science team to not rely on specific
Orchestration tools like Airflow and make any code changes. Instead, change the config
and run the workflow job. This can be enhanced further in CI/CD and automations.

1. Imports necessary databricks sdk modules to run databricks workflow 
2. Use yaml, json to parse the config
3. Create workflow job in databricks as per the configuration input
4. Triggers the job with given params in the config
5. Deletes the job once completed (Commented code)

Pre-requisites:
%pip install --upgrade databricks-sdk

Databricks Profile Setup at ~/.databrickscfg
refer. https://docs.databricks.com/en/dev-tools/cli/profiles.html

"""
import time
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, compute
import json

DATABRICK_PROFILE="dev" # can be an env variable

w = WorkspaceClient(profile=DATABRICK_PROFILE)

def config_parser(config_file: str):
    """
    Parses the YAML configuration file which has the details about the databicks job to be run
    along with source code and run time params.

    Input: config_file - yaml configuration file path
    Output: Dictionary - Key-value mappings of the Job and tasks
    """
    try:
        with open(config_file) as f:
            configuration = yaml.safe_load(f)
            output = {}
            output["job"] = {}
            for k, v in configuration["job"].items():
                output["job"][k] = configuration["job"][k]
            return output
    except Exception as ex:
        raise ValueError(
            "Failied to initialize ApplicationConfiguration, couldn't load yaml config!"
        ) from ex


def create_tasks(tasks_list_input: list):
    """
    Dynamically creates tasks objects along with the provided configurations.

    Input: tasks_list_input - list with tasks and details
    Output: a list of Databricks task objects
    """
    tasks_list_output: list = []
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
                jobs.TaskDependency(task_key=i) for i in tasks_dict["depends_on"]
            ]
            if "depends_on" in tasks_dict
            else None,
        )
        tasks_list_output.append(task)
    return tasks_list_output


config = config_parser("src/configuration.yaml")
job = config["job"]
tasks = job["tasks"]


task_list = []
for task in tasks:
    task_list.append(task["task_key"])

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

# cleanup - uncomment to delete the job once complete.
# w.jobs.delete(job_id=created_job.job_id)
