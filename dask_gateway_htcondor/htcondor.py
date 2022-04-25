from dask_gateway_server.backends.jobqueue.base import JobQueueBackend, JobQueueClusterConfig
from dask_gateway_server.traitlets import default, Type

from traitlets import Dict, Unicode, default

from enum import Enum
import math, os, re, shutil

def htcondor_create_execution_script(execution_script, setup_command, execution_command):
    # write script to staging_dir
    with open(execution_script, "w") as f:
        f.writelines([
            "#!/bin/sh",
            setup_command,
            execution_command
        ])

def htcondor_create_jdl(cluster_config, execution_script, log_dir, cpus, mem):
    # ensure log dir is present otherwise condor_submit will fail
    os.mkdirs(log_dir, exist_ok=True)

    jdl_dict = {"universe": cluster_config.universe,
    "docker_image": cluster_config.docker_image,
    "executable": execution_script,
    "output": f"{log_dir}/$(cluster).$(process).out",
    "error": f"{log_dir}/$(cluster).$(process).err",
    "log": f"{log_dir}/cluster.log",
    "request_cpus": f"{cpus}",
    "request_memory": f"{mem}"
    }
    jdl_dict.update(cluster_config.extra_jdl)

    jdl = "\n".join(f"{key} = {value}" for key, value in jdl_dict.items()) + "\n"
    jdl += "queue 1\n"

    return jdl

def htcondor_memory_format(memory):
    return math.ceil(memory / (1024**2))


class HTCondorJobStates(Enum):
    IDLE = 1
    RUNNING = 2
    REMOVING = 3
    COMPLETED = 4
    HELD = 5
    TRANSFERRING_OUTPUT = 6
    SUSPENDED = 7


class HTCondorClusterConfig(JobQueueClusterConfig):
    """Dask cluster configuration options when running on HTCondor"""
    universe = Unicode("docker", help="The universe to submit jobs to. Defaults to docker", config=True)
    docker_image = Unicode("coffeateam/coffea-dask-cc7-gateway", help="The docker image to run jobs in.", config=True)
    extra_jdl = Dict(help="Additional content of the job description file.", config=True)


class HTCondorBackend(JobQueueBackend):
    """A backend for deploying Dask on a HTCondor cluster."""

    cluster_config_class = Type(
        "dask_gateway_htcondor.HTCondorClusterConfig",
        klass="dask_gateway_server.backends.base.ClusterConfig",
        help="The cluster config class to use",
        config=True,
    )

    @default("submit_command")
    def _default_submit_command(self):
        return shutil.which("condor_submit") or "condor_submit"

    @default("cancel_command")
    def _default_cancel_command(self):
        return shutil.which("condor_rm") or "condor_rm"

    @default("status_command")
    def _default_status_command(self):
        return shutil.which("condor_q") or "condor_q"

    def get_submit_cmd_env_stdin(self, cluster, worker=None):
        cmd = [self.submit_command, "--verbose"]
        staging_dir = self.get_staging_directory(cluster)

        if worker:
            execution_script = os.path.join(staging_dir, f"run_worker_{worker.name}.sh")
            htcondor_create_execution_script(execution_script=execution_script,
                setup_command=cluster.config.worker_setup,
                execution_command=" ".join(self.get_worker_command(cluster, worker.name)))
            env = self.get_worker_env(cluster)
            jdl = htcondor_create_jdl(cluster_config=cluster.config, 
                execution_script=execution_script,
                log_dir=os.path.join(staging_dir, "logs_worker_{worker.name}"),
                cpus=cluster.config.worker_cores, 
                mem=htcondor_memory_format(cluster.config.worker_memory))
        else:
            execution_script = os.path.join(staging_dir, f"run_scheduler_{cluster.name}.sh")
            htcondor_create_execution_script(execution_script=execution_script,
                setup_command=cluster.config.scheduler_setup,
                execution_command=" ".join(self.get_scheduler_command(cluster)))
            env = self.get_scheduler_env(cluster)
            jdl = htcondor_create_jdl(cluster_config=cluster.config,
                log_dir=os.path.join(staging_dir, "logs_scheduler_{cluster.name}"),
                cpus=cluster.config.scheduler_cores,
                mem=htcondor_memory_format(cluster.config.scheduler_memory))

        return cmd, env, jdl

    def get_stop_cmd_env(self, job_id):
        return [self.cancel_command, job_id], {}

    def get_status_cmd_env(self, job_ids):
        return [self.status_command, "-af:j", "JobStatus"], {}

    def parse_job_id(self, stdout):
        # search the Job ID in a submit Proc line
        submit_id_pattern = re.compile(r"Proc\s(\d+\.\d+)", flags=re.MULTILINE)
        return submit_id_pattern.search(stdout).group(1)

    def parse_job_states(self, stdout):
        """Checks if job is okay"""
        status_id_pattern = re.compile(r"^([0-9,.]+)\s(\d)$", flags=re.MULTILINE)
        states = {}
        for match in re.finditer(status_id_pattern, stdout):
            job_id, state= match.groups()
            states[job_id] = HTCondorJobStates(int(state)) in (HTCondorJobStates.IDLE, HTCondorJobStates.RUNNING)
        return states
