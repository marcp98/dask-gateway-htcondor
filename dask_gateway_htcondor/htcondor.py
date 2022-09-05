from dask_gateway_server.backends.jobqueue.base import JobQueueBackend, JobQueueClusterConfig
from dask_gateway_server.traitlets import Type

from traitlets import Dict, Unicode, default

from enum import Enum
import math, os, pwd, re, shutil

def htcondor_create_execution_script(execution_script, setup_command, execution_command):
    # write script to staging_dir
    with open(execution_script, "w") as f:
        f.write("\n".join([
            "#!/bin/sh",
            "env",
            "ls -l",
            setup_command,
            execution_command
        ]))

def htcondor_create_jdl(cluster_config, execution_script, log_dir, cpus, mem, env, tls_path):
    # ensure log dir is present otherwise condor_submit will fail
    os.makedirs(log_dir, exist_ok=True)

    env["DASK_DISTRIBUTED__COMM__TLS__SCHEDULER__CERT"] = "dask.crt"
    env["DASK_DISTRIBUTED__COMM__TLS__WORKER__CERT"] = "dask.crt"
    env["DASK_DISTRIBUTED__COMM__TLS__SCHEDULER__KEY"] = "dask.pem"
    env["DASK_DISTRIBUTED__COMM__TLS__WORKER__KEY"] = "dask.pem"
    env["DASK_DISTRIBUTED__COMM__TLS__CA_FILE"] = "dask.crt"

    jdl_dict = {"universe": cluster_config.universe,
    "docker_image": cluster_config.docker_image,
    "executable": os.path.relpath(execution_script),
    "docker_network_type": "host",
    "should_transfer_files": "YES",
    "transfer_input_files": ",".join(tls_path),
    "when_to_transfer_output": "ON_EXIT",
    "output": f"{log_dir}/$(cluster).$(process).out",
    "error": f"{log_dir}/$(cluster).$(process).err",
    "log": f"{log_dir}/cluster.log",
    "request_cpus": f"{cpus}",
    "request_memory": f"{mem}",
    "environment": ";".join(f"{key}={value}" for key, value in env.items())
    }
    jdl_dict.update(cluster_config.extra_jdl)

    jdl = "\n".join(f"{key} = {value}" for key, value in jdl_dict.items()) + "\n"
    jdl += "queue 1\n"

    return jdl

def htcondor_memory_format(memory):
    return math.ceil(memory / (1024**2))



