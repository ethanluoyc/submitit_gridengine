# Copyright (c) 2020 Yicheng Luo.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
#
"""Example for a GPU job"""
import submitit
import os
import time
from submitit_gridengine import gridengine
import subprocess

def task():
    print(os.environ["CUDA_VISIBLE_DEVICES"])
    print(subprocess.check_call(["nvidia-smi"]))

def main():
    log_folder = "logs/"
    executor = gridengine.GridEngineExecutor(folder=log_folder)
    executor.update_parameters(h_rt="00:01:00", num_gpus=1)
    job = executor.submit(task)
    job.result()
    time.sleep(10)
    print(job.state)
    print(job.result())

if __name__ == "__main__":
    main()