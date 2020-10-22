# Copyright (c) 2020 Yicheng Luo.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
#
import submitit
import time
from submitit_gridengine import gridengine

def task():
    time.sleep(600)

def main():
    log_folder = "logs/"
    executor = gridengine.GridEngineExecutor(folder=log_folder)
    executor.update_parameters(h_rt="00:01:10")
    job = executor.submit(task)
    job.result()
    time.sleep(10)
    print(job.state)

if __name__ == "__main__":
    main()