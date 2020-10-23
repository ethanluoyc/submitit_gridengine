# Copyright (c) 2020 Yicheng Luo.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
#
import submitit
import time
from submitit_gridengine import gridengine

def add(a, b):
    res = a + b
    print(res)
    return a + b

def main():
    log_folder = "logs/"
    a = [1, 2]
    b = [10, 3]
    executor = gridengine.GridEngineExecutor(folder=log_folder)
    jobs = executor.map_array(add, a, b)  # just a list of jobs
    print(jobs)
    [j.result() for j in jobs]
    time.sleep(10)
    print([j.state for j in jobs])
    print([j.result() for j in jobs])

if __name__ == "__main__":
    main()