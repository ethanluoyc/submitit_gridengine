# Copyright (c) Yicheng Luo.
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
#

import functools
import inspect
import os
import re
import shutil
import subprocess
import sys
import typing as tp
import uuid
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from submitit.core import core, job_environment, logger, utils


def parse_accounting_info(output: str) -> Dict[str, str]:
    info = {}
    lines = output.split("\n")
    for line in lines:
        matched = re.match(r"(?P<key>\w+)\s+(?P<value>.*)", line)
        if matched:
            info[matched.group("key")] = matched.group("value").strip()
    return info


class GridEngineInfoWatcher(core.InfoWatcher):
    def __init__(self, delay_s: int = 60) -> None:
        super().__init__(delay_s)
        self._command = None

    def _setup_command(self):
        command = ["qacct"]
        # Sun Grid Engine default accounting file
        #   <sge_root>/<cell>/common/accounting
        # The accounting file may not be available in NFS, in that case, ssh into the master node to get the status
        # See https://users.gridengine.sunsource.narkive.com/v3HDdY50/accounting-file-missing
        sge_root = os.environ["SGE_ROOT"]
        sge_cell = os.environ["SGE_CELL"]
        default_accounting_file = os.path.join(sge_root, sge_cell, "common/accounting")
        act_qmaster_file = os.path.join(sge_root, sge_cell, "common/act_qmaster")
        if not os.path.exists(default_accounting_file):
            master = self._read_act_qmaster(sge_root, sge_cell)
            command = ["ssh", master, "qacct"]
        self._command = command

    def _read_act_qmaster(self, sge_root: str, sge_cell: str) -> str:
        act_qmaster_file = os.path.join(sge_root, sge_cell, "common/act_qmaster")
        with open(act_qmaster_file) as master_file:
            master = master_file.read().strip()
            return master

    def _make_command(self) -> Optional[List[str]]:
        if self._command is None:
            self._setup_command()
        # asking for array id will return all status
        # on the other end, asking for each and every one of them individually takes a huge amount of time
        to_check = {x.split("_")[0] for x in self._registered - self._finished}
        if not to_check or self._command is None:
            return None
        command = self._command[:]
        command.extend(["-j", ",".join(to_check)])
        return command

    def get_state(self, job_id: str, mode: str = "standard") -> str:
        """Returns the state of the job
        State of finished jobs are cached (use watcher.clear() to remove all cache)

        Parameters
        ----------
        job_id: int
            id of the job on the cluster
        mode: str
            one of "force" (forces a call), "standard" (calls regularly) or "cache" (does not call)
        """
        info = self.get_info(job_id, mode=mode)
        return info.get("State") or "UNKNOWN"

    def read_info(self, string: Union[bytes, str]) -> Dict[str, Dict[str, str]]:
        """Reads the output of sacct and returns a dictionary containing main information
        """
        if not isinstance(string, str):
            string = string.decode()
        if len(string.splitlines()) < 2:
            return {}  # one job id does not exist (yet)
        all_stats: Dict[str, Dict[str, str]] = {}
        parts = string.split("==============================================================\n")
        for part in parts:
            part_info = parse_accounting_info(part)
            if not part_info:
                continue
            job_id = (
                "{}_{}".format(part_info["jobnumber"], part_info["taskid"])
                if part_info["taskid"].isnumeric()
                else part_info["jobnumber"]
            )
            all_stats[job_id] = {}
            state = "SUCCESS" if int(part_info["failed"]) == 0 else "FAILED"
            all_stats[job_id]["State"] = state
        return all_stats


class GridEngineJob(core.Job[core.R]):

    watcher = GridEngineInfoWatcher(delay_s=60)


class GridEngineJobEnvironment(job_environment.JobEnvironment):

    _env = {
        "job_id": "SUBMITIT_JOB_ID",
        "num_tasks": "SUBMITIT_NTASKS",
        "num_nodes": "SUBMITIT_JOB_NUM_NODES",
        "node": "SUBMITIT_NODEID",
        "global_rank": "SUBMITIT_GLOBAL_RANK",
        "local_rank": "SUBMITIT_LOCAL_RANK",
    }

    def _requeue(self, countdown: int) -> None:
        raise NotImplementedError  # TODO


class GridEngineExecutor(core.PicklingExecutor):
    """Sge job executor
    This class is used to hold the parameters to run a job on Son of Grid Engine (SGE).
    In practice, it will create a batch file in the specified directory for each job,
    and pickle the task function and parameters. At completion, the job will also pickle
    the output. Logs are also dumped in the same directory.

    Parameters
    ----------
    folder: Path/str
        folder for storing job submission/output and logs.
    max_num_timeout: int
        Maximum number of time the job can be requeued after timeout (if
        the instance is derived from helpers.Checkpointable)

    Note
    ----
    - be aware that the log/output folder will be full of logs and pickled objects very fast,
      it may need cleaning.
    - the folder needs to point to a directory shared through the cluster. This is typically
      not the case for your tmp! If you try to use it, slurm will fail silently (since it
      will not even be able to log stderr.
    - use update_parameters to specify custom parameters (n_gpus etc...). If you
      input erroneous parameters, an error will print all parameters available for you.
    """

    job_class = GridEngineJob

    def __init__(self, folder: Union[Path, str], max_num_timeout: int = 3) -> None:
        super().__init__(folder, max_num_timeout)

    @classmethod
    def name(cls) -> str:
        return "gridengine"

    @property
    def _submitit_command_str(self) -> str:
        # make sure to use the current executable (required in pycharm)
        return f"{sys.executable} -u -m submitit.core._submit '{self.folder}'"

    def _make_submission_file_text(self, command: str, uid: str) -> str:
        return _make_qsub_string(command=command, folder=str(self.folder), map_count=self.parameters["map_count"])

    def _internal_process_submissions(
        self, delayed_submissions: tp.List[utils.DelayedSubmission]
    ) -> tp.List[core.Job[tp.Any]]:
        # Not an array job
        if len(delayed_submissions) == 1:
            return super()._internal_process_submissions(delayed_submissions)

        # Submit as an array job
        folder = utils.JobPaths.get_first_id_independent_folder(self.folder)
        folder.mkdir(parents=True, exist_ok=True)
        pickle_paths = []
        for d in delayed_submissions:
            pickle_path = folder / f"{uuid.uuid4().hex}.pkl"
            d.timeout_countdown = self.max_num_timeout
            d.dump(pickle_path)
            pickle_paths.append(pickle_path)
        n = len(delayed_submissions)
        # Make a copy of the executor, since we don't want other jobs to be
        # scheduled as arrays.
        array_ex = GridEngineExecutor(self.folder, self.max_num_timeout)
        array_ex.update_parameters(**self.parameters)
        array_ex.parameters["map_count"] = n
        self._throttle()

        # Submit delay submissions as an array job,
        # This means that each job will now have id $job_id_$array_id
        first_job: core.Job[tp.Any] = array_ex._submit_command(self._submitit_command_str)
        tasks_ids = list(range(first_job.num_tasks))
        jobs: List[core.Job[tp.Any]] = [
            # SGE expects task ids to be positive
            GridEngineJob(folder=self.folder, job_id=f"{first_job.job_id}_{a}", tasks=tasks_ids)
            for a in range(1, n + 1)
        ]
        # Move individual submitted pickle
        # Each array job expects a submitted pickle from "%j_submitted.pkl"
        # Note that the workers will dump result to %j_%t_result.pkl
        for job, pickle_path in zip(jobs, pickle_paths):
            job.paths.move_temporary_file(pickle_path, "submitted_pickle")
        return jobs

    def _num_tasks(self) -> int:
        nodes: int = 1
        tasks_per_node: int = self.parameters.get("ntasks_per_node", 1)
        return nodes * tasks_per_node

    def _make_submission_command(self, submission_file_path: Path) -> List[str]:
        return ["qsub", str(submission_file_path)]

    @staticmethod
    def _get_job_id_from_submission_command(string: Union[bytes, str]) -> str:
        """Returns the job ID from the output of sbatch string
        """
        if not isinstance(string, str):
            string = string.decode()
        output = re.search(r"job(-array)? (?P<id>[0-9]+)", string)
        if output is None:
            raise utils.FailedSubmissionError(
                'Could not make sense of qsub output "{}"\n'.format(string)
                + "Job instance will not be able to fetch status\n"
                "(you may however set the job job_id manually if needed)"
            )
        return output.group("id")


def _make_qsub_string(
    command: str,
    folder: str,
    map_count: int = 1,
    job_name: str = "submitit",
    tmem: str = "2G",
    h_vmem: str = "2G",
    h_rt: str = "00:02:00",
    shell: str = "/bin/bash",
):
    """Build a SGE submission script

    Parameters
    ---------
    command: str
        the command to run
    job_name: str
        the command to run
    num_jobs: int
        the number of array jobs

    Return
    ------
    script: str
        the submission script that can be used by qsub

    Notes
    -----
    TODO: implement full set of flags in https://hpc.cs.ucl.ac.uk/full-guide/.
    Also see http://gridscheduler.sourceforge.net/htmlman/manuals.html
    """
    lines = []
    lines += [f"#$ -l tmem={tmem}"]
    lines += [f"#$ -l h_vmem={h_vmem}"]
    lines += [f"#$ -l h_rt={h_rt}"]
    lines += [f"#$ -S {shell}"]
    paths = utils.JobPaths(folder=folder)
    if map_count > 1:
        job_id = "${JOB_ID}_${SGE_TASK_ID}"
        stdout = str(paths.stdout).replace("%j", "$JOB_ID_$TASK_ID").replace("%t", "0")
        stderr = str(paths.stderr).replace("%j", "$JOB_ID_$TASK_ID").replace("%t", "0")
    else:
        job_id = "${JOB_ID}"
        stdout = str(paths.stdout).replace("%j", "$JOB_ID")
        stderr = str(paths.stderr).replace("%j", "$JOB_ID")
    lines += [f"#$ -o {stdout}"]
    lines += [f"#$ -e {stderr}"]
    lines += ["#$ -cwd"]
    lines += [f"#$ -N {job_name}"]
    lines += [f"#$ -V"]
    if map_count > 1:
        lines += [f"#$ -t 1-{map_count}"]
    lines += [f"export SUBMITIT_JOB_ID={job_id}"]
    # Support only ntasks=1 for now
    lines += ["export SUBMITIT_NTASKS=1"]
    lines += ["export SUBMITIT_NODEID=0"]
    lines += ["export SUBMITIT_JOB_NUM_NODES=1"]
    lines += ["export SUBMITIT_GLOBAL_RANK=0"]
    lines += ["export SUBMITIT_LOCAL_RANK=0"]
    lines += ["export SUBMITIT_EXECUTOR=gridengine"]
    lines += [command]
    return "\n".join(lines)
