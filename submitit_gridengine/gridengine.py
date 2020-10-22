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
import signal
import shutil
import subprocess
import sys
import time
import typing as tp
import uuid
import warnings
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from submitit.core import core, job_environment, logger, utils
from submitit.core.utils import DelayedSubmission, JobPaths
from submitit.core.job_environment import SignalHandler


def parse_accounting_info(output: str) -> Dict[str, str]:
    info = {}
    lines = output.split("\n")
    for line in lines:
        matched = re.match(r"(?P<key>\w+)\s+(?P<value>.*)", line)
        if matched:
            info[matched.group("key")] = matched.group("value").strip()
    return info


def _is_job_not_found_error(error_output: Union[bytes, str]) -> bool:
    if not isinstance(error_output, str):
        error_output = error_output.decode()
    return re.search(r"error:\sjob\sid\s(\d+)\snot\sfound", error_output) is not None


def parse_array(array_str: str):
    if array_str.isnumeric():
        return [
            int(array_str),
        ]
    elif "," in array_str:
        return list(map(int, array_str.split(",")))
    pattern = r"(?P<start>\d+)-(?P<end>\d+):(?P<step>\d+)"
    match = re.match(pattern, array_str)
    if match:
        return list(range(int(match.group("start")), int(match.group("end")) + 1, int(match.group("step"))))
    return []


def _process_job_list(e):
    info = {}
    state = e.attrib["state"]
    job_number = e.find("JB_job_number").text
    tasks = e.find("tasks")
    if tasks is not None:
        array_ids = parse_array(tasks.text)
        for ai in array_ids:
            info["{}_{}".format(job_number, ai)] = {"State": state}
    else:
        info["{}".format(job_number)] = {"State": state}
    return info


def _parse_qstat(qstat_xml_output: str):
    info = {}
    root = ET.fromstring(qstat_xml_output)
    for elem in root.iter("job_list"):
        info.update(_process_job_list(elem))
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

    def _collect_info_from_qstat(self):
        try:
            output = subprocess.check_output(["qstat", "-xml"], shell=False)
        except Exception as e:
            logger.get_logger().warning(
                f"Call #{self.num_calls} - Bypassing qacct error {e}, status may be inaccurate."
            )
            info = {}
        else:
            info = _parse_qstat(output.decode())
        return info

    def update(self) -> None:
        """Updates the info of all registered jobs with a call to qacct
        """
        # Adapted from core
        command = self._make_command()
        if command is None:
            return
        self._num_calls += 1
        # First use qstat to find out jobs that are still waiting/running
        qstat_info = self._collect_info_from_qstat()
        self._info_dict.update(qstat_info)

        try:
            self._output = subprocess.check_output(command, stderr=subprocess.PIPE, shell=False)
        except subprocess.CalledProcessError as e:
            # be graceful in the case qacct failed because of not having the information for a job.
            if not _is_job_not_found_error(e.stderr):
                raise
        except Exception as e:
            logger.get_logger().warning(
                f"Call #{self.num_calls} - Bypassing qacct error {e}, status may be inaccurate."
            )
        else:
            self._info_dict.update(self.read_info(self._output))
        self._last_status_check = time.time()
        # check for finished jobs
        to_check = self._registered - self._finished
        for job_id in to_check:
            if self.is_done(job_id, mode="cache"):
                self._finished.add(job_id)

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
        """Reads the output of qacct and returns a dictionary containing main information
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
            matched = re.match(r"\d+", part_info["failed"])
            if matched:
                exit_status = matched.group(0)
            else:
                logger.get_logger().warning(
                    f"Unable to parse exit status \"{part_info}\", status may be inaccurate."
                )
                exit_status = "NA"
            state = "SUCCESS" if exit_status == "0" else "FAILED"
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

    def _handle_signals(self, paths: JobPaths, submission: DelayedSubmission) -> None:
        """Set up signals handler for the current executable.

        The default implementation checkpoint the given submission and requeues it.
        @plugin-dev: Should be adapted to the signals used in this cluster.
        """
        handler = SignalHandler(self, paths, submission)
        # In SGE, when -notify is enabled,
        # a SIGUSR2 signal is sent to the process before some time before the SIGKILL.
        # Refer to the time delay in `notify` in `qconf -sq <queuename>` for the time
        # SIGUSR2 is sent before SIGKILL
        signal.signal(signal.SIGUSR2, handler.checkpoint_and_exit)

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

    @classmethod
    def _valid_parameters(cls) -> Set[str]:
        """Parameters that can be set through update_parameters
        """
        # TODO
        return set()

    @property
    def _submitit_command_str(self) -> str:
        # make sure to use the current executable (required in pycharm)
        return f"{sys.executable} -u -m submitit.core._submit '{self.folder}'"

    def _make_submission_file_text(self, command: str, uid: str) -> str:
        return _make_qsub_string(
            command=command, folder=str(self.folder), **self.parameters
        )

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
    job_name: str = "submitit",
    tmem: str = "2G", # physical memory limit
    h_vmem: str = "2G", # virtual memory limit
    h_rt: str = "00:02:00", # wall time
    num_gpus: int = 0,
    shell: str = "/bin/bash",
    merge: bool = False, # whether to merge stdout and stderr
    tc: Optional[int] = None, # concurrent number of tasks
    map_count: int = 1, # For array jobs
):
    """Build a SGE submission script

    Parameters
    ---------
    command: str
        the command to run
    job_name: str
        the command to run

    Return
    ------
    script: str
        the submission script that can be used by qsub

    Notes
    -----
    TODO: implement full set of flags in https://hpc.cs.ucl.ac.uk/full-guide/.
    Also see http://gridscheduler.sourceforge.net/htmlman/manuals.html
        * wd: #$ /home/jbloggs
        * reserve: # -R y
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
        stdout = str(paths.stdout).replace("%j", "$JOB_ID").replace("%t", "0")
        stderr = str(paths.stderr).replace("%j", "$JOB_ID").replace("%t", "0")
    lines += [f"#$ -o {stdout}"]
    lines += [f"#$ -e {stderr}"]
    if merge:
        lines += [f"#$ -j"]
    lines += [f"#$ -N {job_name}"]
    # TODO allow configuring cwd and environment variables
    lines += ["#$ -cwd"]
    lines += [f"#$ -V"]
    if num_gpus > 0:
        lines +=[f"#$ -l gpu=true", f"#$ -pe gpu {num_gpus}"]
    if map_count > 1:
        lines += [f"#$ -t 1-{map_count}"]
    if tc is not None and map_count > 1:
        lines += [f"#$ -tc {tc}"]
    lines += ["#$ -notify"]

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
