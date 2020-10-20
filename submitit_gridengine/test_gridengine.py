# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
#
import contextlib
import os
import signal
import tempfile
import typing as tp
from pathlib import Path
from unittest.mock import patch

import pytest
from submitit import helpers
from submitit.core import job_environment, submission, test_core, utils
from submitit.core.core import Job

from submitit_gridengine import gridengine


def _mock_log_files(job: Job[tp.Any], prints: str = "", errors: str = "") -> None:
    """Write fake log files
    """
    filepaths = [str(x).replace("%j", str(job.job_id)) for x in [job.paths.stdout, job.paths.stderr]]
    for filepath, msg in zip(filepaths, (prints, errors)):
        with Path(filepath).open("w") as f:
            f.write(msg)


@contextlib.contextmanager
def mocked_slurm(state: str = "RUNNING", job_id: str = "12", array: int = 0) -> tp.Iterator[str]:
    with contextlib.ExitStack() as stack:
        stack.enter_context(
            test_core.MockedSubprocess(state=state, job_id=job_id, shutil_which="qsub", array=array).context()
        )
        envs = dict(_USELESS_TEST_ENV_VAR_="1", SUBMITIT_EXECUTOR="gridengine", SLURM_JOB_ID=str(job_id))
        stack.enter_context(utils.environment_variables(**envs))
        tmp = stack.enter_context(tempfile.TemporaryDirectory())
        yield tmp


# def test_mocked_missing_state() -> None:
#     with mocked_slurm(state="       ", job_id="12") as tmp:
#         job: gridengine.GridEngineJob[None] = gridengine.GridEngineJob(tmp, "12")
#         assert job.state == "UNKNOWN"
#         job._interrupt(timeout=False)  # check_call is bypassed by MockedSubprocess


def test_job_environment() -> None:
    with mocked_slurm(job_id="12"):
        assert job_environment.JobEnvironment().cluster == "gridengine"


# def test_slurm_job_mocked() -> None:
#     with mocked_slurm() as tmp:
#         executor = gridengine.GridEngineExecutor(folder=tmp)
#         job = executor.submit(test_core.do_nothing, 1, 2, blublu=3)
#         assert job.job_id == "12"
#         assert job.state == "RUNNING"
#         assert job.stdout() is None
#         _mock_log_files(job, errors="This is the error log\n", prints="hop")
#         job._results_timeout_s = 0
#         with pytest.raises(utils.UncompletedJobError):
#             job._get_outcome_and_result()
#         _mock_log_files(job, errors="This is the error log\n", prints="hop")
#         submission.process_job(job.paths.folder)
#         assert job.result() == 12
#         # logs
#         assert job.stdout() == "hop"
#         assert job.stderr() == "This is the error log\n"
#     assert "_USELESS_TEST_ENV_VAR_" not in os.environ, "Test context manager seems to be failing"


# @pytest.mark.parametrize("context", (True, False))  # type: ignore
# def test_slurm_job_array_mocked(context: bool) -> None:
#     n = 5
#     with mocked_slurm(array=n) as tmp:
#         executor = gridengine.GridEngineExecutor(folder=tmp)
#         executor.update_parameters(array_parallelism=3)
#         data1, data2 = range(n), range(10, 10 + n)

#         def add(x: int, y: int) -> int:
#             assert x in data1
#             assert y in data2
#             return x + y

#         jobs: tp.List[Job[int]] = []
#         if not context:
#             jobs = executor.map_array(add, data1, data2)
#         else:
#             with executor.batch():
#                 for d1, d2 in zip(data1, data2):
#                     jobs.append(executor.submit(add, d1, d2))
#         array_id = jobs[0].job_id.split("_")[0]
#         assert [f"{array_id}_{a}" for a in range(n)] == [j.job_id for j in jobs]

#         for job in jobs:
#             os.environ["SLURM_JOB_ID"] = str(job.job_id)
#             submission.process_job(job.paths.folder)
#         # trying a slurm specific method
#         jobs[0]._interrupt(timeout=True)  # type: ignore
#         assert list(map(add, data1, data2)) == [j.result() for j in jobs]
#         # check submission file
#         sbatch = Job(tmp, job_id=array_id).paths.submission_file.read_text()
#         array_line = [l.strip() for l in sbatch.splitlines() if "array" in l]
#         assert array_line == ["#SBATCH --array=0-4%3"]


# def test_slurm_error_mocked() -> None:
#     with mocked_slurm() as tmp:
#         executor = gridengine.GridEngineExecutor(folder=tmp)
#         executor.update_parameters(time=24, gpus_per_node=0)  # just to cover the function
#         job = executor.submit(test_core.do_nothing, 1, 2, error=12)
#         with pytest.raises(ValueError):
#             submission.process_job(job.paths.folder)
#         _mock_log_files(job, errors="This is the error log\n")
#         with pytest.raises(utils.FailedJobError):
#             job.result()
#         exception = job.exception()
#         assert isinstance(exception, utils.FailedJobError)


# @pytest.mark.skip
# def test_signal(tmp_path: Path, monkeypatch) -> None:
#     monkeypatch.setenv("_TEST_CLUSTER_", "gridengine")
#     job_paths = utils.JobPaths(tmp_path, "1234")
#     fs0 = helpers.FunctionSequence()
#     fs0.add(test_core._three_time, 4)
#     fs0.delayed_functions[0].dump(job_paths.submitted_pickle)  # hack to create a file
#     env = job_environment.JobEnvironment()
#     sig = job_environment.SignalHandler(env, job_paths, utils.DelayedSubmission(fs0))

#     sig.bypass(signal.Signals.SIGTERM)

#     with pytest.raises(SystemExit), patch(
#         "submitit.gridengine.gridengine.GridEngineJobEnvironment._requeue", return_value=None
#     ):
#         sig.checkpoint_and_try_requeue(signal.Signals.SIGUSR1)

#     with pytest.raises(SystemExit):
#         sig.checkpoint_and_exit(signal.Signals.SIGUSR1)

#     resubmitted = utils.DelayedSubmission.load(job_paths.submitted_pickle)
#     output = resubmitted.result()
#     assert output == [12]


# def test_make_batch_string() -> None:
#     string = gridengine._make_sbatch_string(
#         command="blublu",
#         folder="/tmp",
#         partition="learnfair",
#         exclusive=True,
#         additional_parameters=dict(blublu=12),
#     )
#     assert "partition" in string
#     assert "--command" not in string
#     assert "constraint" not in string
#     record_file = Path(__file__).parent / "_sbatch_test_record.txt"
#     if not record_file.exists():
#         record_file.write_text(string)
#     recorded = record_file.read_text()
#     changes = []
#     for k, (line1, line2) in enumerate(zip(string.splitlines(), recorded.splitlines())):
#         if line1 != line2:
#             changes.append(f'line #{k + 1}: "{line2}" -> "{line1}"')
#     if changes:
#         print(string)
#         print("# # # # #")
#         print(recorded)
#         message = ["Difference with reference file:"] + changes
#         message += ["", "Delete the record file if this is normal:", f"rm {record_file}"]
#         raise AssertionError("\n".join(message))


# def test_make_batch_string_gpu() -> None:
#     string = gridengine._make_sbatch_string(command="blublu", folder="/tmp", gpus_per_node=2)
#     assert "--gpus-per-node=2" in string


# def test_update_parameters_error() -> None:
#     with mocked_slurm() as tmp:
#         with pytest.raises(ValueError):
#             executor = gridengine.GridEngineExecutor(folder=tmp)
#             executor.update_parameters(blublu=12)


# def test_read_info() -> None:
#     example = """JobID|State
# 5610980|RUNNING
# 5610980.ext+|RUNNING
# 5610980.0|RUNING
# 20956421_0|RUNNING
# 20956421_[2-4%25]|PENDING
# """
#     output = gridengine.GridEngineInfoWatcher().read_info(example)
#     assert output["5610980"] == {"JobID": "5610980", "State": "RUNNING"}
#     assert output["20956421_2"] == {"JobID": "20956421_[2-4%25]", "State": "PENDING"}
#     assert set(output) == {"5610980", "20956421_0", "20956421_2", "20956421_3", "20956421_4"}
_TEST_QSUB_OUTPUT = """Your job 2466409 ("MyTESTJOBNAME") has been submitted
"""
_TEST_QSTAT_OUTPUT = """job-ID  prior   name       user         state submit/start at     queue                          slots ja-task-ID
-----------------------------------------------------------------------------------------------------------------
2466409 0.00000 MyTESTJOBN yicheluo     qw    10/19/2020 17:34:59                                    1
"""
_TEST_QACCT_OUTPUT = """==============================================================
qname        test.q
hostname     abner-601-2.local
group        cs_postgrad
owner        yicheluo
project      csml
department   defaultdepartment
jobname      submitit
jobnumber    2467870
taskid       undefined
account      sge
priority     0
qsub_time    Mon Oct 19 21:21:25 2020
start_time   Mon Oct 19 21:23:19 2020
end_time     Mon Oct 19 21:23:20 2020
granted_pe   NONE
slots        1
failed       0
exit_status  0
ru_wallclock 1s
ru_utime     0.368s
ru_stime     0.248s
ru_maxrss    18.211KB
ru_ixrss     0.000B
ru_ismrss    0.000B
ru_idrss     0.000B
ru_isrss     0.000B
ru_minflt    42181
ru_majflt    0
ru_nswap     0
ru_inblock   40
ru_oublock   40
ru_msgsnd    0
ru_msgrcv    0
ru_nsignals  0
ru_nvcsw     1167
ru_nivcsw    85
cpu          0.616s
mem          0.000Bs
io           0.000B
iow          0.000s
maxvmem      0.000B
arid         undefined
ar_sub_time  undefined
category     -U csml -l h_rt=120,h_vmem=2G,tmem=2G -P csml
"""


def test_parse_accounting_info():
    info = gridengine.parse_accounting_info(_TEST_QACCT_OUTPUT)
    assert info["jobnumber"] == "2467870"
    assert info["taskid"] == "undefined"


# @pytest.mark.parametrize(  # type: ignore
#     "name,state", [("12_0", "R"), ("12_1", "U"), ("12_2", "X"), ("12_3", "U"), ("12_4", "X")]
# )
# def test_read_info_array(name: str, state: str) -> None:
#     example = "JobID|State\n12_0|R\n12_[2,4-12]|X"
#     watcher = gridengine.gridengineInfoWatcher()
#     for jobid in ["12_2", "12_4"]:
#         watcher.register_job(jobid)
#     output = watcher.read_info(example)
#     assert output.get(name, {}).get("State", "U") == state


# @pytest.mark.parametrize(  # type: ignore
#     "job_id,expected",
#     [
#         ("12", [(12,)]),
#         ("12_0", [(12, 0)]),
#         ("20_[2-7%56]", [(20, 2, 7)]),
#         ("20_[2-7,12-17,22%56]", [(20, 2, 7), (20, 12, 17), (20, 22)]),
#         ("20_[0%1]", [(20, 0)]),
#     ],
# )
# def test_read_job_id(job_id: str, expected: tp.List[tp.Tuple[tp.Union[int, str], ...]]) -> None:
#     output = gridengine.read_job_id(job_id)
#     assert output == [tuple(str(x) for x in group) for group in expected]


@pytest.mark.parametrize(  # type: ignore
    "string,expected",
    [
        (b'Your job 2466409 ("MyTESTJOBNAME") has been submitted\n', "2466409"),
        ('Your job 2466409 ("MyTESTJOBNAME") has been submitted\n', "2466409"),
    ],
)
def test_get_id_from_submission_command(string: str, expected: str) -> None:
    output = gridengine.GridEngineExecutor._get_job_id_from_submission_command(string)
    assert output == expected


def test_get_id_from_submission_command_raise() -> None:
    with pytest.raises(utils.FailedSubmissionError):
        gridengine.GridEngineExecutor._get_job_id_from_submission_command(string=b"blublu")


# def test_watcher() -> None:
#     with mocked_slurm():
#         watcher = sge.SgeInfoWatcher()
#         assert watcher.num_calls == 0
#         state = watcher.get_state(job_id="11")
#         assert state == "UNKNOWN"
#         assert set(watcher._info_dict.keys()) == {"12"}
#         watcher.clear()
#         assert watcher._registered == {"11"}


# def test_get_default_parameters() -> None:
#     defaults = sge._get_default_parameters()
#     assert defaults["nodes"] == 1


def test_name() -> None:
    assert gridengine.GridEngineExecutor.name() == "sge"
