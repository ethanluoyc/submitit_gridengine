"""Mocks for helping testing SGE."""
# pylint: disable=redefined-outer-name
import contextlib
import sys
import time
from pathlib import Path
from typing import Any, Iterator, List, Optional, Union
from unittest.mock import patch

import pytest

from submitit.core import core, submission, utils


_QACCT_OUTPUT = """==============================================================
qname        test.q
jobnumber    {job_id}
taskid       {task_id}
slots        1
failed       0
exit_status  0
"""

_QSTAT_OUTPUT = """<?xml version='1.0'?>
<job_info  xmlns:xsd="http://arc.liv.ac.uk/repos/darcs/sge/source/dist/util/resources/schemas/qstat/qstat.xsd">
  <job_info>
    <job_list state="pending">
      <JB_job_number>{job_id}</JB_job_number>
      <JAT_prio>0.00000</JAT_prio>
      <JB_name>MyTESTJOBNAME</JB_name>
      <JB_owner>yicheluo</JB_owner>
      <state>qw</state>
      <JB_submission_time>2020-10-21T00:47:11</JB_submission_time>
      <queue_name></queue_name>
      <slots>1</slots>
      {task_id}
    </job_list>
  </job_info>
</job_info>
"""

class _SecondCall:
    """Helps mocking CommandFunction which is like a subprocess check_output, but
    with a second call.
    """

    def __init__(self, outputs: Any) -> None:
        self._outputs = outputs

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._outputs

class MockedSubprocess:
    """Helper for mocking subprocess calls"""

    def __init__(
        self, state: str = "RUNNING", job_id: str = "12", shutil_which: Optional[str] = None, array: int = 0
    ) -> None:
        self.state = state
        self.shutil_which = shutil_which
        self.job_id = job_id
        self._qacct = self.qacct(state, job_id, array)
        self._qstat = self.qstat(state, job_id, array)
        self._sbatch = f"Running job {job_id}\n".encode()

    def __call__(self, command: str, **kwargs: Any) -> Any:
        if "qacct" in command:
            return self._qacct
        elif command[0] == "qstat":
            return self._qstat
        elif command[0] == "qsub":
            return "Your job {} (\"MyTESTJOBNAME\") has been submitted".format(self.job_id)
        else:
            raise ValueError(f'Unknown command to mock "{command}".')

    def qstat(self, state: str, job_id: str, array: int) -> bytes:
        if array == 0:
            lines = _QSTAT_OUTPUT.format(job_id=job_id, task_id="" , state=state)
        else:
            lines = "\n".join(_QSTAT_OUTPUT.format(
                job_id=f"{job_id}", 
                task_id="<tasks>{}</tasks>".format(i),
                state=state) for i in range(array))
        return lines.encode()

    def qacct(self, state: str, job_id: str, array: int) -> bytes:
        if array == 0:
            lines = _QACCT_OUTPUT.format(job_id=job_id, task_id="undefined" , state=state)
        else:
            lines = "\n".join(_QACCT_OUTPUT.format(job_id=f"{job_id}", task_id=str(i),  state=state) for i in range(array))
        return lines.encode()

    def which(self, name: str) -> Optional[str]:
        return "here" if name == self.shutil_which else None

    @contextlib.contextmanager
    def context(self) -> Iterator[None]:
        with patch(
            "submitit.core.utils.CommandFunction",
            new=lambda *args, **kwargs: _SecondCall(self(*args, **kwargs)),
        ):
            with patch("subprocess.check_output", new=self):
                with patch("shutil.which", new=self.which):
                    with patch("subprocess.check_call", new=self):
                        yield None