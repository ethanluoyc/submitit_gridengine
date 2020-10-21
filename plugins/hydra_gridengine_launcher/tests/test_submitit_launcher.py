# Copyright (c) 2020 Yicheng Luo.
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
import subprocess
import sys
from pathlib import Path
from typing import Type

import pytest
from hydra.core.plugins import Plugins
from hydra.plugins.launcher import Launcher
from hydra.test_utils.launcher_common_tests import (
    IntegrationTestSuite,
    LauncherTestSuite,
)
from hydra.test_utils.test_utils import chdir_plugin_root

from hydra_plugins.hydra_gridengine_launcher import submitit_launcher

chdir_plugin_root()


@pytest.mark.parametrize(  # type: ignore
    "cls", [submitit_launcher.GridEngineLauncher]
)
def test_discovery(cls: Type[Launcher]) -> None:
    # Tests that this plugin can be discovered via the plugins subsystem when looking for Launchers
    assert cls.__name__ in [x.__name__ for x in Plugins.instance().discover(Launcher)]
