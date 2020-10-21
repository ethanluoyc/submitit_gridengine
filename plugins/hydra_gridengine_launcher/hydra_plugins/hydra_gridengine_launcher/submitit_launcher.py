# Copyright (c) 2020 Yicheng Luo.
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from hydra import TaskFunction
from hydra.core.config_loader import ConfigLoader
from hydra.core.config_search_path import ConfigSearchPath
from hydra.core.singleton import Singleton
from hydra.core.utils import JobReturn, filter_overrides, run_job, setup_globals
from hydra.plugins.launcher import Launcher
from hydra.plugins.search_path_plugin import SearchPathPlugin
from hydra_plugins.hydra_submitit_launcher import submitit_launcher
from hydra_plugins.hydra_submitit_launcher.config import BaseQueueConf
from omegaconf import DictConfig, OmegaConf, open_dict


log = logging.getLogger(__name__)


class SubmititLauncherSearchPathPlugin(SearchPathPlugin):
    def manipulate_search_path(self, search_path: ConfigSearchPath) -> None:
        search_path.append(
            "hydra-gridengine-launcher",
            "pkg://hydra_plugins.hydra_gridengine_launcher.conf",
        )


class GridEngineLauncher(submitit_launcher.BaseSubmititLauncher):
    _EXECUTOR = "gridengine"
