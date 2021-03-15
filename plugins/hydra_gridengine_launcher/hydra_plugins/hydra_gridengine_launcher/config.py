# Copyright (c) 2020 Yicheng Luo.
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from hydra.core.config_store import ConfigStore
from hydra_plugins.hydra_submitit_launcher.config import BaseQueueConf

@dataclass
class GridEngineQueueConf:
    _target_: str = (
        "hydra_plugins.hydra_gridengine_launcher.submitit_launcher.GridEngineLauncher"
    )
    submitit_folder: str = "${hydra.sweep.dir}/.submitit/%j"
    merge = False
    job_name = 'submitit'
    num_gpus = 0
    shell = '/bin/bash'
    tc = None
    tmem = '2G'
    h_vmem = '2G'
    h_timeout = '00:02:00'
    


ConfigStore.instance().store(
    group="hydra/launcher",
    name="submitit_gridengine",
    node=GridEngineQueueConf(),
    provider="submitit_launcher",
)
