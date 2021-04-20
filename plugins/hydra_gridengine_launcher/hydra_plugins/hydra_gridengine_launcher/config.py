# Copyright (c) 2020 Yicheng Luo.
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from hydra.core.config_store import ConfigStore
from hydra_plugins.hydra_submitit_launcher.config import BaseQueueConf, SlurmQueueConf


@dataclass
class GridEngineQueueConf:
    _target_: str = (
        "hydra_plugins.hydra_gridengine_launcher.gridengine_launcher.GridEngineLauncher"
    )

    merge: bool = False
    submitit_folder: str = "${hydra.sweep.dir}/.submitit/%j"
    job_name: str = "submitit"
    num_gpus: int = 0
    shell: str = "/bin/bash"
    tc: Optional[str] = None
    tmem: str = "4G"
    h_vmem: str = "4G"
    h_rt: str = "01:00:00"
    smp: int = 1


ConfigStore.instance().store(
    group="hydra/launcher",
    name="submitit_gridengine",
    node=GridEngineQueueConf(),
    provider="hydra_gridengine_launcher",
)
