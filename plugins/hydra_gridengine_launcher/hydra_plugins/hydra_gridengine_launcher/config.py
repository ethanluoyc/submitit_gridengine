# Copyright (c) 2020 Yicheng Luo.
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from hydra.core.config_store import ConfigStore
from hydra_plugins.hydra_submitit_launcher.config import BaseQueueConf


@dataclass
class GridEngineQueueConf(BaseQueueConf):
    _target_: str = (
        "hydra_plugins.hydra_gridengine_launcher.submitit_launcher.GridEngineLauncher"
    )


ConfigStore.instance().store(
    group="hydra/launcher",
    name="submitit_gridengine",
    node=GridEngineQueueConf(),
    provider="submitit_launcher",
)
