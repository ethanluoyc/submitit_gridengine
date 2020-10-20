#!/usr/bin/env python
#
# Copyright (c) Yicheng Luo.
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
#

import re
from pathlib import Path

from setuptools import find_packages, setup

requirements = {}
for name in ["main", "dev"]:
    requirements[name] = Path(f"requirements/{name}.txt").read_text().splitlines()

init_str = Path("submitit_gridengine/__init__.py").read_text()
match = re.search(r"^__version__ = \"(?P<version>[\w\.]+?)\"$", init_str, re.MULTILINE)
assert match is not None, "Could not find version in submitit/__init__.py"
version = match.group("version")


setup(
    name="submitit_gridengine",
    version=version,
    description="Python 3.6+ toolbox for submitting jobs to SGE",
    author="Yicheng Luo",
    python_requires=">=3.6",
    long_description=Path("README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    project_urls={},
    packages=find_packages(),
    install_requires=requirements["main"],
    extras_require={"dev": requirements["dev"]},
    # Mark the package as compatible with types.
    # https://mypy.readthedocs.io/en/latest/installed_packages.html#making-pep-561-compatible-packages
    # Register as a plugin for submitit
    entry_points="""[submitit]
executor=submitit_gridengine:GridEngineExecutor
job_environment=submitit_gridengine:GridEngineJobEnvironment
""",
    package_data={"submitit_gridengine": ["py.typed"]},
    zip_safe=False,
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Topic :: System :: Distributed Computing",
        "Development Status :: 5 - Production/Stable",
    ],
)
