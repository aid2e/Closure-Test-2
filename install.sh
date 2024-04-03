#!/bin/bash

[source setup_mini_conda.sh]

conda_dir=[your_dir]
conda env create --prefix=$conda_dir  -f conda_almalinux9_requirements.txt
conda activate $conda_dir

# required by AX
# conda install -y -c conda-forge postgresql

# pip install ax ax.metrics
# git clone https://github.com/axonchisel/ax_metrics.git
# fix the setup
# python setup.py install

# ax is not ax-platform
pip install ax-platform

pip install torch pandas numpy matplotlib wandb botorch

pip install idds-client idds-common idds-workflow panda-client
