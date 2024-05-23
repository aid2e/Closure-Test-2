# Closure test 1 

The folder contains scripts that are used to run computation time studies under the following conditions 

* problem - `DTLZ2`
* `M` Objectives - `[3, 4, 5]`
* `d` design parameters - `[5, 10, 50, 100]`
* Surrogate model - `SAASBO`
* Acquisition Function - `qNEHVI`

This documentation will evolve

# run run_grid.sh to generate optimize config files
 
    python run_grid.sh optimize.config

# run PanDA/iDDS jobs
# pip install --upgrade idds-client idds-common idds-workflow
   
    python wrapper_panda_idds.py -c optimize_M2_d5.config
