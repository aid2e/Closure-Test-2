# Closure-Test-2
Closure test with a iDDS/PaNDA integration

# Ideal scenario

I use my local resource (like a laptop or a node) to run MOBO/MOEA optimization of a EIC (ePIC) detector system, the optimization algorithm suggest a set of design points to be evaluated. These design points gets submitted through iDDS/PaNDA as a task for distributed computing, iDDS/PaNDA runs the detector simulation and evaluation for each design point, collects the results and send them back to my local node. My local node then continues the optimization loop based on the new results. 

Note: More detailed workflow will evolve as the project progresses. The goal is to leverage existing iDDS/PaNDA infrastructure for distributed computing while keeping MOBO/MOEA based optimization workflow local.

# Integrating MOBO and ePIC software stacks

There are two ways one can think about the integration. 
1. Running within a given infracstructure like iDDS/PaNDA:


# Updates on 27th June 2024

Combined the Joblib Runner and PanDA idds runner into one. 

To use, add additional argument `--runner` it takes in two values, `joblib` or `panda`.\

# TO DO

* Convert the optimization loop into Scheduler. Use a scheduler which automatically checks if trails are completed in a fixed interval of time and submits subsequent trails.
* Create a flag to run "one Execution" for each design point returning all the objectives instead of running M executions (jobs) corresponding to M objectives
