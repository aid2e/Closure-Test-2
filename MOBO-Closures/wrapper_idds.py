from ProjectUtils.config_editor import *
from ProjectUtils.mobo_utilities import *

import os, pickle, torch, argparse, datetime
import time

import pandas as pd
from ax import *

import numpy as np

from ax.metrics.noisy_function import GenericNoisyFunctionMetric
from ax.service.utils.report_utils import exp_to_df

# Model registry for creating multi-objective optimization models.
from ax.modelbridge.registry import Models

from ax.core.metric import Metric
from botorch.utils.multi_objective.box_decompositions.dominated import (
    DominatedPartitioning,
)

from botorch.test_functions.multi_objective import DTLZ2

import matplotlib.pyplot as plt

import wandb

from idds.iworkflow.workflow import workflow       # workflow    # noqa F401
from idds.iworkflow.work import work


def RunProblem(problem, x, kwargs):
    return problem(torch.tensor(x, **kwargs).clamp(0.0, 1.0))


# @glob_fun
def ftot(x, problem, tkwargs):
    return list(RunProblem(problem, x, tkwargs))


@work
def f1_problem(xdic, problem, tkwargs):
    x = tuple(xdic[k] for k in xdic.keys())
    return float(ftot(x, problem, tkwargs)[0])


@work
def f2_problem(xdic, problem, tkwargs):
    x = tuple(xdic[k] for k in xdic.keys())
    return float(ftot(x, problem, tkwargs)[1])


@work
def f3_problem(xdic, problem, tkwargs):
    x = tuple(xdic[k] for k in xdic.keys())
    return float(ftot(x, problem, tkwargs)[2])


@work
def f4_problem(xdic, problem, tkwargs):
    x = tuple(xdic[k] for k in xdic.keys())
    return float(ftot(x, problem, tkwargs)[3])


# BNL doesn't support docker
# singularity is ok

# @workflow(service='panda', local=True, cloud='US', queue='BNL_OSG_2', init_env="singularity exec /cvmfs/unpacked.cern.ch/registry.hub.docker.com/atlasml/ml-base:latest ")
# @workflow(service='panda', local=True, cloud='US', queue='FUNCX_TEST', init_env="singularity exec /cvmfs/unpacked.cern.ch/registry.hub.docker.com/atlasml/ml-base:latest ")
# @workflow(service='panda', local=True, cloud='US', queue='BNL_OSG_2', init_env="singularity remote list; singularity remote add --no-login SylabsCloud1 cloud.sylabs.io; singularity exec library://wguanicedew/ml/idds_ml_ax_al9.sif:latest")
# @workflow(service='panda', local=True, cloud='US', queue='BNL_OSG_2', init_env="if [[ $(singularity remote list|grep cloud.sylabs.io) ]]; then echo 'already exist'; else singularity remote add --no-login SylabsCloud1 cloud.sylabs.io; fi; singularity exec library://wguanicedew/ml/idds_ml_ax_al9.sif:latest")
# @workflow(service='panda', local=True, cloud='US', queue='BNL_OSG_2', init_env="singularity remote list; singularity exec library://wguanicedew/ml/idds_ml_ax_al9.sif:latest")
# @workflow(service='panda', local=True, cloud='US', queue='BNL_OSG_2', init_env="docker run --rm -it gitlab-registry.cern.ch/wguan/mlcontainer:py311_0.0.1")
@workflow(service='panda', local=True, cloud='US', queue='BNL_OSG_2', init_env="singularity exec --pwd $(pwd) -B $(pwd):$(pwd) /cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/wguan/mlcontainer:py311_0.0.3")
def run_main(config, doMonitor, search_space, optimization_config, hv_pareto):

    N_INIT = max(config["n_initial_points"], M * (d + 1))
    BATCH_SIZE = config["n_batch"]
    N_BATCH = config["n_calls"]
    num_samples = 2 if (not config.get("MOBO_params")) else config["MOBO_params"]["num_samples"]
    warmup_steps = 2 if (not config.get("MOBO_params")) else config["MOBO_params"]["warmup_steps"]
    if (doMonitor):
        MLTracker.config["BATCH_SIZE"] = BATCH_SIZE
        MLTracker.config["N_BATCH"] = N_BATCH
        MLTracker.config["num_samples"] = num_samples
        MLTracker.config["warmup_steps"] = warmup_steps
        MLTracker.define_metric("iterations")
        logMetrics = ["MCMC Training [s]", 
                      f"Gen Acq func (q = {BATCH_SIZE}) [s]",
                      f"Trail Exec (q = {BATCH_SIZE}) [s]",
                      "HV",
                      "Increase in HV w.r.t true pareto",
                      "HV Calculation [s]",
                      "Total time [s]"]
        for l in logMetrics:
            MLTracker.define_metric(l, step_metric = "iterations")
    hv_list = []
    time_gen = []
    time_mcmc = []
    time_hv = []
    time_tot = []
    time_trail = []
    converged_list = []
    hv = 0.0
    model = None
    last_call = 0
    
    if(not jsonFile):
        start_tot = time.time()
        experiment = build_experiment(search_space,optimization_config)
        start_gen = time.time()
        data = initialize_experiment(experiment,N_INIT)
        end_gen = time.time()
        exp_df = exp_to_df(experiment)
        outcomes = torch.tensor(exp_df[names[:M]].values, **tkwargs)
        start_hv = time.time()
        partitioning = DominatedPartitioning(ref_point=problem.ref_point, Y=outcomes)
        try:
            hv = partitioning.compute_hypervolume().item()
        except:
            hv = 0.
        end_hv = time.time()
        end_tot = time.time()
        time_tot.append(end_tot - start_tot)
        time_gen.append(end_gen - start_gen)
        time_hv.append(end_hv - start_hv)
        time_mcmc.append(-1.)
        time_trail.append(-1.)
        hv_list.append(hv)
        print(f"Initialized points, HV: {hv}")
        with open(os.path.join(outdir, "ax_state_init.json"), 'wb') as handle:
            list_dump = {"last_call": last_call,
                         "experiment": experiment,
                         "HV_PARETO": hv_pareto,
                         "hv_list": hv_list,
                         "data": data,
                         "outcomes": outcomes,
                         "time_tot": time_tot,
                         "time_gen": time_gen,
                         "time_hv": time_hv,
                         "time_mcmc" : time_mcmc,
                         "time_trail" : time_trail
                         }
            pickle.dump(list_dump, handle, pickle.HIGHEST_PROTOCOL)
            print("saved initial generation file")
    if (jsonFile): 
        print("\n\n WARNING::YOU ARE LOADING AN EXISTING FILE: ", jsonFile, "\n\n")
        tmp_list = pickle.load(open(jsonFile, "rb" ))
        last_call = tmp_list["last_call"]
        experiment = tmp_list["experiment"]
        hv_pareto = tmp_list["HV_PARETO"]
        hv_list = tmp_list["hv_list"]
        hv = hv_list[-1]
        data = tmp_list["data"]
        outcomes = tmp_list["outcomes"]
        time_tot = tmp_list["time_tot"]
        time_gen = tmp_list["time_gen"]
        time_hv = tmp_list["time_hv"]
        time_mcmc = tmp_list["time_mcmc"]
        time_trail = tmp_list["time_trail"]
        
    tol = config["hv_tolerance"]
    max_calls = config["max_calls"]
    converged = (hv_pareto - hv)/hv_pareto 
    converged_list.append(converged)
    check_imp = True
    roll = 30
    roll2 = min(len(hv_list)-1, 2*roll)
    if (len(hv_list) > roll):
        tmp_tol = 1. if hv_list[-roll]==0. else abs((hv_list[-1] - hv_list[-roll])/hv_list[-roll])

        # atleast 5% improvement w.r.t. last 5 calls and last call is better than first call
        check_imp = (tmp_tol > 0.0001) or (hv_list[-roll2] >= hv_list[-1]) #or (abs((hv_list[-1] - hv_list[1])/hv_list[1]) < 0.01)
        
    if (profiler):
        Profile_data = {"time_tot": time_tot,
                        "time_gen": time_gen,
                        "time_hv": time_hv,
                        "time_mcmc" : time_mcmc,
                        "time_trail" : time_trail,
                        "hv_list" : hv_list,
                        "converged_list" : converged_list
                        }
        pd.DataFrame(Profile_data).to_csv(os.path.join(outdir, "profile_data.csv"))
    if (doMonitor and jsonFile):
        logMetrics = {f"Trail Exec (q = {BATCH_SIZE}) [s]" : time_trail[-1],
                      "HV": hv,
                      "Increase in HV w.r.t true pareto": converged,
                      "HV Calculation [s]": time_hv[-1],
                      "Total time [s]": time_tot[-1],
                      "iterations": last_call
                      }
        MLTracker.log(logMetrics)

    model = Models.FULLYBAYESIANMOO(
        experiment=experiment,
        data=data,
        num_samples=num_samples,
        warmup_steps=warmup_steps,
        torch_device=tkwargs["device"],
        torch_dtype=tkwargs["dtype"],
        verbose=False,  # Set to True to print stats from MCMC
        disable_progbar=False,  # Set to False to print a progress bar from MCMC
    )

    generator_run = model.gen(BATCH_SIZE)

    while(converged > tol and last_call <= max_calls and check_imp):
        start_tot = time.time()
        start_mcmc = time.time()
        end_mcmc = time.time()
        start_gen = time.time()

        end_gen = time.time()
        start_trail = time.time()

        trial = experiment.new_batch_trial(generator_run=generator_run)
        # print(trial)
        trial.run()

        end_trail = time.time()
        data = Data.from_multiple_data([data, trial.fetch_data()])
        exp_df = exp_to_df(experiment)
        outcomes = torch.tensor(exp_df[names[:M]].values, **tkwargs)
        start_hv = time.time()
        partitioning = DominatedPartitioning(ref_point=problem.ref_point, Y=outcomes)
        try:
            hv = partitioning.compute_hypervolume().item()
        except:
            hv = 0.
        
        end_hv = time.time()
        end_tot = time.time()
        
        last_call += 1
        converged = (hv_pareto - hv)/hv_pareto
        hv_list.append(hv)
        if (len(hv_list) > roll):
            tmp_tol = 1. if(hv_list[-roll] == 0.) else abs((hv_list[-1] - hv_list[-roll])/hv_list[-roll])
            # atleast 5% improvement w.r.t. last #roll calls and last call is better than first call
            check_imp = (tmp_tol > 0.0001) or (hv_list[-roll2] >= hv_list[-1]) #or (abs((hv_list[-1] - hv_list[1])/hv_list[-1]) < 0.01)
        time_tot.append(end_tot - start_tot)
        time_mcmc.append(end_mcmc - start_mcmc)
        time_gen.append(end_gen - start_gen)
        time_trail.append(end_trail - start_trail)
        time_hv.append(end_hv - start_hv)
        converged_list.append(converged)
        roll2+=1
        
        with open(os.path.join(outdir, optimInfo), "a") as f:
            f.write("Optimization call: " + str(last_call) + "\n")
            f.write("Optimization HV: " + str(hv) + "\n")
            f.write(f"Optimization Pareto HV - HV / Pareto HV: {converged:.4f} \n")
            f.write("Optimization converged: " + str(converged < tol) + "\n")
        
        if last_call % save_every_n == 0:
            with open(os.path.join(outdir, f'optim_iteration_{last_call}.json'), 'wb') as handle:
                list_dump = {"last_call": last_call,
                             "experiment": experiment,
                             "HV_PARETO": hv_pareto,
                             "hv_list": hv_list,
                             "data": data,
                             "outcomes": outcomes,
                             "time_tot": time_tot,
                             "time_gen": time_gen,
                             "time_hv": time_hv,
                             "time_mcmc" : time_mcmc,
                             "time_trail" : time_trail
                             }
                pickle.dump(list_dump, handle)
                print(f"saved the file for {last_call} iteration")
            if (profiler):
                Profile_data = {"time_tot": time_tot,
                                "time_gen": time_gen,
                                "time_hv": time_hv,
                                "time_mcmc" : time_mcmc,
                                "time_trail" : time_trail,
                                "hv_list" : hv_list,
                                "converged_list" : converged_list
                                }
                pd.DataFrame(Profile_data).to_csv(os.path.join(outdir, "profile_data.csv"))
            if (doMonitor):
                logMetrics = {"MCMC Training [s]" : time_mcmc[-1],
                            f"Gen Acq func (q = {BATCH_SIZE}) [s]": time_gen[-1],
                            f"Trail Exec (q = {BATCH_SIZE}) [s]" : time_trail[-1],
                            "HV": hv,
                            "Increase in HV w.r.t true pareto": converged,
                            "HV Calculation [s]": time_hv[-1],
                            "Total time [s]": time_tot[-1],
                            "iterations" : last_call
                            }
                MLTracker.log(logMetrics)
    if MLTracker is not None:
        MLTracker.finish()
        
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description= "Optimization Closure Test-2")
    parser.add_argument('-c', '--config',
                        help='Optimization configuration file',
                        type = str, required = True)
    parser.add_argument('-j', '--json_file',
                        help = "The json file to load and continue optimization",
                        type = str, required=False)
    parser.add_argument('-s', '--secret_file',
                        help = "The file containing the secret key for weights and biases",
                        type = str, required = False,
                        default = "secrets.key")
    parser.add_argument('-p', '--profile',
                        help = "Profile the code",
                        type = bool, required = False,
                        default = False)
    args = parser.parse_args()

    # READ SOME INFO 
    config = ReadJsonFile(args.config)
    jsonFile = args.json_file
    profiler = args.profile
    outdir = config["OUTPUT_DIR"]
    save_every_n = config["save_every_n_call"]
    doMonitor = (True if config.get("WandB_params") else False) and profiler
    MLTracker = None
    if (doMonitor):
        if (not os.getenv("WANDB_API_KEY") and not os.path.exists(args.secret_file)):
            print ("Please set WANDB_API_KEY in your environment variables or include a file named secrets.key in the same directory as this script.")
            sys.exit()
        else:
            os.environ["WANDB_API_KEY"] = ReadJsonFile(args.secret_file)["WANDB_API_KEY"] if not os.getenv("WANDB_API_KEY") else os.environ["WANDB_API_KEY"]
            wandb.login(anonymous='never', key = os.environ['WANDB_API_KEY'], relogin=True)
            track_config = {"n_design_params": config["n_design_params"], "n_objectives" : config["n_objectives"]}
            MLTracker = wandb.init(config = track_config, **config["WandB_params"])

    optimInfo = "optimInfo.txt" if not jsonFile else "optimInfo_continued.txt"
    if(not os.path.exists(outdir)):
        os.makedirs(outdir)
    d = config["n_design_params"]
    M = config["n_objectives"]
    isGPU = torch.cuda.is_available()
    tkwargs = {
        "dtype": torch.double,
        "device": torch.device("cuda" if isGPU else "cpu"),
    }

    with open(os.path.join(outdir, optimInfo), "w") as f:
        f.write("Optimization Info with name : " + config["name"] + "\n")
        f.write("Optimization has " + str(config["n_objectives"]) + " objectives\n")
        f.write("Optimization has " + str(config["n_design_params"]) + " design parameters\n")
        f.write("Optimization Info with description : " + config["description"] + "\n")
        f.write("Starting optimization at " + str(datetime.datetime.now()) + "\n")
        f.write(f"Optimization is running on {os.uname().nodename}\n")
        f.write("Optimization description : " + config["description"] + "\n")
        if(isGPU):
            f.write("Optimization is running on GPU : " + torch.cuda.get_device_name() + "\n")
    print ("Running on GPU? ", isGPU)

    problem = DTLZ2(dim=d, num_objectives=M, negate=True).to(**tkwargs)

    problem.ref_point = torch.tensor([-max(1.1, d/10.) for _ in range(M)], **tkwargs)

    print ("Problem Reference points : ", problem.ref_point)

    NPoints = 10000
    pareto_fronts = problem.gen_pareto_front(NPoints//10)
    hv_pareto = DominatedPartitioning(ref_point=problem.ref_point,
                                      Y=pareto_fronts
                                      ).compute_hypervolume().item()
    print (f"Pareto Front Hypervolume: {hv_pareto}")
    n_points = problem(torch.rand(NPoints*d, **tkwargs).reshape(NPoints, d))
    hv_npoints = DominatedPartitioning(ref_point=problem.ref_point,
                                       Y=n_points
                                       ).compute_hypervolume().item()
    print (f"Random Points Hypervolume: {hv_npoints}")

    if (doMonitor):
        MLTracker.summary["HV"] = hv_pareto
        MLTracker.summary["HV_RandomPoints"] = hv_npoints
        MLTracker.summary["ref_point"] = str(problem.ref_point.tolist())

    with open(os.path.join(outdir, optimInfo), "a") as f:
        f.write("Problem Reference points : " + str(problem.ref_point) + "\n")
        f.write("Problem Pareto Front Hypervolume : " + str(hv_pareto) + "\n")
        f.write("Problem Random Points Hypervolume : " + str(hv_npoints) + "\n")

    def f1(xdic):
        return f1_problem(xdic, problem, tkwargs)

    # here f1_problem is defined with wrapper @work, it will be automatically
    # converted to a PanDA-iDDS task.
    # if calls f1_problem with f1_problem(xdic, problem, tkwargs, multi_jobs_kwargs_list=[{'xdic': xdic1}, {'xdic': xdic2}, {'xdic': xdic3}]),
    # the wrapper will automatically convert it to a PanDA task with three jobs:
    # f1_problem(xdic1, problem, tkwargs), f1_problem(xdic2, problem, tkwargs, f1_problem(xdic3, problem, tkwargs)
    # todo: how to let AX to generate multiple trial parameters in one call. one solution is to create
    # a new child class BatchTrial in AX.

    def f2(xdic):
        return f2_problem(xdic, problem, tkwargs)

    def f3(xdic):
        return f3_problem(xdic, problem, tkwargs)

    def f4(xdic):
        return f4_problem(xdic, problem, tkwargs)

    search_space = SearchSpace(
        parameters=[
            RangeParameter(name=f"x{i}",
                           lower=0, upper=1,
                           parameter_type=ParameterType.FLOAT)
            for i in range(d)],
        )
    param_names = [f"x{i}" for i in range(d)]

    names = ["a", "b", "c", "d"]
    functions = [f1, f2, f3, f4]
    metrics = []

    for name, function in zip(names[:M], functions[:M]):
        metrics.append(
            GenericNoisyFunctionMetric(
                name=name, f=function, noise_sd=0.0, lower_is_better=False
            )
        )

    mo = MultiObjective(
        objectives=[Objective(m) for m in metrics],
        )
    objective_thresholds = [
        ObjectiveThreshold(metric=metric, bound=val, relative=False)
        for metric, val in zip(mo.metrics, problem.ref_point.to(tkwargs["device"]))
        ]
    optimization_config = MultiObjectiveOptimizationConfig(objective=mo,
                                                           objective_thresholds=objective_thresholds,)

    run_main(config, doMonitor, search_space, optimization_config, hv_pareto)
