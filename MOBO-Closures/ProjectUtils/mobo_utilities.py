import time
import torch # pytorch package, allows using GPUs

from ax.metrics.noisy_function import GenericNoisyFunctionMetric
from ax.service.utils.report_utils import exp_to_df  #https://ax.dev/api/service.html#ax.service.utils.report_utils.exp_to_df
from ax.runners.synthetic import SyntheticRunner

# Plotting imports and initialization
#from ax.utils.notebook.plotting import render, init_notebook_plotting
from ax.plot.contour import plot_contour
from ax.plot.pareto_utils import compute_posterior_pareto_frontier
from ax.plot.pareto_frontier import plot_pareto_frontier
#init_notebook_plotting()

# Model registry for creating multi-objective optimization models.
from ax.modelbridge.registry import Models

# Analysis utilities, including a method to evaluate hypervolumes
from ax.modelbridge.modelbridge_utils import observed_hypervolume

from ax import SumConstraint
from ax import OrderConstraint
from ax import ParameterConstraint
from ax.core.search_space import SearchSpace
from ax.core.parameter import RangeParameter,ParameterType

from ax.core.objective import MultiObjective, Objective, ScalarizedObjective
from ax.core.optimization_config import ObjectiveThreshold, MultiObjectiveOptimizationConfig

from ax.core.experiment import Experiment

from botorch.utils.multi_objective.box_decompositions.dominated import DominatedPartitioning
from ax.core.data import Data

from ax.core.types import ComparisonOp

from sklearn.utils import shuffle
from functools import wraps, lru_cache

from matplotlib import pyplot as plt

from matplotlib.cm import ScalarMappable




#---------------------- TOY FUNCTIONS ------------------------#

def glob_dummy(loc_fun):
    @wraps(loc_fun)
    # @lru_cache(maxsize=None)
    def inner(xdic):
        x_sorted = [xdic[p_name] for p_name in xdic.keys()] #it assumes x will be given as, e.g., dictionary
        res = loc_fun(x_sorted)
        return res

    return inner
# Define the objectives
@glob_dummy
def f1(x):
  res = np.sum(x*x)
  return res #obj1

@glob_dummy
def f2(x):
  res = 1./(np.sum(x*x)+0.01)
  return res #obj2
"""
def glob_fun(loc_fun):
  @lru_cache(maxsize=None)
  def inner(xdic):
    x_sorted = [xdic[p_name] for p_name in xdic.keys()]
    res = list(loc_fun(x_sorted))
    return res
  return inner
"""
def glob_fun(loc_fun):
    @wraps(loc_fun)
    # @lru_cache(maxsize=None)
    def inner(xsorted):
      res = list(loc_fun(xsorted))
      return res
    return inner

#---------------------- BOTORCH FUNCTIONS ------------------------#

def build_experiment(search_space,optimization_config):
    experiment = Experiment(
        name="pareto_experiment",
        search_space=search_space,
        optimization_config=optimization_config,
        runner=SyntheticRunner(),
    )
    return experiment


def build_experiment_pandaidds(search_space, optimization_config, runner):
    experiment = Experiment(
        name="pareto_experiment",
        search_space=search_space,
        optimization_config=optimization_config,
        runner=runner,
    )
    return experiment


def initialize_experiment(experiment,N_INIT):
    sobol = Models.SOBOL(search_space=experiment.search_space)

    batch_trial = experiment.new_batch_trial(sobol.gen(N_INIT))

    print("initialize trial")
    # print(batch_trial)
    batch_trial.run()
    print("initialized trial")

    return experiment.fetch_data() # Runs the simulation.


def initialize_experiment_pandaidds(experiment,N_INIT):
    sobol = Models.SOBOL(search_space=experiment.search_space)

    batch_trial = experiment.new_batch_trial(sobol.gen(N_INIT))

    print("initialize trial")
    # print(batch_trial)
    batch_trial.run()
    print("initialized trial")

    print("Waiting for batch trial to terminate")
    print(batch_trial)
    time_checkpoint = time.time()
    while not batch_trial.status.is_terminal:
        batch_trial.runner.poll_trial_status([batch_trial])

        if time.time() - time_checkpoint > 300:
            print("waiting for terminating")
            print(batch_trial)
            time_checkpoint = time.time()
        time.sleep(30)

    print("batch trial is terminated")
    print(batch_trial)

    return experiment.fetch_data() # Runs the simulation.
