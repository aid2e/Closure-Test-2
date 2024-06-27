import pandas as pd

from collections import defaultdict
from typing import Iterable

from ax.core.base_trial import BaseTrial, TrialStatus
from ax.core.data import Data
from ax.core.metric import Metric, MetricFetchResult, MetricFetchE
from ax.core.runner import Runner
from ax.utils.common.result import Ok, Err

def run_workflow_func(func, tkwargs):
    return func(**tkwargs)

class JobLibRunner(Runner):
    def __init__(self, runner_funcs=None):
        self.runner_funcs = runner_funcs
        self.transforms = {}

    def run(self, trial: BaseTrial):

        print(self.runner_funcs)
        if not isinstance(trial, BaseTrial):
            raise ValueError("This runner only handles `BaseTrial`.")

        params_list = []
        for arm in trial.arms:
            params_list.append([arm.parameters])
        # print(params_list)

        name_results = {}
        self.transforms[trial.index] = {}
        self.transforms[trial.index][self.retries] = {}
        for name in self.runner_funcs:
            # one work is one objective
            # with multiple objectives, there will be multiple work objects
            function = self.runner_funcs[name]['function']
            pre_kwargs = self.runner_funcs[name]['pre_kwargs']
            w = run_workflow_func(function, pre_kwargs)
            # w.store()
            print("trial %s: create a task for %s: %s" % (trial.index, name, w))
            name_results[name] = w
        return {'name_results': name_results}

    def verify_results(self, trial: BaseTrial):
        name_results = trial.run_metadata.get("name_results")
        all_arms_finished = True,
        unfinished_arms = {}
        for arm in trial.arms:
            # all_has_results = True
            for name in self.transforms[trial.index][self.retries]:
                results = name_results.get(name, None)
                ret = results.get_result(name=None, args=[arm.parameters])
                if ret is None:
                    # all_has_results = False
                    all_arms_finished = False
                    if name not in unfinished_arms:
                        unfinished_arms[name] = []
                    unfinished_arms[name].append(arm)
            """
            if not all_has_results:
                all_arms_finished = False
                for name in self.transforms[trial.index]:
                    results = name_results.get(name, None)
                    results.set_result(name=None, args=[arm.parameters], value=None)
            """
        return all_arms_finished, unfinished_arms

    
    def get_trial_status(self, trial: BaseTrial):
        name_results = trial.run_metadata.get("name_results")
        all_finished = False
        for k, v in name_results:
            if k and v:
                all_finished = True
            else:
                all_finished = False
            

        if all_finished:
            return TrialStatus.COMPLETED
        elif not all_finished:
            self.verify_results(trial)
            return TrialStatus.FAILED

        return TrialStatus.RUNNING

    def poll_trial_status(self, trials: Iterable[BaseTrial]):
        # print("poll_trial_status")
        status_dict = defaultdict(set)
        for trial in trials:
            status = self.get_trial_status(trial)
            # print("trail %s: status: %s" % (trial.index, status))
            status_dict[status].add(trial.index)
            try:
                trial.mark_as(status)
            except Exception:
                pass
            except ValueError:
                pass
        # print(status_dict)
        return status_dict


class JobLibMetric(Metric):  # Pulls data for trial from external system.
    # def __init__(self, name: str, lower_is_better: Optional[bool] = None, properties: Optional[Dict[str, Any]] = None, function: optional[Any] = None) -> None:

    def fetch_trial_data(self, trial: BaseTrial) -> MetricFetchResult:
        print("fetch_trial_data")
        if not isinstance(trial, BaseTrial):
            raise ValueError("This metric only handles `BaseTrial`.")

        try:
            name_results = trial.run_metadata.get("name_results")
            df_dict_list = []
            results = name_results.get(self.name, None)

            if results is not None:
                for arm in trial.arms:
                    ret = results.get_result(name=None, args=[arm.parameters])
                    if ret is not None:
                        df_dict = {
                            "trial_index": trial.index,
                            "metric_name": self.name,
                            "arm_name": arm.name,
                            "mean": ret,
                            "sem": 0.0   # unkown noise
                        }
                        df_dict_list.append(df_dict)
                    else:
                        print(f"{self.name} misses result for {arm.name}")
                        df_dict = {
                            "trial_index": trial.index,
                            "metric_name": self.name,
                            "arm_name": arm.name,
                            "mean": ret,
                            "sem": 0.0   # unkown noise
                        }
                        df_dict_list.append(df_dict)

            return Ok(value=Data(df=pd.DataFrame.from_records(df_dict_list)))
        except Exception as e:
            return Err(
                MetricFetchE(message=f"Failed to fetch {self.name}", exception=e)
            )
