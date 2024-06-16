
import pandas as pd

from collections import defaultdict
from typing import Iterable

from ax.core.base_trial import BaseTrial, TrialStatus
from ax.core.data import Data
from ax.core.metric import Metric, MetricFetchResult, MetricFetchE
from ax.core.runner import Runner
from ax.utils.common.result import Ok, Err

from idds.iworkflow.workflow import workflow as workflow_def      # workflow    # noqa F401
from idds.iworkflow.work import work as work_def


# @workflow(service='panda', local=True, cloud='US', queue='BNL_OSG_2', init_env="singularity exec --pwd $(pwd) -B $(pwd):$(pwd) /cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/wguan/mlcontainer:py311_0.0.3")
# @workflow_def(service='panda', local=True, cloud='US', queue='BNL_OSG_2', return_workflow=True, init_env="singularity exec --pwd $(pwd) -B $(pwd):$(pwd) /cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/wguan/mlcontainer:py311_0.0.3")
# @workflow_def(service='panda', source_dir=None, source_dir_parent_level=1, local=True, cloud='US', queue='FUNCX_TEST', return_workflow=True, init_env="singularity exec --pwd $(pwd) -B $(pwd):$(pwd) /cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/wguan/mlcontainer:py311_0.0.3")
@workflow_def(service='panda', source_dir=None, source_dir_parent_level=1, local=True, cloud='US', queue='BNL_OSG_1', exclude_source_files=["DTLZ2*", ".*json", ".*log"], return_workflow=True, max_walltime=3600, init_env="singularity exec --pwd $(pwd) -B $(pwd):$(pwd) /cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/wguan/mlcontainer:py311_0.0.3")
def empty_workflow_func():
    pass


class PanDAIDDSRunner(Runner):
    def __init__(self, runner_funcs=None):
        self.runner_funcs = runner_funcs
        self.transforms = {}
        self.workflow = None

        # self.workflow = empty_workflow_func()
        # print(self.workflow)
        # self.workflow.pre_run()

    def run(self, trial: BaseTrial):
        self.workflow = empty_workflow_func()
        print(self.workflow)
        self.workflow.pre_run()

        print(self.runner_funcs)
        if not isinstance(trial, BaseTrial):
            raise ValueError("This runner only handles `BaseTrial`.")

        params_list = []
        for arm in trial.arms:
            params_list.append([arm.parameters])
        # print(params_list)

        name_results = {}
        self.transforms[trial.index] = {}
        for name in self.runner_funcs:
            # one work is one objective
            # with multiple objectives, there will be multiple work objects

            function = self.runner_funcs[name]['function']
            pre_kwargs = self.runner_funcs[name]['pre_kwargs']
            work = work_def(function, workflow=self.workflow, pre_kwargs=pre_kwargs, return_work=True, map_results=True)
            w = work(multi_jobs_kwargs_list=params_list)
            w.store()
            print("trial %s: create a task for %s: %s" % (trial.index, name, w))
            self.transforms[trial.index][name] = {'tf_id': None, 'work': w, 'results': None}

        # prepare workflow is after the work.store.
        # in this way, the workflow will upload the work's files
        self.workflow.store()
        self.workflow.prepare()
        self.workflow.submit()

        for name in self.transforms[trial.index]:
            w = self.transforms[trial.index][name]['work']
            tf_id = w.submit()
            w.init_async_result()
            self.transforms[trial.index][name] = {'tf_id': tf_id, 'work': w, 'results': None}
            name_results[name] = None
        return {'name_results': name_results}

    def get_trial_status(self, trial: BaseTrial):
        name_results = trial.run_metadata.get("name_results")
        all_terminated, all_finished, all_failed = True, True, True

        for name in self.transforms[trial.index]:
            w = self.transforms[trial.index][name]['work']
            w.init_async_result()

            results = w.get_results()
            # print(results)

            self.transforms[trial.index][name]['results'] = results
            name_results[name] = results

            if not w.is_terminated():
                all_terminated, all_finished, all_failed = False, False, False
            else:
                if not w.is_finished():
                    all_finished = False
                if not w.is_failed():
                    all_failed = False

        trial.update_run_metadata({'name_results': name_results})

        if all_finished:
            if self.workflow:
                self.workflow.close()
            return TrialStatus.COMPLETED
        elif all_failed:
            return TrialStatus.FAILED
        elif all_terminated:
            # no subfinished status
            return TrialStatus.COMPLETED

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


class PanDAIDDSMetric(Metric):  # Pulls data for trial from external system.
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

            return Ok(value=Data(df=pd.DataFrame.from_records(df_dict_list)))
        except Exception as e:
            return Err(
                MetricFetchE(message=f"Failed to fetch {self.name}", exception=e)
            )
