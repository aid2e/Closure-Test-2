
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
        self.retries = 0

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
        self.transforms[trial.index][self.retries] = {}
        for name in self.runner_funcs:
            # one work is one objective
            # with multiple objectives, there will be multiple work objects
            function = self.runner_funcs[name]['function']
            pre_kwargs = self.runner_funcs[name]['pre_kwargs']
            work = work_def(function, workflow=self.workflow, pre_kwargs=pre_kwargs, return_work=True, map_results=True)
            w = work(multi_jobs_kwargs_list=params_list)
            # w.store()
            print("trial %s: create a task for %s: %s" % (trial.index, name, w))
            self.transforms[trial.index][self.retries][name] = {'tf_id': None, 'work': w, 'results': None}

        # prepare workflow is after the work.store.
        # in this way, the workflow will upload the work's files
        # self.workflow.store()
        self.workflow.prepare()
        self.workflow.submit()

        for name in self.transforms[trial.index][self.retries]:
            w = self.transforms[trial.index][self.retries][name]['work']
            tf_id = w.submit()
            if not tf_id:
                raise Exception("Failed to submit work to PanDA")
            w.init_async_result()
            self.transforms[trial.index][self.retries][name] = {'tf_id': tf_id, 'work': w, 'results': None}
            name_results[name] = None
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

    def submit_retries(self, trial, unfinished_arms):
        self.retries += 1
        print(f"submit retries {self.retries} for {unfinished_arms}")
        self.transforms[trial.index][self.retries] = {}
        for name in unfinished_arms:
            # one work is one objective
            # with multiple objectives, there will be multiple work objects

            function = self.runner_funcs[name]['function']
            pre_kwargs = self.runner_funcs[name]['pre_kwargs']
            params_list = []
            for arm in unfinished_arms[name]:
                params_list.append([arm.parameters])

            work = work_def(function, workflow=self.workflow, pre_kwargs=pre_kwargs, return_work=True, map_results=True)
            w = work(multi_jobs_kwargs_list=params_list)
            # w.store()
            print("trial %s, retries %s: create a task for %s: %s" % (trial.index, self.retries, name, w))
            tf_id = w.submit()
            print("trial %s, retries %s: submit a task for %s: %s, %s" % (trial.index, self.retries, name, w, tf_id))
            if not tf_id:
                raise Exception("Failed to submit work to PanDA")
            self.transforms[trial.index][self.retries][name] = {'tf_id': tf_id, 'work': w, 'results': None}

    def get_trial_status(self, trial: BaseTrial):
        name_results = trial.run_metadata.get("name_results")
        all_terminated, all_finished, all_failed = True, True, True

        if self.retries == 0:
            for name in self.transforms[trial.index][self.retries]:
                w = self.transforms[trial.index][self.retries][name]['work']
                w.init_async_result()

                results = w.get_results()
                # print(results)

                self.transforms[trial.index][self.retries][name]['results'] = results
                name_results[name] = results

                if not w.is_terminated():
                    all_terminated, all_finished, all_failed = False, False, False
                else:
                    if not w.is_finished():
                        all_finished = False
                    if not w.is_failed():
                        all_failed = False
        else:
            for name in self.transforms[trial.index][self.retries]:
                w = self.transforms[trial.index][self.retries][name]['work']
                tf_id = self.transforms[trial.index][self.retries][name]['tf_id']
                w.init_async_result()

                results = w.get_results()
                # print(results)

                self.transforms[trial.index][self.retries][name]['results'] = results
                # name_results[name] = results

                if not w.is_terminated():
                    all_terminated, all_finished, all_failed = False, False, False
                else:
                    if not w.is_finished():
                        all_finished = False
                    if not w.is_failed():
                        all_failed = False
                if all_terminated or all_finished:
                    print(f"Work {name} terminated")
                    avail_results = name_results[name]
                    for arm in trial.arms:
                        ret = avail_results.get_result(name=None, args=[arm.parameters])
                        if ret is None:
                            print(f"Work {name} missing results for arm {arm.name}")
                            # get results from new retries
                            if results:
                                new_ret = results.get_result(name=None, args=[arm.parameters])
                                print(f"Work {name} checking results for arm {arm.name} from new transform {tf_id}, new results: {new_ret}")
                                if new_ret is not None:
                                    print(f"Work {name} set result for arm {arm.name} from new transform {tf_id} to {new_ret}")
                                    # avail_results.set_result(name=None, args=[arm.parameters], value=new_ret)
                                    name_results[name].set_result(name=None, args=[arm.parameters], value=new_ret)

        if all_finished or all_terminated:
            print(f"Work {name} terminated.")
            ret, unfinished_arms = self.verify_results(trial)
            if not ret and self.retries < 3:
                all_finished = False
                all_terminated = False
                # retry failed jobs
                self.submit_retries(trial, unfinished_arms)

        trial.update_run_metadata({'name_results': name_results})

        if all_finished:
            if self.workflow:
                self.workflow.close()
            return TrialStatus.COMPLETED
        elif all_failed:
            self.verify_results(trial)
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
