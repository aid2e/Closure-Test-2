
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
# @workflow_def(service='panda', source_dir=None, source_dir_parent_level=1, local=True, cloud='US', queue='BNL_OSG_1', exclude_source_files=["DTLZ2*", ".*json", ".*log"], return_workflow=True, max_walltime=3600, init_env="singularity exec --pwd $(pwd) -B $(pwd):$(pwd) /cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/wguan/mlcontainer:py311_0.0.3")
# @workflow_def(service='panda', source_dir=None, source_dir_parent_level=1, local=True, cloud='US', queue='BNL_PanDA_1', exclude_source_files=["DTLZ2*", ".*json", ".*log"], return_workflow=True, max_walltime=3600, init_env=None)
# @workflow_def(service='panda', source_dir=None, source_dir_parent_level=1, local=True, cloud='US', queue='BNL_OSG_2', exclude_source_files=["DTLZ2*", ".*json", ".*log"], return_workflow=True, max_walltime=3600, init_env=None)
# @workflow_def(service='panda', source_dir=None, source_dir_parent_level=1, local=True, cloud='US', queue='BNL_OSG_2', exclude_source_files=["DTLZ2*", ".*json", ".*log"], return_workflow=True, max_walltime=3600, init_env="conda env create --prefix=./workdir -f environment.yml; conda activate ./workdir; pip install --no-cache-dir ax-platform torch pandas numpy matplotlib wandb botorch idds-client idds-common idds-workflow panda-client;")
# @workflow_def(service='panda', source_dir=None, source_dir_parent_level=1, local=True, cloud='US', queue='BNL_OSG_2', exclude_source_files=["DTLZ2*", ".*json", ".*log"], return_workflow=True, max_walltime=3600, init_env="python3 -m venv ./workdir; source ./workdir/bin/activate; pip install --no-cache-dir numpy==1.26.4 ax-platform torch pandas matplotlib wandb botorch idds-client idds-common idds-workflow panda-client; which python; ", clean_env="rm -fr ./workdir")
@workflow_def(service='panda', source_dir=None, source_dir_parent_level=1, local=True, cloud='US', queue='BNL_OSG_2', exclude_source_files=["DTLZ2*", ".*json", ".*log"], return_workflow=True, max_walltime=3600, init_env="source /cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/wguan/mlcontainer:py311_conda_0.9/opt/conda/setup_mamba.sh; ")
# @workflow_def(service='panda', source_dir=None, source_dir_parent_level=1, local=True, cloud='US', queue='BNL_OSG_PanDA_1', exclude_source_files=["DTLZ2*", ".*json", ".*log"], return_workflow=True, max_walltime=3600, init_env="source /cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/wguan/mlcontainer:py311_conda_0.9/opt/conda/setup_mamba.sh; source ${SINGULARITY_ENVIRONMENT}; which singularity; which apptainer; /usr/bin/singularity -h; /usr/bin/apptainer -h; ")
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
            print("trial %s: create a task for %s: (%s) %s" % (trial.index, name, w.internal_id, w))
            self.transforms[trial.index][self.retries][name] = {'tf_id': None, 'work': w, 'results': None, 'status': 'new'}

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
            self.transforms[trial.index][self.retries][name] = {'tf_id': tf_id, 'work': w, 'results': None, 'status': 'new'}
            name_results[name] = {'status': 'running', 'retries': 0, 'result': None}
        return {'name_results': name_results}

    def verify_results(self, trial: BaseTrial, name, results):
        all_arms_finished = True,
        unfinished_arms = []
        print(f"trial {trial.index} work {name} verify_results: {results}")
        for arm in trial.arms:
            ret = results.get_result(name=None, args=[arm.parameters])
            if ret is None:
                all_arms_finished = False
                unfinished_arms.append(arm)
        return all_arms_finished, unfinished_arms

    def submit_retries(self, trial, name, retries, unfinished_arms):
        print(f"trial {trial.index} work {name} submit retries {retries} for {unfinished_arms}")
        self.transforms[trial.index][retries] = {}

        # one work is one objective
        # with multiple objectives, there will be multiple work objects

        function = self.runner_funcs[name]['function']
        pre_kwargs = self.runner_funcs[name]['pre_kwargs']
        params_list = []
        for arm in unfinished_arms:
            params_list.append([arm.parameters])

        work = work_def(function, workflow=self.workflow, pre_kwargs=pre_kwargs, return_work=True, map_results=True)
        w = work(multi_jobs_kwargs_list=params_list)
        # w.store()
        print("trial %s, retries %s: create a task for %s: %s" % (trial.index, retries, name, w))
        tf_id = w.submit()
        print("trial %s, retries %s: submit a task for %s: %s, %s" % (trial.index, retries, name, w, tf_id))
        if not tf_id:
            raise Exception("Failed to submit work to PanDA")
        self.transforms[trial.index][retries][name] = {'tf_id': tf_id, 'work': w, 'results': None}

    def get_trial_status(self, trial: BaseTrial):
        name_results = trial.run_metadata.get("name_results")
        all_finished, all_failed, all_terminated = True, True, True

        for name in name_results:
            status = name_results[name]['status']
            retries = name_results[name]['retries']
            avail_results = name_results[name]['result']
            if status not in ['finished', 'terminated', 'failed']:
                w = self.transforms[trial.index][retries][name]['work']
                tf_id = self.transforms[trial.index][retries][name]['tf_id']
                w.init_async_result()

                # results = w.get_results()
                # print(results)

                if not w.is_terminated():
                    all_finished, all_failed, all_terminated = False, False, False
                else:
                    results = w.get_results()
                    print(f"trial {trial.index} work {name} retries {retries} results: {results}")
                    self.transforms[trial.index][retries][name]['results'] = results
                    if w.is_finished():
                        print(f"trial {trial.index} work {name} retries {retries} finished")
                        name_results[name]['status'] = 'finished'
                        all_failed = False
                    elif w.is_failed():
                        print(f"trial {trial.index} work {name} retries {retries} failed")
                        name_results[name]['status'] = 'failed'
                        all_finished = False
                    else:
                        print(f"trial {trial.index} work {name} retries {retries} terminated")
                        name_results[name]['status'] = 'terminated'
                        all_finished = False
                    if avail_results is None:
                        name_results[name]['result'] = results
                    else:
                        for arm in trial.arms:
                            ret = avail_results.get_result(name=None, args=[arm.parameters])
                            if ret is None:
                                print(f"trial {trial.index} work {name} missing results for arm {arm.name}")
                                # get results from new retries
                                if results:
                                    new_ret = results.get_result(name=None, args=[arm.parameters])
                                    print(f"trial {trial.index} work {name} checking results for arm {arm.name} from new transform {tf_id}, new results: {new_ret}")
                                    if new_ret is not None:
                                        print(f"trial {trial.index} work {name} set result for arm {arm.name} from new transform {tf_id} to {new_ret}")
                                        # avail_results.set_result(name=None, args=[arm.parameters], value=new_ret)
                                        name_results[name]['result'].set_result(name=None, args=[arm.parameters], value=new_ret)

                    ret, unfinished_arms = self.verify_results(trial, name, name_results[name]['result'])
                    print(f"trial {trial.index} work {name} verify_results: ret: {ret}, unfinished_arms: {unfinished_arms}")
                    if ret:
                        all_finished = False
                    if not ret and retries < 3:
                        retries += 1
                        all_finished, all_failed, all_terminated = False, False, False
                        self.submit_retries(trial, name, retries, unfinished_arms)
                        name_results[name]['retries'] = retries
                        name_results[name]['status'] = 'running'

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
        print(f"trial {trial.index} fetch_trial_data")
        if not isinstance(trial, BaseTrial):
            raise ValueError("This metric only handles `BaseTrial`.")

        try:
            name_results = trial.run_metadata.get("name_results")
            df_dict_list = []
            results_dict = name_results.get(self.name, {})
            results = results_dict.get('result', None)

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
                        print(f"trial {trial.index} {self.name} misses result for {arm.name}")
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
                MetricFetchE(message=f"trial {trial.index} failed to fetch {self.name}", exception=e)
            )
