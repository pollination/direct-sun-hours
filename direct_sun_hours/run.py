import sys
import luigi
import os
import time
from multiprocessing import freeze_support
from queenbee_local import local_scheduler, _copy_artifacts, update_params, parse_input_args

import flow.main as direct_sun_hours_workerbee


_recipe_default_inputs = {'model': None, 'north': 0.0, 'sensor_count': 200, 'wea': None}


class LetDirectSunHoursFly(luigi.WrapperTask):
    # global parameters
    _input_params = luigi.DictParameter()

    def requires(self):
        yield [direct_sun_hours_workerbee._MainOrchestrator(_input_params=self._input_params)]


def start(project_folder, user_values, workers):
    freeze_support()

    input_params = update_params(_recipe_default_inputs, user_values)

    if 'simulation_folder' not in input_params or not input_params['simulation_folder']:
        if 'simulation_id' not in input_params or not input_params['simulation_id']:
            simulation_id = 'direct_sun_hours_%d' % int(round(time.time(), 2) * 100)
        else:
            simulation_id = input_params['simulation_id']

        simulation_folder = os.path.join(project_folder, simulation_id)
        input_params['simulation_folder'] = simulation_folder
    else:
        simulation_folder = input_params['simulation_folder']

    # copy project folder content to simulation folder
    artifacts = ['model', 'wea']
    for artifact in artifacts:
        from_ = os.path.join(project_folder, input_params[artifact])
        to_ = os.path.join(simulation_folder, input_params[artifact])
        _copy_artifacts(from_, to_)

    luigi.build(
        [LetDirectSunHoursFly(_input_params=input_params)],
        local_scheduler=local_scheduler(),
        workers=workers
    )



if __name__ == '__main__':
    project_folder, user_values, workers = parse_input_args(sys.argv)
    start(project_folder, user_values, workers)
