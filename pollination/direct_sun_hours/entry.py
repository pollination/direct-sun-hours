from pollination_dsl.dag import Inputs, DAG, task, Outputs
from dataclasses import dataclass

# input/output alias
from pollination.alias.inputs.model import hbjson_model_grid_input
from pollination.alias.inputs.wea import wea_input
from pollination.alias.inputs.north import north_input
from pollination.alias.inputs.grid import grid_filter_input, \
    min_sensor_count_input, cpu_count
from pollination.alias.outputs.daylight import direct_sun_hours_results, \
    cumulative_sun_hour_results

from ._prepare_folder import DirectSunHoursPrepareFolder
from ._direct_sunlight_calc import DirectSunHoursCalculation
from ._postprocess import DirectSunHoursPostprocess


@dataclass
class DirectSunHoursEntryPoint(DAG):
    """Direct sun hours entry point."""

    # inputs
    timestep = Inputs.int(
        description='Input wea timestep. This value will be used to divide the '
        'cumulative results to ensure the units of the output are in hours.', default=1,
        spec={'type': 'integer', 'minimum': 1, 'maximum': 60}
    )

    north = Inputs.float(
        default=0,
        description='A number for rotation from north.',
        spec={'type': 'number', 'minimum': 0, 'maximum': 360},
        alias=north_input
    )

    cpu_count = Inputs.int(
        default=50,
        description='The maximum number of CPUs for parallel execution. This will be '
        'used to determine the number of sensors run by each worker.',
        spec={'type': 'integer', 'minimum': 1},
        alias=cpu_count
    )

    min_sensor_count = Inputs.int(
        description='The minimum number of sensors in each sensor grid after '
        'redistributing the sensors based on cpu_count. This value takes '
        'precedence over the cpu_count and can be used to ensure that '
        'the parallelization does not result in generating unnecessarily small '
        'sensor grids. The default value is set to 1, which means that the '
        'cpu_count is always respected.', default=500,
        spec={'type': 'integer', 'minimum': 1},
        alias=min_sensor_count_input
    )

    grid_filter = Inputs.str(
        description='Text for a grid identifier or a pattern to filter the sensor grids '
        'of the model that are simulated. For instance, first_floor_* will simulate '
        'only the sensor grids that have an identifier that starts with '
        'first_floor_. By default, all grids in the model will be simulated.',
        default='*',
        alias=grid_filter_input
    )

    model = Inputs.file(
        description='A Honeybee model in HBJSON file format.',
        extensions=['json', 'hbjson', 'pkl', 'hbpkl', 'zip'],
        alias=hbjson_model_grid_input
    )

    wea = Inputs.file(
        description='Wea file.',
        extensions=['wea'],
        alias=wea_input
    )

    @task(template=DirectSunHoursPrepareFolder)
    def prepare_folder_direct_sun_hours(
        self, timestep=timestep, north=north,
        cpu_count=cpu_count, min_sensor_count=min_sensor_count,
        grid_filter=grid_filter, model=model, wea=wea
    ):
        return [
            {
                'from': DirectSunHoursPrepareFolder()._outputs.model_folder,
                'to': 'model'
            },
            {
                'from': DirectSunHoursPrepareFolder()._outputs.resources,
                'to': 'resources'
            },
            {
                'from': DirectSunHoursPrepareFolder()._outputs.initial_results,
                'to': 'initial_results'
            },
            {
                'from': DirectSunHoursPrepareFolder()._outputs.sensor_grids
            }
        ]

    @task(
        template=DirectSunHoursCalculation,
        needs=[prepare_folder_direct_sun_hours],
        loop=prepare_folder_direct_sun_hours._outputs.sensor_grids,
        sub_folder='initial_results/{{item.full_id}}',  # subfolder for each grid
        sub_paths={
            'octree_file': 'scene_with_suns.oct',
            'sensor_grid': 'grid/{{item.full_id}}.pts',
            'sun_modifiers': 'suns.mod',
            'bsdfs': 'bsdf'
            }
    )
    def direct_sun_hours_raytracing(
        self,
        timestep=timestep,
        sensor_count='{{item.count}}',
        octree_file=prepare_folder_direct_sun_hours._outputs.resources,
        grid_name='{{item.full_id}}',
        sensor_grid=prepare_folder_direct_sun_hours._outputs.resources,
        sun_modifiers=prepare_folder_direct_sun_hours._outputs.resources,
        bsdfs=prepare_folder_direct_sun_hours._outputs.model_folder
    ):
        pass

    @task(
        template=DirectSunHoursPostprocess,
        needs=[prepare_folder_direct_sun_hours, direct_sun_hours_raytracing],
        sub_paths={
            'grids_info': 'grids_info.json',
            'sun_up_hours': 'sun-up-hours.txt',
            'timestep': 'timestep.txt'
            }
    )
    def postprocess_direct_sun_hours(
        self, input_folder=prepare_folder_direct_sun_hours._outputs.initial_results,
        grids_info=prepare_folder_direct_sun_hours._outputs.resources,
        sun_up_hours=prepare_folder_direct_sun_hours._outputs.resources,
        timestep=prepare_folder_direct_sun_hours._outputs.resources,
        wea=wea,
    ):
        return [
            {
                'from': DirectSunHoursPostprocess()._outputs.results,
                'to': 'results'
            }
        ]

    direct_sun_hours = Outputs.folder(
        source='results/direct_sun_hours',
        description='Hourly results for direct sun hours.',
        alias=direct_sun_hours_results
    )

    cumulative_sun_hours = Outputs.folder(
        source='results/cumulative',
        description='Cumulative direct sun hours for all the input hours.',
        alias=cumulative_sun_hour_results
    )
