from pollination_dsl.dag import Inputs, DAG, task
from dataclasses import dataclass

from pollination.honeybee_radiance.post_process import ConvertToBinary, SumRow
from pollination.honeybee_radiance.contrib import DaylightContribution


@dataclass
class DirectSunHoursCalculation(DAG):

    sun_modifiers = Inputs.file(
        description='A file with sun modifiers.'
    )

    sensor_grid = Inputs.file(
        description='Sensor grid file.',
        extensions=['pts']
    )

    octree_file = Inputs.file(
        description='A Radiance octree file with suns.',
        extensions=['oct']
    )

    sensor_count = Inputs.int(
        default=200,
        description='The maximum number of grid points per parallel execution',
        spec={'type': 'integer', 'minimum': 1}
    )

    grid_name = Inputs.str(
        description='Sensor grid file name. This is useful to rename the final result '
        'file to {grid_name}.ill'
    )

    @task(template=DaylightContribution, sub_folder='direct-radiation')
    def direct_radiation_calculation(
        self,
        fixed_radiance_parameters='-aa 0.0 -I -faa -ab 0 -dc 1.0 -dt 0.0 -dj 0.0 -dr 0',
        conversion='0.265 0.670 0.065',
        sensor_count=sensor_count,
        modifiers=sun_modifiers,
        sensor_grid=sensor_grid,
        grid_name=grid_name,
        scene_file=octree_file
            ):
        return [
            {
                'from': DaylightContribution()._outputs.result_file,
                'to': '{{self.grid_name}}.ill'
            }
        ]

    @task(
        template=ConvertToBinary, needs=[direct_radiation_calculation],
        sub_folder='direct-sun-hours'
    )
    def convert_to_sun_hours(
        self, input_mtx=direct_radiation_calculation._outputs.result_file,
        grid_name=grid_name, minimum=0, include_min='exclude'
    ):
        return [
            {
                'from': ConvertToBinary()._outputs.output_mtx,
                'to': '{{self.grid_name}}.ill'
            }
        ]

    @task(
        template=SumRow, needs=[convert_to_sun_hours],
        sub_folder='cumulative-sun-hours'
    )
    def calculate_cumulative_hours(
        self, input_mtx=convert_to_sun_hours._outputs.output_mtx, grid_name=grid_name
    ):
        return [
            {
                'from': SumRow()._outputs.output_mtx,
                'to': '{{self.grid_name}}.res'
            }
        ]
