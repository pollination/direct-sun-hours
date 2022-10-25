from pollination_dsl.dag import Inputs, DAG, task
from dataclasses import dataclass

from pollination.honeybee_radiance.post_process import ConvertToBinary, SumRow
from pollination.honeybee_radiance.contrib import DaylightContribution
from pollination.path.copy import Copy


@dataclass
class DirectSunHoursCalculation(DAG):

    timestep = Inputs.int(
        description='Input wea timestep. This value will be used to divide the '
        'cumulative results to ensure the units of the output are in hours.', default=1,
        spec={'type': 'integer', 'minimum': 1, 'maximum': 60}
    )

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
        description='Number of sensors in the input sensor grid.'
    )

    grid_name = Inputs.str(
        description='Sensor grid file name. This is useful to rename the final result '
        'file to {grid_name}.ill'
    )

    bsdfs = Inputs.folder(
        description='Folder containing any BSDF files needed for ray tracing.',
        optional=True
    )

    @task(template=DaylightContribution)
    def direct_irradiance_calculation(
        self,
        fixed_radiance_parameters='-aa 0.0 -I -faa -ab 0 -dc 1.0 -dt 0.0 -dj 0.0 -dr 0',
        conversion='0.265 0.670 0.065',
        sensor_count=sensor_count,
        modifiers=sun_modifiers,
        sensor_grid=sensor_grid,
        grid_name=grid_name,
        scene_file=octree_file,
        bsdf_folder=bsdfs
    ):
        return [
            {
                'from': DaylightContribution()._outputs.result_file,
                'to': '{{self.grid_name}}.ill'
            }
        ]

    @task(
        template=ConvertToBinary, needs=[direct_irradiance_calculation]
    )
    def convert_to_sun_hours(
        self, input_mtx=direct_irradiance_calculation._outputs.result_file,
        grid_name=grid_name, minimum=0, include_min='exclude'
    ):
        return [
            {
                'from': ConvertToBinary()._outputs.output_mtx,
                'to': '{{self.grid_name}}_sun_hours.ill'
            }
        ]

    @task(template=Copy, needs=[convert_to_sun_hours])
    def copy_sun_hours(
            self, grid_name=grid_name, src=convert_to_sun_hours._outputs.output_mtx):
        return [
            {
                'from': Copy()._outputs.dst,
                'to': '../direct_sun_hours/{{self.grid_name}}.ill'
            }
        ]

    @task(
        template=SumRow, needs=[convert_to_sun_hours],
    )
    def calculate_cumulative_hours(
        self, input_mtx=convert_to_sun_hours._outputs.output_mtx,
        grid_name=grid_name, divisor=timestep
    ):
        return [
            {
                'from': SumRow()._outputs.output_mtx,
                'to': '../cumulative/{{self.grid_name}}.res'
            }
        ]
