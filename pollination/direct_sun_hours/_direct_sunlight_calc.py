from pollination_dsl.dag import Inputs, DAG, task
from dataclasses import dataclass

from pollination.honeybee_radiance_postprocess.post_process import DirectSunHours
from pollination.honeybee_radiance.contrib import DaylightContribution


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
        fixed_radiance_parameters='-aa 0.0 -I -faf -ab 0 -dc 1.0 -dt 0.0 -dj 0.0 -dr 0',
        conversion='0.265 0.670 0.065',
        sensor_count=sensor_count,
        modifiers=sun_modifiers,
        sensor_grid=sensor_grid,
        grid_name=grid_name,
        scene_file=octree_file,
        output_format='f',
        bsdf_folder=bsdfs
    ):
        return [
            {
                'from': DaylightContribution()._outputs.result_file,
                'to': '{{self.grid_name}}.ill'
            }
        ]

    @task(
        template=DirectSunHours, needs=[direct_irradiance_calculation]
    )
    def calculate_direct_sun_hours(
        self, input_mtx=direct_irradiance_calculation._outputs.result_file,
        divisor=timestep, grid_name=grid_name
    ):
        return [
            {
                'from': DirectSunHours()._outputs.direct_sun_hours,
                'to': '../direct_sun_hours/{{self.grid_name}}.ill'
            },
            {
                'from': DirectSunHours()._outputs.cumulative_direct_sun_hours,
                'to': '../cumulative/{{self.grid_name}}.res'
            }
        ]
