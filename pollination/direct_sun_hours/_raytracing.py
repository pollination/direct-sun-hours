"""Raytracing DAG for direct sun hours."""

from pollination_dsl.dag import Inputs, DAG, task
from dataclasses import dataclass

from pollination.honeybee_radiance.grid import SplitGrid, MergeFiles

from ._direct_sunlight_calc import DirectSunHoursCalculation


@dataclass
class DirectSunHoursEntryLoop(DAG):
    # inputs
    sensor_count = Inputs.int(
        default=200,
        description='The maximum number of grid points per parallel execution',
        spec={'type': 'integer', 'minimum': 1}
    )

    octree_file = Inputs.file(
        description='A Radiance octree file with suns.',
        extensions=['oct']
    )

    grid_name = Inputs.str(
        description='Sensor grid file name. This is useful to rename the final result '
        'file to {grid_name}.ill'
    )

    sensor_grid = Inputs.file(
        description='Sensor grid file.',
        extensions=['pts']
    )

    sun_modifiers = Inputs.file(
        description='A file with sun modifiers.'
    )

    @task(template=SplitGrid)
    def split_grid(self, sensor_count=sensor_count, input_grid=sensor_grid):
        return [
            {'from': SplitGrid()._outputs.grids_list},
            {'from': SplitGrid()._outputs.output_folder, 'to': 'sub_grids'}
        ]

    @task(
        template=DirectSunHoursCalculation, needs=[split_grid],
        loop=split_grid._outputs.grids_list,
        sub_paths={'sensor_grid': '{{item.path}}'}
    )
    def direct_sunlight(
        self,
        octree_file=octree_file,
        sensor_count='{{item.count}}',
        grid_name='{{item.name}}',
        sun_modifiers=sun_modifiers,
        sensor_grid=split_grid._outputs.output_folder,
        scene_file=octree_file
            ):
        # the results of the loop will be collected under direct_sunlight subfolder.
        pass

    @task(
        template=MergeFiles, needs=[direct_sunlight]
    )
    def merge_direct_sun_hours(
        self, name=grid_name, extension='.ill', folder='direct-sun-hours'
            ):
        """Merge direct sun hour results from several grids into a single file."""
        return [
            {
                'from': MergeFiles()._outputs.result_file,
                'to': '../../results/direct_sun_hours/{{self.name}}.ill'
            }
        ]

    @task(
        template=MergeFiles, needs=[direct_sunlight]
    )
    def merge_cumulative_sun_hours(
        self, name=grid_name, extension='.res', folder='cumulative-sun-hours'
            ):
        """Merge cumulative sun hour results from several grids into a single file."""
        return [
            {
                'from': MergeFiles()._outputs.result_file,
                'to': '../../results/cumulative/{{self.name}}.res'
            }
        ]
