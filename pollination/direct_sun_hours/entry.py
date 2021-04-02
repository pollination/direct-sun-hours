from pollination_dsl.dag import Inputs, DAG, task, Outputs
from dataclasses import dataclass
from pollination.ladybug.translate import WeaToConstant
from pollination.honeybee_radiance.sun import CreateSunMatrix, ParseSunUpHours
from pollination.honeybee_radiance.translate import CreateRadianceFolderGrid
from pollination.honeybee_radiance.octree import CreateOctreeWithSky
from pollination.path.copy import CopyMultiple

# input/output alias
from pollination.alias.inputs.model import hbjson_model_input
from pollination.alias.inputs.wea import wea_input
from pollination.alias.inputs.north import north_input
from pollination.alias.inputs.grid import sensor_count_input, grid_filter_input
from pollination.alias.outputs.daylight import direct_sun_hours_results, \
    cumulative_sun_hour_results, direct_radiation_results

from ._raytracing import DirectSunHoursEntryLoop


@dataclass
class DirectSunHoursEntryPoint(DAG):
    """Direct sun hours entry point."""

    # inputs
    north = Inputs.float(
        default=0,
        description='A number for rotation from north.',
        spec={'type': 'number', 'minimum': 0, 'maximum': 360},
        alias=north_input
    )

    sensor_count = Inputs.int(
        default=200,
        description='The maximum number of grid points per parallel execution.',
        spec={'type': 'integer', 'minimum': 1},
        alias=sensor_count_input
    )

    grid_filter = Inputs.str(
        description='Text for a grid identifer or a pattern to filter the sensor grids '
        'of the model that are simulated. For instance, first_floor_* will simulate '
        'only the sensor grids that have an identifier that starts with '
        'first_floor_. By default, all grids in the model will be simulated.',
        default='*',
        alias=grid_filter_input
    )

    model = Inputs.file(
        description='A Honeybee model in HBJSON file format.',
        extensions=['json', 'hbjson'],
        alias=hbjson_model_input
    )

    wea = Inputs.file(
        description='Wea file.',
        extensions=['wea'],
        alias=wea_input
    )

    @task(template=WeaToConstant)
    def convert_wea_to_constant(self, wea=wea):
        """Convert a wea to have constant irradiance values."""
        return [
            {
                'from': WeaToConstant()._outputs.constant_wea,
                'to': 'resources/constant.wea'
            }
        ]

    @task(template=CreateSunMatrix, needs=[convert_wea_to_constant])
    def generate_sunpath(
            self, wea=convert_wea_to_constant._outputs.constant_wea,
            north=north, output_type=1
        ):
        """Create sunpath for sun-up-hours."""
        return [
            {'from': CreateSunMatrix()._outputs.sunpath, 'to': 'resources/sunpath.mtx'},
            {
                'from': CreateSunMatrix()._outputs.sun_modifiers,
                'to': 'resources/suns.mod'
            }
        ]

    @task(template=CreateRadianceFolderGrid)
    def create_rad_folder(self, input_model=model, grid_filter=grid_filter):
        """Translate the input model to a radiance folder."""
        return [
            {'from': CreateRadianceFolderGrid()._outputs.model_folder, 'to': 'model'},
            {
                'from': CreateRadianceFolderGrid()._outputs.sensor_grids_file,
                'to': 'results/direct_sun_hours/grids_info.json'
            },
            {
                'from': CreateRadianceFolderGrid()._outputs.sensor_grids,
                'description': 'Sensor grids information.'
            }
        ]

    @task(template=CopyMultiple, needs=[create_rad_folder])
    def copy_grid_info(self, src=create_rad_folder._outputs.sensor_grids_file):
        return [
            {
                'from': CopyMultiple()._outputs.dst_1,
                'to': 'results/cumulative/grids_info.json'
            },
            {
                'from': CopyMultiple()._outputs.dst_2,
                'to': 'results/direct_radiation/grids_info.json'
            }
        ]

    @task(
        template=CreateOctreeWithSky, needs=[generate_sunpath, create_rad_folder]
    )
    def create_octree(
        self, model=create_rad_folder._outputs.model_folder,
        sky=generate_sunpath._outputs.sunpath
    ):
        """Create octree from radiance folder and sunpath for direct studies."""
        return [
            {
                'from': CreateOctreeWithSky()._outputs.scene_file,
                'to': 'resources/scene_with_suns.oct'
            }
        ]

    @task(template=ParseSunUpHours, needs=[generate_sunpath])
    def parse_sun_up_hours(self, sun_modifiers=generate_sunpath._outputs.sun_modifiers):
        return [
            {
                'from': ParseSunUpHours()._outputs.sun_up_hours,
                'to': 'results/direct_sun_hours/sun-up-hours.txt'
            }
        ]

    @task(
        template=DirectSunHoursEntryLoop,
        needs=[create_octree, generate_sunpath, create_rad_folder],
        loop=create_rad_folder._outputs.sensor_grids,
        sub_folder='initial_results/{{item.name}}',  # create a subfolder for each grid
        sub_paths={'sensor_grid': 'grid/{{item.full_id}}.pts'}  # sub_path for sensor_grid arg
    )
    def direct_sun_hours_raytracing(
        self,
        sensor_count=sensor_count,
        octree_file=create_octree._outputs.scene_file,
        grid_name='{{item.full_id}}',
        sensor_grid=create_rad_folder._outputs.model_folder,
        sunpath=generate_sunpath._outputs.sunpath,
        sun_modifiers=generate_sunpath._outputs.sun_modifiers
    ):
        pass

    direct_sun_hours = Outputs.folder(
        source='results/direct_sun_hours',
        description='Hourly results for direct sun hours.',
        alias=direct_sun_hours_results
    )

    cumulative_sun_hours = Outputs.folder(
        source='results/cumulative',
        description='Cumulative results for direct sun hours for all the input hours.',
        alias=cumulative_sun_hour_results
    )
