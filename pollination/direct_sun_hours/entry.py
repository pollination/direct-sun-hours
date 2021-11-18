from pollination_dsl.dag import Inputs, DAG, task, Outputs
from dataclasses import dataclass
from pollination.ladybug.translate import WeaToConstant
from pollination.honeybee_radiance.sun import CreateSunMatrix, ParseSunUpHours
from pollination.honeybee_radiance.translate import CreateRadianceFolderGrid
from pollination.honeybee_radiance.octree import CreateOctreeWithSky
from pollination.honeybee_radiance.grid import SplitGridFolder, MergeFolderData
from pollination.path.copy import Copy
from pollination.path.write import WriteInt

# input/output alias
from pollination.alias.inputs.model import hbjson_model_grid_input
from pollination.alias.inputs.wea import wea_input
from pollination.alias.inputs.north import north_input
from pollination.alias.inputs.grid import grid_filter_input, \
    min_sensor_count_input, cpu_count
from pollination.alias.outputs.daylight import direct_sun_hours_results, \
    cumulative_sun_hour_results

from ._direct_sunlight_calc import DirectSunHoursCalculation


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
        'cpu_count is always respected.', default=1,
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
        extensions=['json', 'hbjson'],
        alias=hbjson_model_grid_input
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

    @task(template=ParseSunUpHours, needs=[generate_sunpath])
    def parse_sun_up_hours(self, sun_modifiers=generate_sunpath._outputs.sun_modifiers):
        return [
            {
                'from': ParseSunUpHours()._outputs.sun_up_hours,
                'to': 'results/direct_sun_hours/sun-up-hours.txt'
            }
        ]

    @task(template=WriteInt)
    def write_timestep(self, src=timestep):
        return [
            {
                'from': WriteInt()._outputs.dst,
                'to': 'results/direct_sun_hours/timestep.txt'
            }
        ]

    @task(template=CreateRadianceFolderGrid)
    def create_rad_folder(self, input_model=model, grid_filter=grid_filter):
        """Translate the input model to a radiance folder."""
        return [
            {
                'from': CreateRadianceFolderGrid()._outputs.model_folder,
                'to': 'model'
            },
            {
                'from': CreateRadianceFolderGrid()._outputs.bsdf_folder,
                'to': 'model/bsdf'
            },
            {
                'from': CreateRadianceFolderGrid()._outputs.sensor_grids_file,
                'to': 'results/direct_sun_hours/grids_info.json'
            },
            {
                'from': CreateRadianceFolderGrid()._outputs.sensor_grids,
                'description': 'Sensor grids information.'
            }
        ]

    @task(template=Copy, needs=[create_rad_folder])
    def copy_grid_info(self, src=create_rad_folder._outputs.sensor_grids_file):
        return [
            {
                'from': Copy()._outputs.dst,
                'to': 'results/cumulative/grids_info.json'
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

    @task(
        template=SplitGridFolder, needs=[create_rad_folder],
        sub_paths={'input_folder': 'grid'}
    )
    def split_grid_folder(
        self, input_folder=create_rad_folder._outputs.model_folder,
        cpu_count=cpu_count, cpus_per_grid=1, min_sensor_count=min_sensor_count
    ):
        """Split sensor grid folder based on the number of CPUs"""
        return [
            {
                'from': SplitGridFolder()._outputs.output_folder,
                'to': 'resources/grid'
            },
            {
                'from': SplitGridFolder()._outputs.dist_info,
                'to': 'initial_results/direct_sun_hours/_redist_info.json'
            },
            {
                'from': SplitGridFolder()._outputs.sensor_grids,
                'description': 'Sensor grids information.'
            }
        ]

    @task(template=Copy, needs=[split_grid_folder])
    def copy_redist_info(self, src=split_grid_folder._outputs.dist_info):
        return [
            {
                'from': Copy()._outputs.dst,
                'to': 'initial_results/cumulative/_redist_info.json'
            }
        ]

    @task(
        template=DirectSunHoursCalculation,
        needs=[create_octree, generate_sunpath, create_rad_folder, split_grid_folder],
        loop=split_grid_folder._outputs.sensor_grids,
        sub_folder='initial_results/{{item.full_id}}',  # create a subfolder for each grid
        sub_paths={'sensor_grid': '{{item.full_id}}.pts'}  # sensor_grid sub_path
    )
    def direct_sun_hours_raytracing(
        self,
        timestep=timestep,
        sensor_count='{{item.count}}',
        octree_file=create_octree._outputs.scene_file,
        grid_name='{{item.full_id}}',
        sensor_grid=split_grid_folder._outputs.output_folder,
        sunpath=generate_sunpath._outputs.sunpath,
        sun_modifiers=generate_sunpath._outputs.sun_modifiers,
        bsdfs=create_rad_folder._outputs.bsdf_folder
    ):
        pass

    @task(
        template=MergeFolderData,
        needs=[direct_sun_hours_raytracing]
    )
    def restructure_timestep_results(
        self, input_folder='initial_results/direct_sun_hours',
        extension='ill'
    ):
        return [
            {
                'from': MergeFolderData()._outputs.output_folder,
                'to': 'results/direct_sun_hours'
            }
        ]

    @task(
        template=MergeFolderData,
        needs=[direct_sun_hours_raytracing]
    )
    def restructure_cumulative_results(
        self, input_folder='initial_results/cumulative',
        extension='res'
    ):
        return [
            {
                'from': MergeFolderData()._outputs.output_folder,
                'to': 'results/cumulative'
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
