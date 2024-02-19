from pollination_dsl.dag import Inputs, GroupedDAG, task, Outputs
from dataclasses import dataclass
from pollination.honeybee_radiance.grid import MergeFolderData
from pollination.honeybee_radiance_postprocess.grid import MergeFolderData as MergeFolderDataPostprocess
from pollination.path.copy import CopyFile, CopyFileMultiple


@dataclass
class DirectSunHoursPostprocess(GroupedDAG):
    """Post-process for direct sun hours."""

    # inputs
    input_folder = Inputs.folder(
        description='Folder with initial results before redistributing the '
        'results to the original grids.'
    )

    grids_info = Inputs.file(
        description='Grids information from the original model.'
    )

    sun_up_hours = Inputs.file(
        description='Sun up hours up file.'
    )

    timestep = Inputs.file(
        description='Timestep file.'
    )

    dist_info = Inputs.file(
        description='Distribution information file.',
    )

    @task(template=CopyFile)
    def copy_timestep(self, src=timestep):
        return [
            {
                'from': CopyFile()._outputs.dst,
                'to': 'results/direct_sun_hours/timestep.txt'
            }
        ]

    @task(template=CopyFileMultiple)
    def copy_sun_up_hours(self, src=sun_up_hours):
        return [
            {
                'from': CopyFileMultiple()._outputs.dst_1,
                'to': 'results/direct_sun_hours/sun-up-hours.txt'
            },
            {
                'from': CopyFileMultiple()._outputs.dst_2,
                'to': 'results/cumulative/sun-up-hours.txt'
            }
        ]

    @task(template=CopyFileMultiple)
    def copy_grid_info(self, src=grids_info):
        return [
            {
                'from': CopyFileMultiple()._outputs.dst_1,
                'to': 'results/direct_sun_hours/grids_info.json'
            },
            {
                'from': CopyFileMultiple()._outputs.dst_2,
                'to': 'results/cumulative/grids_info.json'
            }
        ]

    @task(
        template=MergeFolderData, needs=[copy_sun_up_hours, copy_grid_info]
    )
    def restructure_cumulative_results(
        self, input_folder=input_folder,
        extension='res'
    ):
        return [
            {
                'from': MergeFolderData()._outputs.output_folder,
                'to': 'results/cumulative'
            }
        ]

    results = Outputs.folder(
        source='results', description='results folder.'
    )
