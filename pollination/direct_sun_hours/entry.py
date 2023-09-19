from pollination_dsl.dag import Inputs, DAG, task, Outputs
from dataclasses import dataclass

from pollination.ladybug_radiance.direct_sunhours import DirectSunHours


@dataclass
class DirectSunHoursEntryPoint(DAG):
    """Direct sun hours entry point."""

    # inputs
    vectors = Inputs.file(
        description='A file that includes sun vectors.'
    )

    offset_dist = Inputs.float(
        description='Number in model units for the distance to move points from '
        'the surfaces of the input geometry.', default=0.01,
        spec={'type': 'number', 'maximum': 1.0, 'minimum': 0.001}
    )

    timestep = Inputs.int(
        description='A number for vectors timesteps', default=1,
        spec={'type': 'number', 'maximum': 60, 'minimum': 1}
    )

    study_mesh = Inputs.file(
        description='Path to a JSON file for input study mesh in Ladybug Geometry '
        'format.'
    )

    context_mesh = Inputs.file(
        description='Path to a JSON file for input context mesh in Ladybug Geometry '
        'format.', optional=True
    )

    @task(template=DirectSunHours)
    def run_direct_sun_hours(
        self, vectors=vectors, offset_dist=offset_dist, timestep=timestep,
        study_mesh=study_mesh, context_mesh=context_mesh
    ):
        return [
            {
                'from': DirectSunHours()._outputs.direct_sunlight_values,
                'to': 'direct_sun_hours.json'
            },
            {
                'from': DirectSunHours()._outputs.visualization_set,
                'to': 'visualization/viz.vsf'
            }
        ]

    direct_sun_hours = Outputs.file(
        source='direct_sun_hours.json',
        description='Hourly results for direct sun hours.'
    )

    visualization_set = Outputs.file(
        source='visualization/viz.vsf',
        description='Direct sun hours visualization.',
    )
