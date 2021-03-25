from pollination.direct_sun_hours.entry import DirectSunHoursEntryPoint
from queenbee.recipe.dag import DAG


def test_direct_sun_hours():
    recipe = DirectSunHoursEntryPoint().queenbee
    assert recipe.name == 'direct-sun-hours-entry-point'
    assert isinstance(recipe, DAG)
