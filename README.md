# direct-sun-hours

Direct sun hours for Pollination

Calculate the number of hours of direct sunlight received by grids of sensors during the
year. The recipe generates 3 subfolders under results folder:

1. `direct_sun_hours`: Hourly results for number of hours that each sensor is exposed
  to sun. The units are the timestep of input wea file. For an hourly wea each value
  corresponds to an hour of direct sunlight.

2. `cumulative`: The cumulative number of hours for each sensor during all the timesteps.
  The value is a single integer for each input sensor.

3. `direct_radiation`: Hourly direct radiation from sun. This output does not include the
  indirect sky radiation. For total radiation see `annual radiation` or
`annual sky radiation` recipes.

# Limitations

This recipe is limited to calculating up to `9999` number of suns at a time. This means
you might need to break down the annual studies with smaller timesteps into smaller
runs. This limitation is a result of how Radiance sets a limit on number of modifiers
in a single run. [See here](https://discourse.radiance-online.org/t/increase-maximum-number-of-modifiers-in-rcontrib/4684). We can address this limitation by either building our own version of Radiance
or breaking down the modifier list and looping over them and merging back the results.
