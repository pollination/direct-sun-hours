# direct-sun-hours

Direct sun hours recipe.

Calculate the number of hours of direct sunlight received by grids of sensors during
the time period of a specified Wea. The recipe generates 2 sub-folders of results:

1. `direct_sun_hours`: A matrix of zero/one valued indicating whether each sensor
  is exposed to the sun at a given time step of the input Wea.

2. `cumulative`: The cumulative number of Wea time steps that each sensor can see the
  sun. Each value is a single integer for each input sensor.
