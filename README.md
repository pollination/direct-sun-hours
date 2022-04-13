# direct-sun-hours

Direct sun hours recipe calculates the number of hours of direct sun received by grids of sensors during
the time period of a specified Wea. The recipe generates 2 sub-folders of results:

1. `direct_sun_hours`: Contains matrices of zero/one values indicating whether
  each sensor is exposed to the sun at a given time step of the input Wea.

2. `cumulative`: The cumulative number of hours that each sensor can see the
  sun. Each value is always in hours.

Using the -viz version of this recipe will produce a VTKjs file for visualizing
the cumulative results.
