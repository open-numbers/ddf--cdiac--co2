# ddf--cdiac-co2

data source: http://cdiac.ornl.gov/ftp/ndp030/CSV-FILES/

## Issue

For now, we made the concepts extracted from nation file and global file independent.
In fact, some of the concepts, for example `Total carbon emissions from fossil-fuels (million metric tons of C)` 
from global file and `Total CO2 emissions from fossil-fuels (thousand metric tons of C)`
from nation file are same concept but with a different scale.

Note: One ton of carbon equals 44/12 = 11/3 = 3.67 tons of carbon dioxide

We keep it as it is for now. We will probably come back to solve this nicer, harmonize it.

## Reference

- [the difference between C and CO_2](http://thinkprogress.org/climate/2008/03/25/202471/the-biggest-source-of-mistakes-c-vs-co2/)
- [CDIAC's data exploration tools](http://cdiac.ornl.gov/CO2_Emission/timeseries/global): 
we know the source data metric is _ton of carbon_ by comparing the source data and the data downloaded from the tool

