# ddf--cdiac-co2

data source: http://cdiac.ornl.gov/ftp/ndp030/CSV-FILES/

## Issue

- Negative values found in data. 

## Implementation Notes

From [global ems file](http://cdiac.ornl.gov/ftp/ndp030/global.1751_2013.ems)
and [nation ems file](http://cdiac.ornl.gov/ftp/ndp030/nation.1751_2013.ems),
we know that:

for national data:

> All emission estimates are expressed in thousand metric tons of carbon.

for global data:

> All emission estimates are expressed in million metric tons of carbon.

except for per capita data, it's same in both file:

> Per capita emission estimates are expressed in metric tons of carbon.

Because concepts from nation file and global file are mostly the same but with different
scales, we unify them by multiplying global data by 1000, so they both have same units. 
(Except for per capita data, we don't change the values in both files)

## Note on C and CO2

One ton of carbon equals 44/12 = 11/3 = 3.667 tons of carbon dioxide

## Reference

- [the difference between C and CO_2](http://thinkprogress.org/climate/2008/03/25/202471/the-biggest-source-of-mistakes-c-vs-co2/)
- [CDIAC's data exploration tools](http://cdiac.ornl.gov/CO2_Emission/timeseries/global): 
we know the source data metric is _ton of carbon_ by comparing the source data and the data downloaded from the tool

