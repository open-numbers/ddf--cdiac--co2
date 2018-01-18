# -*- coding: utf-8 -*-
"""transform the CDIAC CO2 data set to DDF model"""

import os
import sys

import numpy as np
import pandas as pd
import requests

from datetime import datetime
from io import BytesIO

from ddf_utils.datapackage import dump_json, get_datapackage
from ddf_utils.io import cleanup
from ddf_utils.str import format_float_digits, to_concept_id


# configuration of file path
nation_file = '../source/nation.csv'
global_file = '../source/global.csv'
out_dir = '../../'
source_url_tmpl = 'http://cdiac.ess-dive.lbl.gov/ftp/ndp030/CSV-FILES/{group}.1751_{year}.csv'
last_update = 2014

def update_source():
    current_year = datetime.now().year

    nation_fp = None
    global_fp = None

    for y in range(current_year, last_update, -1):
        res = get_file_for('nation', y)
        if res is not None:
            nation_fp = res
    for y in range(current_year, last_update, -1):
        res = get_file_for('global', y)
        if res is not None:
            global_fp = res

    if nation_fp is None or global_fp is None:
        print('no new source files! If you believe there are new source files, '
              'please check the etl script. Source files can be fond at:'
              'http://cdiac.ess-dive.lbl.gov/ftp/ndp030/CSV-FILES/')
        return False
    else:
        with open(nation_file, 'wb') as f:
            f.write(nation_fp.read())
            f.close()

        with open(global_file, 'wb') as f:
            f.write(global_fp.read())
            f.close
        return True

def get_file_for(group, year):
    url = source_url_tmpl.format(group=group, year=year)
    r = requests.get(url)
    if r.status_code == 200:
        return BytesIO(r.content)
    else:
        return None


def read_source(f, skip=0, **kwargs):
    df = pd.read_csv(f, **kwargs)
    # quick fix for malformed csv downloaded from data povider
    if df.columns[0] == 'Year"':
        df = df.rename(columns={'Year"': 'Year'})
    df.columns = list(map(lambda x: x.lower().replace('\n', ''), df.columns))
    df = df.iloc[skip:]  # skip first few rows of data

    return df


def get_concept_id(name):
    """return concept name for given indicator name.
    """
    if 'total ' in name.lower():
        return 'total_carbon_emissions'
    else:
        subtypes = [
            'gas fuel consumption', 'liquid fuel consumption', 'solid fuel consumption',
            'cement production', 'gas flaring', 'bunker fuels', 'per capita'
        ]
        for i in subtypes:
            if i in name.lower():
                return 'carbon_emissions_'+to_concept_id(i)
        # if nothing found, it should be a non measure concept.
        return to_concept_id(name)


def get_concept_name(concept):
    if concept.startswith('carbon_emissions'):
        n0 = 'Carbon Emissions'
        n1 = concept.replace('carbon_emissions_', '').replace('_', ' ').title()
        return n0 + ' From ' + n1
    else:
        return concept.replace('_', ' ').title()


if __name__ == '__main__':

    # updating source files
    print("update source files..")
    updated = update_source()
    if not updated:
        print('no update, end the process.')
        sys.exit(0)

    # cleanup the output dir
    print('clear up the old files..')
    cleanup(out_dir)

    # generate the dataset
    print("generating dataset...")

    # read source data
    nation_data = read_source(nation_file, skip=3, na_values='.')
    global_data = read_source(global_file, skip=1, na_values='.')

    # fix year to int
    nation_data.year = nation_data.year.map(int)
    global_data.year = global_data.year.map(int)

    # fix nation name for hkg and mac. There is a typo in it.
    nation_data['nation'] = nation_data['nation'].map(
            lambda x: x.replace('ADMINSTRATIVE', 'ADMINISTRATIVE') if 'ADMINSTRATIVE' in x else x)

    # Concept Table
    concept_discrete = ['year', 'nation', 'global', 'name', 'unit', 'description']
    concept_all = np.r_[concept_discrete, list(map(get_concept_id, global_data.columns)),
                        list(map(get_concept_id, nation_data.columns))]
    concept_all = list(set(concept_all))

    cdf = pd.DataFrame(concept_all, columns=['concept'])
    cdf['name'] = cdf.concept.map(get_concept_name)

    # filling concept_type and unit
    cdf['concept_type'] = cdf.concept.map(lambda x: 'measure' if 'carbon' in x else 'string')
    cdf['unit'] = cdf.concept.map(lambda x: 'thousand metric tons' if 'carbon' in x else np.nan)

    # manually set some properties
    cdf = cdf.set_index('concept')
    cdf.loc[['global', 'nation'], 'concept_type'] = 'entity_domain'

    cdf.loc['total_carbon_emissions', 'description'] = \
        'Sum of fossil fuel consumption, cement production and gas flaring emissions'
    cdf.loc['carbon_emissions_per_capita', 'unit'] = 'metric tonnes per person'
    cdf.loc['year', 'concept_type'] = 'time'

    cdf = cdf.reset_index()
    cdf = cdf.sort_values(by=['concept_type', 'concept'])

    cdf.to_csv(os.path.join(out_dir, 'ddf--concepts.csv'), index=False)

    # Entities Tables
    nations_df = pd.DataFrame([nation_data.nation.map(to_concept_id).unique(),
                               nation_data.nation.unique()])

    nations_df = nations_df.T
    nations_df.columns = ['nation', 'name']

    nations_df.to_csv(os.path.join(out_dir, 'ddf--entities--nation.csv'), index=False)

    global_ent = pd.DataFrame([['world', 'World']], columns=['global', 'name'])
    global_ent.to_csv(os.path.join(out_dir, 'ddf--entities--global.csv'), index=False)

    # Datapoint Tables
    nation_data.columns = list(map(get_concept_id, nation_data.columns))
    nation_data.nation = nation_data.nation.map(to_concept_id)

    ndf = nation_data.set_index(['nation', 'year']).copy()

    for col in ndf:
        # ndf[col] = ndf[col].map(format_float_digits)
        (ndf[col]
         .dropna()
         .to_csv(os.path.join(out_dir,
                              'ddf--datapoints--{}--by--nation--year.csv'.format(col)),
                 header=True))

    global_data.columns = list(map(get_concept_id, global_data.columns))
    global_data['global'] = 'world'
    global_data.year = global_data.year.map(int)

    # global data is expressed in million tonnes, so we need to multiply them to make
    # them same uint as nation data
    gdf = global_data.set_index(['global', 'year']).copy()

    for col in gdf:
        ser = gdf[col].map(float)  # some columns not reconized as float. fix those here.
        if 'per_capita' in col:  # don't change per capita data
            (ser
             .dropna()
             .to_csv(
                 os.path.join(out_dir,
                              'ddf--datapoints--{}--by--global--year.csv'.format(col)),
                 header=True))
        else:  # multiply 1000
            ((ser*1000)
             .dropna()
             .to_csv(
                 os.path.join(out_dir,
                              'ddf--datapoints--{}--by--global--year.csv'.format(col)),
                 header=True))

    dump_json(os.path.join(out_dir, 'datapackage.json'), get_datapackage(out_dir, update=True))

    print("dataset generated!")
