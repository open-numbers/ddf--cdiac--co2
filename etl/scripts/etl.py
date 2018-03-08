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


def replace_negative(ser):
    '''replacing negative numbers with zeros'''
    ser.loc[ser < 0] = 0
    return ser


if __name__ == '__main__':

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
        ser = ndf[col].copy()
        ser = replace_negative(ser)
        # ndf[col] = ndf[col].map(format_float_digits)
        (ser
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
        ser = replace_negative(ser)
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

    # dump_json(os.path.join(out_dir, 'datapackage.json'), get_datapackage(out_dir, update=True))

    print("dataset generated!")
