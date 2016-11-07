# -*- coding: utf-8 -*-
"""transform the CDIAC CO2 data set to DDF model"""

### TODO: outdated and needs update.

import pandas as pd
import numpy as np
import re
from ddf_utils.index import create_index_file
from ddf_utils.str import to_concept_id, format_float_sigfig
from ddf_utils.io import cleanup

# configuration of file path
source_dir = '../source/'
out_dir = '../../'


def concat_data(files, skip=0, **kwargs):
    """concatenate a list of data from source and return one dataframe.
    Additional keyword arguments will be passed to pandas read_csv function.
    """
    res = []
    for x in files:
        path = os.path.join(source_dir, x)
        df = pd.read_csv(path, **kwargs)
        # quick fix for malformed csv downloaded from data povider
        if df.columns[0] == 'Year"':
            df = df.rename(columns={'Year"': 'Year'})
        df.columns = list(map(lambda x: x.lower().replace('\n', ''), df.columns))

        df = df.ix[skip:]  # skip first few rows of data
        res.append(df)

    df_all = pd.concat(res)
    if df_all.year.hasnans:
        print('droping lines where year is NaN:')
        if 'nation' in df_all.columns:
            print(df_all[pd.isnull(df_all['year'])][['nation', 'year']])
        else:
            print(df_all[pd.isnull(df_all['year'])][['global', 'year']])
        df_all = df_all.dropna(subset=['year']).copy()
    return df_all


def get_concept_name(name):
    """return concept name for given indicator name.
    """

    print(name)
    if 'total emissions' in name.lower():
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


def extract_concepts(global_df, nation_df):
    """extract both discrete and continuous concepts from concatenated data
    returns a tuple of concepts dataframe, one is discrete and the other
    is continuous.
    """

    # headers for dataframe and csv exports
    headers = ['concept', 'name', 'concept_type']

    # define discrete and continuous concepts
    concept_discrete = ['year', 'nation', 'global', 'name']

    cols = np.r_[global_df.columns, nation_df.columns]
    concept_continuous = [x for x in cols if x not in concept_discrete]

    # build dataframe
    discrete_df = pd.DataFrame([], columns=headers)
    discrete_df['concept'] = concept_discrete
    discrete_df['name'] = discrete_df['concept'].str.title()
    discrete_df['concept_type'] = ['time', 'entity_domain', 'entity_domain', 'string']

    continuous_df = pd.DataFrame([], columns=headers)
    continuous_df['name'] = concept_continuous
    continuous_df['concept_type'] = 'measure'
    continuous_df['concept'] = continuous_df['name'].apply(get_concept_name)
    coutinuous_df = continuous_df.drop_duplicates(subset='concept')

    return (discrete_df, continuous_df)


def extract_datapoints(df):
    """extract data points from concatenated data"""

    res = {}

    df.columns = list(map(get_concept_name, df.columns))
    df['year'] = df['year'].apply(int)

    if 'nation' in df.columns:  # if it's nation data, make 'nation' as index
        df['nation'] = df['nation'].map(to_concept_id)
        df = df.set_index(['nation', 'year'])
    else:
        df['global'] = 'world'
        df = df.set_index(['global', 'year'])

    for col in df.columns:
        res[col] = df[col].dropna()

    return res


if __name__ == '__main__':
    import os

    cleanup(out_dir)

    print('reading source files...')
    global_files = [x for x in os.listdir(source_dir) if x.startswith('global')]
    nation_files = [x for x in os.listdir(source_dir) if x.startswith('nation')]

    global_df = concat_data(global_files, skip=1)
    nation_df = concat_data(nation_files, skip=2, na_values='.')

    print('creating concepts files...')
    discrete_df, continuous_df = extract_concepts(global_df, nation_df)
    discrete_df.to_csv(os.path.join(out_dir, 'ddf--concepts--discrete.csv'), index=False)
    continuous_df.to_csv(os.path.join(out_dir, 'ddf--concepts--continuous.csv'), index=False)

    print('creating entities files...')
    nation = nation_df[['nation']].drop_duplicates()
    nation['nation_id'] = nation['nation'].map(to_concept_id)
    nation.columns = ['name', 'nation']
    path = os.path.join(out_dir, 'ddf--entities--nation.csv')
    nation.to_csv(path, index=False)

    global_ent = pd.DataFrame([['world', 'World']], columns=['global', 'name'])
    global_ent.to_csv(os.path.join(out_dir, 'ddf--entities--global.csv'), index=False)

    print('creating data points files...')
    for c, df in extract_datapoints(global_df).items():
        path = os.path.join(out_dir, 'ddf--datapoints--'+c+'--by--global--version--year.csv')
        df = df.map(format_float_sigfig)
        df.to_csv(path, header=True)

    for c, df in extract_datapoints(nation_df).items():
        path = os.path.join(out_dir, 'ddf--datapoints--'+c+'--by--nation--version--year.csv')
        df = df.map(format_float_sigfig)
        df.to_csv(path, header=True)

    print('creating index file...')
    create_index_file(out_dir)

    print('Done!')
