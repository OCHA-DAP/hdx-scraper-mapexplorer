#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Update acled
------------

"""
from datetime import timedelta

import pandas as pd
import dateutil.relativedelta
from hdx.data.resource import Resource
from os.path import join

from hdx.location.country import Country

from helpers import OutputError, hxlate, drop_columns


def get_country_url_string(country_list):
    return ':OR:iso='.join([str(Country.get_m49_from_iso3(Country.get_iso3_country_code(country))) for country in country_list])


def update_acled(today, base_url, downloader, countries_to_keep, valid_names, replace_values,
                 column_for_cannonicalization, output_path, resource_id, admbin=0):
    # configuration
    columns_to_keep = ['EVENT_ID_CNTY', 'EVENT_DATE', 'EVENT_TYPE', 'ACTOR1', 'ASSOC_ACTOR_1',
                       'ACTOR2', 'ASSOC_ACTOR_2', 'COUNTRY', 'ADMIN1', 'ADMIN2',
                       'LOCATION', 'LATITUDE', 'LONGITUDE', 'SOURCE', 'NOTES', 'FATALITIES']

    def cannonize_names(df, valid_names, column_for_cannonicalization, replace_values):
        # get the list of desired admin names
        cannon_column_name = 'Name'
        names = pd.DataFrame({cannon_column_name: valid_names})
        # proper case the names
        df[column_for_cannonicalization] = df[column_for_cannonicalization].str.title()
        # strip out leading/trailing white space
        df[column_for_cannonicalization] = df[column_for_cannonicalization].str.strip()
        # replace values
        df.replace(to_replace={column_for_cannonicalization: replace_values}, inplace=True)
        # joining data to preferred admin names.  "How" specifies what to keep in the output.
        merged = pd.merge(df, names, left_on=column_for_cannonicalization, right_on=cannon_column_name, how="left")
        bad_names = merged.loc[merged[cannon_column_name].isnull()]
        no_bad_names = bad_names.shape[0]
        # should be no rows if cannonicalization has worked well
        if no_bad_names > 0:
            raise OutputError('Invalid location (not in names list)' % bad_names.loc[:, column_for_cannonicalization].unique())
        return df

    def latest_dates(df):
        # get latest date in dataset and go back 5 months
        x = df.EVENT_DATE.max() - dateutil.relativedelta.relativedelta(months=5)
        # go to the first day of that month
        x = x.replace(day=1)
        df = df.loc[df.EVENT_DATE >= x, :]
        return df

    def binning(admbin, df):
        # grouping into bins
        grouper = pd.Grouper(key='EVENT_DATE', freq='1M')
        groupby = None
        if admbin == 1:
            groupby = ['COUNTRY', 'ADMIN1', grouper]
        elif admbin == 2:
            groupby = ['COUNTRY', 'ADMIN1', 'ADMIN2', grouper]

        df = df.groupby(groupby).sum()
        # rename date column
        df.index.set_names('PERIOD_ENDING', level=admbin + 1, inplace=True)
        # flatten the indices
        df.reset_index(inplace=True)
        return df

    startdate = today - timedelta(days=365)
    url = '%siso=%s&event_date=%s|%s&event_date_where=BETWEEN' % (base_url, get_country_url_string(countries_to_keep), startdate.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d'))
    df = pd.read_csv(url)
    df.columns = df.columns.str.upper()
    df = drop_columns(df, columns_to_keep)
    df.EVENT_DATE = pd.to_datetime(df.EVENT_DATE, dayfirst=True)

    df = latest_dates(df)
    if df.shape[0] == 0:
        raise OutputError('No rows to output!')
    df = cannonize_names(df, valid_names, column_for_cannonicalization, replace_values)
    if admbin == -1:
        hxl_names = {'EVENT_ID_CNTY': '#event+code', 'EVENT_DATE': '#date', 'EVENT_TYPE': '#event+type',
                     'ACTOR1': '#actor', 'ASSOC_ACTOR_1': '#actor',
                     'ACTOR2': '#actor', 'ASSOC_ACTOR_2': '#actor', 'COUNTRY': '#country+name', 'ADMIN1': '#adm1+name',
                     'ADMIN2': '#adm2+name',
                     'LOCATION': '#loc+name', 'LATITUDE': '#geo+lat', 'LONGITUDE': '#geo+lon',
                     'SOURCE': '#meta+source', 'NOTES': '#notes', 'FATALITIES': '#affected+killed'
                     }
        df.EVENT_DATE = df['EVENT_DATE'].dt.strftime('%Y-%m-%d')
    else:
        hxl_names = {'COUNTRY': '#country+name', 'ADMIN1': '#adm1+name', 'ADMIN2': '#adm2+name',
                     'PERIOD_ENDING': '#date+bin', 'FATALITIES': '#affected+killed'}
        df.drop(['LATITUDE', 'LONGITUDE'], axis=1, inplace=True)  # otherwise they will be summed by groupby
        df = binning(admbin, df)
        df.PERIOD_ENDING = df.PERIOD_ENDING.dt.strftime('%Y-%m-%d')
        if admbin == 1:
            columns_to_keep = [x for x in hxl_names.keys() if x != 'ADMIN2']
        else:
            columns_to_keep = hxl_names.keys()
        df = drop_columns(df, columns_to_keep)
    df = hxlate(df, hxl_names)
    df.to_csv(output_path, encoding='utf-8', index=False, date_format='%Y-%m-%d', float_format='%.0f')
    resource = Resource.read_from_hdx(resource_id)
    resource.set_file_to_upload(output_path)
    resource.update_in_hdx()


def update_lc_acled(today, base_url, downloader, country_list, lc_names_url, replace_values, folder):
    column_for_cannonicalization = 'ADMIN1'
    output_path = join(folder, 'Lake_Chad_Basin_Recent_Conflict_Events.csv')
    resource_id = 'fc396bf2-d204-48b2-84d2-337ada015273'
    update_acled(today, base_url, downloader, country_list, lc_names_url, replace_values, column_for_cannonicalization,
                 output_path, resource_id, admbin=-1)
    output_path = join(folder, 'Lake_Chad_Basin_Recent_Conflict_Event_Total_Fatalities.csv')
    resource_id = '3792ee5d-ca30-4e5c-96c8-618c6b625d12'
    update_acled(today, base_url, downloader, country_list, lc_names_url, replace_values, column_for_cannonicalization,
                 output_path, resource_id, admbin=1)


def update_ssd_acled(today, base_url, downloader, country_list, ssd_names_url, replace_values, folder):
    column_for_cannonicalization = 'ADMIN2'
    output_path = join(folder, 'South_Sudan_Recent_Conflict_Events.csv')
    resource_id = '3480f362-67bb-44d0-b749-9e8fc0963fc0'
    update_acled(today, base_url, downloader, country_list, ssd_names_url, replace_values, column_for_cannonicalization,
                 output_path, resource_id, admbin=-1)
    output_path = join(folder, 'South_Sudan_Recent_Conflict_Event_Total_Fatalities.csv')
    resource_id = 'a67b85ee-50b4-4345-9102-d88bf9091e95'
    update_acled(today, base_url, downloader, country_list, ssd_names_url, replace_values, column_for_cannonicalization,
                 output_path, resource_id, admbin=2)
