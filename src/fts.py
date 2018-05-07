#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Update fts
----------

"""
import pandas as pd
from hdx.data.resource import Resource
from hdx.location.country import Country
from pandas.io.json import json_normalize

from src.helpers import drop_columns, hxlate


def update_fts(base_url, downloader, country_list, resource_updates):
    requirements_url = '%splan/country/' % base_url
    funding_url = '%sfts/flow?groupby=plan&countryISO3=' % base_url

    columns_to_keep = ['country', 'id', 'name', 'code', 'startDate', 'endDate', 'year', 'revisedRequirements',
                       'totalFunding']
    combined = pd.DataFrame()
    hxl_names = {
        "country": "#country+name",
        "id": "#x_appeal+id",
        "name": "#x_appeal+name",
        "code": "#x_appeal+code",
        "revisedRequirements": "#x_requirement+x_usd+x_current",
        "endDate": "#date+end",
        "totalFunding": "#x_funding+x_usd",
        "startDate": "#date+start",
        "year": "#date+year",
        "percentFunded": "#x_requirement+x_met+x_percent"
    }

    for country in country_list:
        iso3 = Country.get_iso3_country_code(country)
        r = downloader.download('%s%s' % (requirements_url, iso3))
        data = r.json()['data']
        dfreq_norm = json_normalize(data)
        dfreq_norm['id'].fillna('missing')
        dfreq_loc = json_normalize(data, 'locations')
        dfreq_loc.rename(columns={'name': 'country'}, inplace=True)
        del dfreq_loc['id']
        dfreq_norm_loc = dfreq_norm.join(dfreq_loc)
        dfreq_year = json_normalize(data, 'years')
        del dfreq_year['id']
        dfreq = dfreq_norm_loc.join(dfreq_year)
        r = downloader.download('%s%s' % (funding_url, iso3))
        data = r.json()['data']['report3']['fundingTotals']['objects'][0]['objectsBreakdown']
        dffund = json_normalize(data)
        df = dfreq.merge(dffund, on='id')
        df.totalFunding += df.onBoundaryFunding
        df.rename(columns={'name_x': 'name'}, inplace=True)
        # drop unwanted columns
        df = drop_columns(df, columns_to_keep)
        combined = combined.append(df, ignore_index=True)

    # trim date strings
    combined.startDate = combined.startDate.str[:10]
    combined.endDate = combined.endDate.str[:10]

    # add column for % funded
    combined['percentFunded'] = (pd.to_numeric(combined.totalFunding) / pd.to_numeric(
        combined.revisedRequirements)) * 100

    # sort
    combined.sort_values(['country', 'endDate'], ascending=[True, False], inplace=True)

    # add HXL tags
    combined = hxlate(combined, hxl_names)

    # convert floats to string and trim ( formatters don't work on columns with mixed types)
    combined['percentFunded'] = combined['percentFunded'].astype(str)
    combined['percentFunded'] = \
    combined['percentFunded'].loc[combined['percentFunded'].str.contains('.')].str.split('.').str[0]

    combined.to_csv(resource_updates['fts']['path'], encoding='utf-8', index=False, date_format='%Y-%m-%d')
