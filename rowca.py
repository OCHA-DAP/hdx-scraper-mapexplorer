#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Update rowca
------------

"""

from dateutil import parser
from hdx.data.resource import Resource
from hdx.utilities.dictandlist import write_list_to_csv

from helpers import cannonize_name


def reformat_date(date):
    return parser.parse(date).strftime('%Y-%m-%d')


def update_rowca_population(base_url, downloader, country_numbers, valid_names, replace_values,
                            folder, filename, resource_id):
    response = downloader.download('%scountry=%s&subcat=4&inclids=yes&final=1&format=json&lng=en' % (base_url,
                                                                                                     country_numbers))
    rows = [['#country+name', '#adm1+name', '#population', '#date']]
    rowset = set()
    for item in response.json():
        if item['KeyFigure'] != 'Total Population' or len(item['PCode']) != 6:
            continue
        country = item['Country']
        admin1 = cannonize_name(item['ReportedLocation'], valid_names, replace_values)
        value = item['Total']
        asofdate = reformat_date(item['AsOfDate'])
        row = (country, admin1, value, asofdate)
        curlen = len(rowset)
        rowset.add(row)
        if len(rowset) == curlen:
            continue
        rows.append(row)

    rows = sorted(rows, key=lambda x: [x[0], x[1]])
    file_to_upload = write_list_to_csv(rows, folder, filename, headers=['Country', 'ReportedLocation', 'Total',
                                                                        'AsOfDate'])
    resource = Resource.read_from_hdx(resource_id)
    resource.set_file_to_upload(file_to_upload)
    resource.update_in_hdx()


def update_rowca_movement(base_url, downloader, country_numbers, valid_names, replace_values,
                          folder, filename, resource_id):
    response = downloader.download('%scountry=%s&subcat=9,10&inclids=yes&format=json&lng=en' % (base_url,
                                                                                                        country_numbers))
    types = {15: 'refugee', 21: 'idp'}
    rowdict = {(-1, ): (('#country+name', '#adm1+name', '#date+bin', '#affected+displaced', '#affected+type'), )}
    months = ['02-28', '04-30', '06-30', '08-31', '10-31', '12-31']

    def get_month_end(date):
        # round date to nearest 2 months
        month_index = (date.month - 1) // 2
        return '%d-%s' % (date.year, months[month_index])

    for item in response.json():
        if len(item['PCode']) != 6:
            continue
        keyfigureid = item['KeyFigureId']
        if keyfigureid not in types.keys():
            continue
        country = item['Country']
        admin1 = cannonize_name(item['ReportedLocation'], valid_names, replace_values)
        if country == 'Cameroon':
            if admin1 != 'Extrême-Nord':
                continue
        elif country == 'Chad':
            if admin1 not in ['Mayo-Kebbi Est', 'Lac']:
                continue
        value = item['Total']
        asofdate = parser.parse(item['AsOfDate'])
        month_end = get_month_end(asofdate)
        key = (keyfigureid, country, admin1, month_end)
        existing_row = rowdict.get(key)
        if not existing_row or (existing_row and asofdate > existing_row[1]):
            row = ((country, admin1, month_end, value, types[keyfigureid]), asofdate)
            rowdict[key] = row

    rows = [rowdict[key][0] for key in sorted(rowdict)]
    file_to_upload = write_list_to_csv(rows, folder, filename, headers=['Country', 'ReportedLocation', 'Period',
                                                                        'Total', 'DisplType'])
    resource = Resource.read_from_hdx(resource_id)
    resource.set_file_to_upload(file_to_upload)
    resource.update_in_hdx()


def update_lc_rowca(base_url, downloader, folder, valid_names, replace_values):
    country_numbers = '3,4,8,9'
    population_filename = 'Lake_Chad_Basin_Estimated_Population.csv'
    population_resource_id = '048df35c-e35f-4b1f-aa1a-2d1ce1292f22'
    update_rowca_population(base_url, downloader, country_numbers, valid_names, replace_values,
                            folder, population_filename, population_resource_id)
    movement_filename = 'Lake_Chad_Basin_Displaced.csv'
    movement_resource_id = '1bdcc8f3-223c-4f7d-9bc6-48be317d50c5'
    update_rowca_movement(base_url, downloader, country_numbers, valid_names, replace_values,
                          folder, movement_filename, movement_resource_id)
