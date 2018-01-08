#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Update cbpf
------------

"""
from hdx.data.resource import Resource
from hdx.utilities.dictandlist import write_list_to_csv

from helpers import cannonize_name


def update_cbpf(base_url, downloader, poolfundabbrv, year, valid_names, replace_values, folder, filename, resource_id):
    response = downloader.download('%sProjectSummary?poolfundAbbrv=%s' % (base_url, poolfundabbrv))
    jsonresponse = response.json()
    projects = jsonresponse['value']
    transactions = dict()
    for project in projects:
        if project['AllocationYear'] != year:
            continue
        code = project['ChfProjectCode']
        budget = project['Budget']
        directcost = project['TotalDirectCost']
        supportcost = project['TotalSupportCost']
        transactions[code] = budget, directcost, supportcost

    response = downloader.download('%sLocation?poolfundAbbrv=%s' % (base_url, poolfundabbrv))
    jsonresponse = response.json()
    locations = jsonresponse['value']
    totals = dict()
    for location in locations:
        if location['AllocationYear'] != year:
            continue
        code = location['ChfProjectCode']
        admin1 = cannonize_name(location['AdminLocation1'], valid_names, replace_values)
        percentage = float(location['Percentage']) / 100.0
        budget, directcost, supportcost = transactions[code]
        totalbudget, totaldirectcost, totalsupportcost = totals.get(admin1, (0.0, 0.0, 0.0))
        budget *= percentage
        directcost *= percentage
        supportcost *= percentage
        totalbudget += budget
        totaldirectcost += directcost
        totalsupportcost += supportcost
        totals[admin1] = totalbudget, totaldirectcost, totalsupportcost

    rows = list()
    rows.append(['#adm1+name', '#cashflow+type', '#cashflow+value'])
    for admin1 in sorted(totals):
        budget, directcost, supportcost = totals[admin1]
        rows.append([admin1, 'Budget', round(budget)])
        rows.append([admin1, 'Direct Cost', round(directcost)])
        rows.append([admin1, 'Support Cost', round(supportcost)])
    file_to_upload = write_list_to_csv(rows, folder, filename, headers=['Admin Location', 'Cashflow Type', 'Cashflow Value'])
    resource = Resource.read_from_hdx(resource_id)
    resource.set_file_to_upload(file_to_upload)
    resource.update_in_hdx()
