#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
SCRAPERNAME:
------------

Reads ScraperName JSON and creates datasets.

"""
import logging

from hdx.data.resource import Resource
from hdx.utilities.dictandlist import write_list_to_csv

logger = logging.getLogger(__name__)


def update_cbpf(base_url, downloader, year, folder):
    response = downloader.download('%sProjectSummary?poolfundAbbrv=SSD19' % base_url)
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

    response = downloader.download('%sLocation?poolfundAbbrv=SSD19' % base_url)
    jsonresponse = response.json()
    locations = jsonresponse['value']
    totals = dict()
    norows = 0
    for location in locations:
        if location['AllocationYear'] != year:
            continue
        norows += 1
        code = location['ChfProjectCode']
        admin1 = location['AdminLocation1']
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
        rows.append([admin1, 'Budget', budget])
        rows.append([admin1, 'Direct Cost', directcost])
        rows.append([admin1, 'Support Cost', supportcost])
    file_to_upload = write_list_to_csv(rows, folder, 'cbpf.csv', headers=['Admin Location', 'Cashflow Type', 'Cashflow Value'])
    resource = Resource.read_from_hdx('d6b18405-5982-4075-bb0a-a1a85f09d842')
    resource.set_file_to_upload(file_to_upload)
    resource.update_in_hdx()




