#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Update map explorers
--------------------

"""
import datetime
import logging
from os.path import join

from acled import update_lc_acled, update_ssd_acled
from cbpf import update_cbpf
from fts import update_fts
from rowca import update_lc_rowca

logger = logging.getLogger(__name__)


def update_lc(downloader, folder, dataset_base_url, lc_names_url, lc_mappings_url, fts_base_url, rowca_base_url):
    logger.info('Lake Chad Map Explorer Data')
    country_list = ['Cameroon', 'Nigeria', 'Niger', 'Chad']
    rows_gen = downloader.get_tabular_rows(lc_names_url, dict_rows=True, headers=['ISO', 'Name'])
    valid_names = [x['Name'] for x in rows_gen if x['Name'] != 'Name']
    replace_values = downloader.download_tabular_key_value(lc_mappings_url)
    logger.info('Lake Chad - ACLED')
    update_lc_acled(dataset_base_url, downloader, country_list, valid_names, replace_values, folder)
    logger.info('Lake Chad - FTS')
    output_path = join(folder, 'Lake_Chad_Basin_Appeal_Status.csv')
    update_fts(fts_base_url, downloader, country_list, output_path, '2890c719-4fb2-4178-acdb-e0c5c91cfbce')
    logger.info('Lake Chad - ROWCA')
    update_lc_rowca(rowca_base_url, downloader, folder, valid_names, replace_values)


def update_ssd(downloader, folder, dataset_base_url, ssd_names_url, ssd_mappings_url, cbpf_base_url):
    logger.info('South Sudan Map Explorer Data')
    country_list = ['South Sudan']
    rows_gen = downloader.get_tabular_rows(ssd_names_url, dict_rows=True, headers=['Name'])
    valid_names = [x['Name'] for x in rows_gen if x['Name'] != 'Name']
    replace_values = downloader.download_tabular_key_value(ssd_mappings_url)
    logger.info('South Sudan - ACLED')
    update_ssd_acled(dataset_base_url, downloader, country_list, valid_names, replace_values, folder)
    logger.info('South Sudan - CBPF')
    now = datetime.datetime.now()
    year = now.year
    if now.month <= 3:
        year -= 1
    update_cbpf(cbpf_base_url, downloader, 'SSD19', year, valid_names, replace_values, folder,
                'South_Sudan_Country_Based_Pool_Funds.csv', 'd6b18405-5982-4075-bb0a-a1a85f09d842')
