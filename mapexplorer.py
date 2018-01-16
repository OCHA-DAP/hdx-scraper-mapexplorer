#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Update map explorers
--------------------

"""
import datetime
import logging
from os.path import join

from hdx.data.dataset import Dataset

from acled import update_lc_acled, update_ssd_acled
from cbpf import update_cbpf
from fts import update_fts
from rowca import update_lc_rowca

logger = logging.getLogger(__name__)


def get_valid_names(downloader, url, headers):
    rows_gen = downloader.get_tabular_rows(url, dict_rows=True, headers=headers)
    return [x['Name'] for x in rows_gen if x['Name'] != 'Name']


def update_lc(today, downloader, folder, lc_names_url, lc_mappings_url,
              acled_base_url, fts_base_url, rowca_base_url):
    logger.info('Lake Chad Map Explorer Data')
    country_list = ['Cameroon', 'Nigeria', 'Niger', 'Chad']
    valid_names = get_valid_names(downloader, lc_names_url, headers=['ISO', 'Name'])
    replace_values = downloader.download_tabular_key_value(lc_mappings_url)
    logger.info('Lake Chad - ACLED')
    update_lc_acled(today, acled_base_url, downloader, country_list, valid_names, replace_values, folder)
    logger.info('Lake Chad - FTS')
    output_path = join(folder, 'Lake_Chad_Basin_Appeal_Status.csv')
    update_fts(fts_base_url, downloader, country_list, output_path, '2890c719-4fb2-4178-acdb-e0c5c91cfbce')
    logger.info('Lake Chad - ROWCA')
    update_lc_rowca(rowca_base_url, downloader, folder, valid_names, replace_values)
    logger.info('Lake Chad - Dataset Date')
    dataset = Dataset.read_from_hdx('lake-chad-crisis-map-explorer-data')
    dataset.set_dataset_date_from_datetime(today)
    dataset.update_in_hdx()


def update_ssd(today, downloader, folder, ssd_adm1_names_url, ssd_adm2_names_url, ssd_mappings_url,
               acled_base_url, cbpf_base_url):
    logger.info('South Sudan Map Explorer Data')
    country_list = ['South Sudan']
    valid_adm1_names = get_valid_names(downloader, ssd_adm1_names_url, headers=['Name'])
    valid_adm2_names = get_valid_names(downloader, ssd_adm2_names_url, headers=['Name'])
    replace_values = downloader.download_tabular_key_value(ssd_mappings_url)
    logger.info('South Sudan - ACLED')
    update_ssd_acled(today, acled_base_url, downloader, country_list, valid_adm2_names, replace_values, folder)
    logger.info('South Sudan - CBPF')
    year = today.year
    if today.month <= 3:
        year -= 1
    update_cbpf(cbpf_base_url, downloader, 'SSD19', year, valid_adm1_names, replace_values, folder,
                'South_Sudan_Country_Based_Pool_Funds.csv', 'd6b18405-5982-4075-bb0a-a1a85f09d842')
    logger.info('South_Sudan_ - Dataset Date')
    dataset = Dataset.read_from_hdx('south-sudan-crisis-map-explorer-data')
    dataset.set_dataset_date_from_datetime(today)
    dataset.update_in_hdx()
