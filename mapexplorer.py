#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Update map explorers
--------------------

"""
import logging
from os.path import join

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource

from src.acled import update_lc_acled, update_ssd_acled
from src.cbpf import update_cbpf
from src.fts import update_fts
from src.rowca import update_rowca

logger = logging.getLogger(__name__)


def get_valid_names(downloader, url, headers):
    rows_gen = downloader.get_tabular_rows(url, dict_rows=True, headers=headers)
    return [x['Name'] for x in rows_gen if x['Name'] != 'Name']


def update_resources(resource_updates):
    for resource_info in resource_updates.values():
        resource = Resource.read_from_hdx(resource_info['id'])
        resource.set_file_to_upload(resource_info['path'])
        resource.update_in_hdx()


def update_lc(today, downloader, folder, lc_names_url, lc_mappings_url,
              acled_base_url, fts_base_url, rowca_base_url):
    logger.info('Lake Chad Map Explorer Data')
    country_list = ['Cameroon', 'Nigeria', 'Niger', 'Chad']
    valid_names = get_valid_names(downloader, lc_names_url, headers=['ISO', 'Name'])
    replace_values = downloader.download_tabular_key_value(lc_mappings_url)
    resource_updates = dict()
    resource_updates['acled_events'] = {'id': 'fc396bf2-d204-48b2-84d2-337ada015273',
                                        'path': join(folder, 'Lake_Chad_Basin_Recent_Conflict_Events.csv')}
    resource_updates['acled_fatalities'] = {'id': '3792ee5d-ca30-4e5c-96c8-618c6b625d12',
                                            'path': join(folder, 'Lake_Chad_Basin_Recent_Conflict_Event_Total_Fatalities.csv')}
    resource_updates['fts'] = {'id': '2890c719-4fb2-4178-acdb-e0c5c91cfbce',
                               'path': join(folder, 'Lake_Chad_Basin_Appeal_Status.csv')}
    resource_updates['rowca_population'] = {'id': '048df35c-e35f-4b1f-aa1a-2d1ce1292f22',
                                            'path': join(folder, 'Lake_Chad_Basin_Estimated_Population.csv')}
    resource_updates['rowca_displaced'] = {'id': '1bdcc8f3-223c-4f7d-9bc6-48be317d50c5',
                                           'path': join(folder, 'Lake_Chad_Basin_Displaced.csv')}
    logger.info('Lake Chad - ACLED')
    update_lc_acled(today, acled_base_url, country_list, valid_names, replace_values, resource_updates)
    logger.info('Lake Chad - FTS')
    update_fts(fts_base_url, downloader, country_list, resource_updates)
    logger.info('Lake Chad - ROWCA')
    update_rowca(rowca_base_url, downloader, valid_names, replace_values, resource_updates)
    logger.info('Lake Chad - Dataset Date')
    update_resources(resource_updates)
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
    resource_updates = dict()
    resource_updates['acled_events'] = {'id': '3480f362-67bb-44d0-b749-9e8fc0963fc0',
                                        'path': join(folder, 'South_Sudan_Recent_Conflict_Events.csv')}
    resource_updates['acled_fatalities'] = {'id': 'a67b85ee-50b4-4345-9102-d88bf9091e95',
                                            'path': join(folder, 'South_Sudan_Recent_Conflict_Event_Total_Fatalities.csv')}
    resource_updates['cbpf'] = {'id': 'd6b18405-5982-4075-bb0a-a1a85f09d842',
                                'path': join(folder, 'South_Sudan_Country_Based_Pool_Funds.csv')}
    logger.info('South Sudan - ACLED')
    update_ssd_acled(today, acled_base_url, country_list, valid_adm2_names, replace_values, resource_updates)
    logger.info('South Sudan - CBPF')
    update_cbpf(cbpf_base_url, downloader, 'SSD19', today, valid_adm1_names, replace_values, resource_updates)
    logger.info('South_Sudan_ - Dataset Date')
    update_resources(resource_updates)
    dataset = Dataset.read_from_hdx('south-sudan-crisis-map-explorer-data')
    dataset.set_dataset_date_from_datetime(today)
    dataset.update_in_hdx()
