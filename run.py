#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import datetime
import logging
from os.path import join, expanduser
from tempfile import gettempdir

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download


# Remove 2 lines below if you don't want emails when there are errors
from hdx.facades import logging_kwargs

from mapexplorer import update_lc, update_ssd

logging_kwargs['smtp_config_yaml'] = join('config', 'smtp_configuration.yml')

from hdx.facades.hdx_scraperwiki import facade

logger = logging.getLogger(__name__)


def main():
    """Generate dataset and create it in HDX"""

    lc_names_url = Configuration.read()['lc_names_url']
    lc_mappings_url = Configuration.read()['lc_mappings_url']
    ssd_adm1_names_url = Configuration.read()['ssd_adm1_names_url']
    ssd_adm2_names_url = Configuration.read()['ssd_adm2_names_url']
    ssd_mappings_url = Configuration.read()['ssd_mappings_url']
    acled_base_url = Configuration.read()['acled_base_url']
    fts_base_url = Configuration.read()['fts_base_url']
    rowca_base_url = Configuration.read()['rowca_base_url']
    cbpf_base_url = Configuration.read()['cbpf_base_url']
    folder = gettempdir()
    today = datetime.datetime.utcnow()
    with Download(basic_auth_file=join(expanduser("~"), '.ftskey')) as downloader:
        update_lc(today, downloader, folder, lc_names_url, lc_mappings_url,
                  acled_base_url, fts_base_url, rowca_base_url)
        update_ssd(today, downloader, folder, ssd_adm1_names_url, ssd_adm2_names_url, ssd_mappings_url, acled_base_url,
                   cbpf_base_url)


if __name__ == '__main__':
    facade(main, hdx_site='test', project_config_yaml=join('config', 'project_configuration.yml'))

