#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join
from tempfile import gettempdir

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download


# Remove 2 lines below if you don't want emails when there are errors
from hdx.facades import logging_kwargs

from mapexplorer import update_cbpf

logging_kwargs['smtp_config_yaml'] = join('config', 'smtp_configuration.yml')

from hdx.facades.hdx_scraperwiki import facade

logger = logging.getLogger(__name__)


def main():
    """Generate dataset and create it in HDX"""

    cbpf_base_url = Configuration.read()['cbpf_base_url']
    folder = gettempdir()
    with Download() as downloader:
        update_cbpf(cbpf_base_url, downloader, 2017, folder)


if __name__ == '__main__':
    facade(main, hdx_site='test', project_config_yaml=join('config', 'project_configuration.yml'))

