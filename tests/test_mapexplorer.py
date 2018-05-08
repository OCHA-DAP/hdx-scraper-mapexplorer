#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for scrapername.

'''
import filecmp
from datetime import datetime
from os.path import join
from tempfile import gettempdir

import pytest
from hdx.hdx_configuration import Configuration
import hdx.utilities.downloader
from hdx.utilities.loader import load_json

from src.acled import update_lc_acled, update_ssd_acled
from mapexplorer import get_valid_names
from src.cbpf import update_cbpf
from src.fts import update_fts
#from src.rowca import update_rowca


class TestScraperName:
    @pytest.fixture(scope='class')
    def configuration(self):
        Configuration._create(user_agent='test', hdx_read_only=True,
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))

    @pytest.fixture(scope='class')
    def folder(self, configuration):
        return gettempdir()

    @pytest.fixture(scope='class')
    def downloader(self):
        return hdx.utilities.downloader.Download()

    @pytest.fixture(scope='class')
    def today(self):
        return datetime.strptime('2018-01-16', '%Y-%m-%d')

    @pytest.fixture(scope='class')
    def lc_country_list(self, configuration):
        return ['Nigeria']

    @pytest.fixture(scope='class')
    def ssd_country_list(self, configuration):
        return ['South Sudan']

    @pytest.fixture(scope='class')
    def valid_lc_names(self, downloader):
        lc_names_url = Configuration.read()['lc_names_url']
        return get_valid_names(downloader, lc_names_url, headers=['ISO', 'Name'])

    @pytest.fixture(scope='class')
    def replace_lc_values(self, downloader):
        lc_mappings_url = Configuration.read()['lc_mappings_url']
        return downloader.download_tabular_key_value(lc_mappings_url)

    @pytest.fixture(scope='class')
    def valid_ssd_adm1_names(self, downloader):
        ssd_adm1_names_url = Configuration.read()['ssd_adm1_names_url']
        return get_valid_names(downloader, ssd_adm1_names_url, headers=['Name'])

    @pytest.fixture(scope='class')
    def valid_ssd_adm2_names(self, downloader):
        ssd_adm2_names_url = Configuration.read()['ssd_adm2_names_url']
        return get_valid_names(downloader, ssd_adm2_names_url, headers=['Name'])

    @pytest.fixture(scope='class')
    def replace_ssd_values(self, downloader):
        ssd_mappings_url = Configuration.read()['ssd_mappings_url']
        return downloader.download_tabular_key_value(ssd_mappings_url)

    @pytest.fixture(scope='function')
    def downloaderfts(self):
        class Response:
            @staticmethod
            def json():
                pass

        class Download:
            @staticmethod
            def download(url):
                response = Response()
                if url == 'http://lala/plan/country/NGA':
                    def fn():
                        return load_json(join('tests', 'fixtures', 'FTS_plan_NGA.json'))
                    response.json = fn
                elif url == 'http://lala/fts/flow?groupby=plan&countryISO3=NGA':
                    def fn():
                        return load_json(join('tests', 'fixtures', 'FTS_flow_NGA.json'))
                    response.json = fn
                return response
        return Download()

    @pytest.fixture(scope='function')
    def downloaderrowca(self):
        class Response:
            @staticmethod
            def json():
                pass

        class Download:
            @staticmethod
            def download(url):
                response = Response()
                if url == 'http://haha/country=3,4,8,9&subcat=4&inclids=yes&final=1&format=json&lng=en':
                    def fn():
                        return load_json(join('tests', 'fixtures', 'ROWCA_population.json'))
                    response.json = fn
                elif url == 'http://haha/country=3,4,8,9&subcat=9,10&inclids=yes&final=1&format=json&lng=en':
                    def fn():
                        return load_json(join('tests', 'fixtures', 'ROWCA_movement.json'))
                    response.json = fn
                return response
        return Download()

    @pytest.fixture(scope='function')
    def downloadercbpf(self):
        class Response:
            @staticmethod
            def json():
                pass

        class Download:
            @staticmethod
            def download(url):
                response = Response()
                if url == 'http://mama/ProjectSummary?poolfundAbbrv=SSD19':
                    def fn():
                        return load_json(join('tests', 'fixtures', 'CBPF_ProjectSummary_SSD.json'))
                    response.json = fn
                elif url == 'http://mama/Location?poolfundAbbrv=SSD19':
                    def fn():
                        return load_json(join('tests', 'fixtures', 'CBPF_Location_SSD.json'))
                    response.json = fn
                return response
        return Download()

    def test_lc_acled(self, folder, today, lc_country_list, valid_lc_names, replace_lc_values):
        resource_updates = dict()
        filename = 'Lake_Chad_Basin_Recent_Conflict_Events.csv'
        expected_events = join('tests', 'fixtures', filename)
        actual_events = join(folder, filename)
        resource_updates['acled_events'] = {'path': actual_events}
        filename = 'Lake_Chad_Basin_Recent_Conflict_Event_Total_Fatalities.csv'
        expected_fatalities = join('tests', 'fixtures', filename)
        actual_fatalities = join(folder, filename)
        resource_updates['acled_fatalities'] = {'path': actual_fatalities}
        update_lc_acled(today, 'https://raw.githubusercontent.com/mcarans/hdxscraper-mapexplorer/master/tests/fixtures/ACLEDNigeria.csv?', lc_country_list, valid_lc_names, replace_lc_values, resource_updates)
        assert filecmp.cmp(expected_events, actual_events, shallow=False) is True, 'Expected: %s and Actual: %s do not match!' % (expected_events, actual_events)
        assert filecmp.cmp(expected_fatalities, actual_fatalities, shallow=False) is True, 'Expected: %s and Actual: %s do not match!' % (expected_fatalities, actual_fatalities)

    def test_ssd_acled(self, folder, today, ssd_country_list, valid_ssd_adm2_names, replace_ssd_values):
        resource_updates = dict()
        filename = 'South_Sudan_Recent_Conflict_Events.csv'
        expected_events = join('tests', 'fixtures', filename)
        actual_events = join(folder, filename)
        resource_updates['acled_events'] = {'path': actual_events}
        filename = 'South_Sudan_Recent_Conflict_Event_Total_Fatalities.csv'
        expected_fatalities = join('tests', 'fixtures', filename)
        actual_fatalities = join(folder, filename)
        resource_updates['acled_fatalities'] = {'path': actual_fatalities}
        update_ssd_acled(today, 'https://raw.githubusercontent.com/mcarans/hdxscraper-mapexplorer/master/tests/fixtures/ACLEDSouthSudan.csv?', ssd_country_list, valid_ssd_adm2_names, replace_ssd_values, resource_updates)
        assert filecmp.cmp(expected_events, actual_events, shallow=False) is True, 'Expected: %s and Actual: %s do not match!' % (expected_events, actual_events)
        assert filecmp.cmp(expected_fatalities, actual_fatalities, shallow=False) is True, 'Expected: %s and Actual: %s do not match!' % (expected_fatalities, actual_fatalities)

    def test_fts(self, folder, downloaderfts, lc_country_list):
        resource_updates = dict()
        filename = 'Lake_Chad_Basin_Appeal_Status.csv'
        expected = join('tests', 'fixtures', filename)
        actual = join(folder, filename)
        resource_updates['fts'] = {'path': actual}
        update_fts('http://lala/', downloaderfts, lc_country_list, resource_updates)
        assert filecmp.cmp(expected, actual, shallow=False) is True, 'Expected: %s and Actual: %s do not match!' % (expected, actual)

    def test_cbpf(self, folder, today, downloadercbpf, valid_ssd_adm1_names, replace_ssd_values):
        resource_updates = dict()
        filename = 'South_Sudan_Country_Based_Pool_Funds.csv'
        expected = join('tests', 'fixtures', filename)
        actual = join(folder, filename)
        resource_updates['cbpf'] = {'path': actual}
        update_cbpf('http://mama/', downloadercbpf, 'SSD19', today, valid_ssd_adm1_names, replace_ssd_values, resource_updates)
        assert filecmp.cmp(expected, actual, shallow=False) is True, 'Expected: %s and Actual: %s do not match!' % (expected, actual)


    # def test_rowca(self, folder, downloaderrowca, valid_lc_names, replace_lc_values):
    #     resource_updates = dict()
    #     filename = 'Lake_Chad_Basin_Estimated_Population.csv'
    #     expected_population = join('tests', 'fixtures', filename)
    #     actual_population = join(folder, filename)
    #     resource_updates['rowca_population'] = {'path': actual_population}
    #     filename = 'Lake_Chad_Basin_Displaced.csv'
    #     expected_displaced = join('tests', 'fixtures', filename)
    #     actual_displaced = join(folder, filename)
    #     resource_updates['rowca_displaced'] = {'path': actual_displaced}
    #     update_rowca('http://haha/', downloaderrowca, valid_lc_names, replace_lc_values, resource_updates)
    #     assert filecmp.cmp(expected_population, actual_population, shallow=False) is True, 'Expected: %s and Actual: %s do not match!' % (expected_population, actual_population)
    #     assert filecmp.cmp(expected_displaced, actual_displaced, shallow=False) is True, 'Expected: %s and Actual: %s do not match!' % (expected_displaced, actual_displaced)
