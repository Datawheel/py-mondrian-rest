import unittest
from mock import patch, MagicMock, Mock
from operator import itemgetter
from urlparse import urljoin
import json
import os

import requests

from .client import MondrianClient, Cube, Aggregation, CUBE_ATTRS

API_BASE = 'http://mondrian'
FIXTURES_DIR =  os.path.join(os.path.dirname(os.path.realpath(__file__)), 'test_fixtures')

class TestMondrianClient(unittest.TestCase):

    def setUp(self):
        self.client = MondrianClient(API_BASE)
        with open(
                os.path.join(
                    FIXTURES_DIR,
                    'cube_response.json'
                )) as f:
            self.cube_fixture = f.read()

    @patch('requests.get')
    def test_get_cubes(self, MockRequests):
        MockRequests.json.return_value = { 'cubes': [json.loads(self.cube_fixture)] }
        cs = self.client.get_cubes()

        assert len(cs) == 1
        assert type(cs[0]) == Cube
        MockRequests.assert_called_with(urljoin(API_BASE, 'cubes'))

    @patch('requests.get')
    def test_get_one_cube(self, MockRequests):
        MockRequests.json.return_value = self.cube_fixture
        c = self.client.get_cube('foodmart')
        assert type(c) == Cube
        MockRequests.assert_called_with(urljoin(API_BASE, 'cubes/foodmart'))

    @patch('client.MondrianClient._request')
    def test_get_aggregation(self, mock_client_request):
        cube = json.loads(self.cube_fixture)
        c = self.client.get_aggregation('exports',
                                        {
                                            'drilldown': [cube['dimensions'][0]['hierarchies'][0]['levels'][1]],
                                            'measures': [cube['measures'][0]],
                                            'parents': 'true'
                                        })

        mock_client_request.assert_called_with(
            urljoin(API_BASE, 'cubes/exports/aggregate'),
            {
                'drilldown[]': ['[Date].[Year]'],
                'measures[]': ['Q Traded'],
                'nonempty': 'false',
                'parents': 'true',
                'distinct': 'false'
            }
        )

class TestAggregation(unittest.TestCase):
    def setUp(self):
        self.client = MondrianClient(API_BASE)
        with open(os.path.join(FIXTURES_DIR, 'aggregation_response.json')) as f:
            self.aggregation_fixture = json.load(f)

        with open(os.path.join(FIXTURES_DIR, 'aggregation_reponse_with_ancestors.json')) as f:
            self.aggregation_fixture_with_parents = json.load(f)

        with open(os.path.join(FIXTURES_DIR, 'cube_export.json')) as f:
            self.cube_response = json.load(f)

    def test_tidy_data(self):
        agg = Aggregation(self.aggregation_fixture)

        # TODO: add assertions
        print(len(agg.tidy['data']))


    def test_pandas_with_parents(self):
        cube = Cube(*(itemgetter(*CUBE_ATTRS)(self.cube_response) + (self.client,)))
        agg = Aggregation(self.aggregation_fixture_with_parents,
                          cube,
                          {'nonempty': True,
                           'drilldown': [{u'caption': u'Year', u'name': u'Year', u'full_name': u'[Date].[Year]'}, {u'caption': u'HS0', u'name': u'HS0', u'full_name': u'[HS].[HS2]'}],
                           'cut': [{'full_name': u'[Export Geography].[Region Metropolitana Santiago]'}],
                           'parents': True,
                           'measures': [{u'caption': u'FOB US', u'name': u'FOB US', u'full_name': u'[Measures].[FOB US]', u'annotations': {}}, {u'caption': u'Geo Rank Across Time', u'name': u'Geo Rank Across Time', u'full_name': u'[Measures].[Geo Rank Across Time]', u'annotations': {}}]})
        p = agg.to_pandas()

        #assert list(p.columns) == [u'Origin Country', u'Year', u'Exports']
        #assert len(p) == len(agg.tidy['data'])


if __name__ == '__main__':
    unittest.main()
