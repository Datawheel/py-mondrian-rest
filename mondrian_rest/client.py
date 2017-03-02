from operator import itemgetter
from urlparse import urljoin
from itertools import product, ifilter, izip
from functools import reduce as reduce_
import copy

import numpy as np
import pandas as pd
import requests

from mondrian_rest.identifier import Identifier

CUBE_ATTRS = ['name', 'dimensions', 'measures', 'annotations']
BOOL_OPTS = ['nonempty', 'distinct', 'parents']

class Aggregation(object):

    def __init__(self, data, cube, url, agg_params=None):
        self._data = data
        self._cube = cube
        self._agg_params = agg_params
        self.url = url

        self._tidy = None

    @property
    def axes(self):
        return self._data['axes']

    @property
    def measures(self):
        return self.axes[0]['members']

    @property
    def values(self):
        return self._data['values']

    @property
    def axis_dimensions(self):
        return self._data['axis_dimensions']

    @property
    def tidy(self):
        """ Unroll the JSON representation of a result into a
            'tidy' (http://vita.had.co.nz/papers/tidy-data.pdf) dataset. """

        if self._tidy is not None:
            return self._tidy

        data = self._data
        measures = data['axes'][0]['members']
        prod = [izip(e['members'],
                    xrange(len(e['members'])))
                for e in data['axes'][1:]]
        values = data['values']

        def buildRow(cell):
            cidxs = list(reversed([ c[1] for c in cell ]))

            cm = [
                c[0]
                for c in cell
            ]

            mvalues = [ reduce(lambda memo, cur: memo[cur],  # navigate to values[coords]
                               cidxs + [mi],
                               values)
                        for mi, m in enumerate(measures) ]

            return cm + mvalues

        self._tidy = {
            'axes': data['axis_dimensions'][1:],
            'measures': measures,
            'data': [ buildRow(cell) for cell in product(*prod) ]
        }

        return self._tidy

    def to_pandas(self, filter_empty_measures=True):
        tidy = self.tidy
        columns = []
        table = []

        # header row
        if self._agg_params['parents']:
            slices = []
            for dd in tidy['axes']:
                slices.append(dd['level_depth'])
                for ancestor_level in self._cube.dimensions_by_name[dd['name']]['hierarchies'][0]['levels'][1:dd['level_depth']]:
                    columns += ['ID %s' % ancestor_level['caption'], ancestor_level['caption']]
                columns += ['ID %s' % dd['level'], dd['level']]

            # measure names
            columns += [m['caption'] for m in self._agg_params['measures']]

            for row in tidy['data']:
                r = []
                for j, cell in enumerate(row[:len(tidy['axes'])]):
                    for ancestor in reversed(cell['ancestors'][:slices[j]-1]):
                        r += [ancestor['key'], ancestor['caption']]
                    r += [cell['key'], cell['caption']]

                for mvalue in row[len(tidy['axes']):]:
                    r.append(mvalue)

                table.append(r)

        else: # no parents
            for dd in tidy['axes']:
                columns += ['ID %s' % dd['level'], dd['level']]
            # measure names
            columns += [m['caption'] for m in self._agg_params['measures']]

            for row in tidy['data']:
                r = []
                for cell in row[:len(tidy['axes'])]:
                    r += [cell['key'], cell['caption']]

                for mvalue in row[len(tidy['axes']):]:
                    r.append(mvalue)

                table.append(r)

        df = pd.DataFrame(table,
                          columns=columns) \
               .set_index(columns[:-len(self._agg_params['measures'])])

        if filter_empty_measures:
            df = df[reduce_(np.logical_and,
                            [df[msr['name']].notnull()
                            for msr in self.measures])]

        return df


class Cube(object):

    def __init__(self, name, dimensions, measures, annotations, client):
        self.name = name
        self.dimensions = dimensions
        self.measures = measures
        self.annotations = annotations
        self.client = client

        self._dimensions_by_name = {
            d['name']: d for d in self.dimensions
        }

    @property
    def time_dimension(self):
        tds = [d for d in self.dimensions if d['type'] == 'time']
        if len(tds) == 0:
            raise Exception("No time dimension defined in cube %s" % self.name)
        elif len(tds) > 1:
            raise Exception("More than 1 time dimension defined in cube %s" % self.name)
        return tds[0]

    @property
    def std_dimensions(self):
        """ Dict of non-time dimensions keyed by their name """
        return {
            d['name']: d
            for d in self.dimensions
            if d['type'] != 'time'
        }

    @property
    def dimensions_by_name(self):
        """ Dict of dimensions keyed by their name """
        return {
            d['name']: d
            for d in self.dimensions
        }

    @property
    def measures_by_name(self):
        return {
            m['name']: m
            for m in self.measures
        }

    def get_level(self, dimension_name, level_name, hierarchy=0):
        """
        Get level with name `level_name` in dimension with name `dimension_name`
        in hierarchy `hierarchy` (default: 0)
        """
        d = self._dimensions_by_name[dimension_name]
        for l in d['hierarchies'][hierarchy]['levels']:
            if l['name'] == level_name:
                return l

        # raise if not found
        raise ValueError('level with name `%s`` not found' % level_name)

    def get_members(self, dimension_name, level_name):
        return self.client.get_members(
            self.name,
            dimension_name,
            level_name
        )['members']

    def get_aggregation(self, drilldown=[], cut=[], measures=[], **extra_params):

        agg_params = copy.copy(extra_params)

        agg_params['drilldown'] = [
            self.get_level(*[seg.name for seg in Identifier.parse(dd).segments])
            for dd in drilldown
        ]

        agg_params['cut'] = cut

        agg_params['measures'] = [
            self.measures_by_name[m]
            for m in measures
        ]


        return self.client.get_aggregation(self,
                                           agg_params)


class MondrianClient(object):
    def __init__(self, api_base):
        self.api_base = api_base

    def get_cubes(self):
        r = self._request(urljoin(self.api_base, 'cubes')).json()
        return [Cube(*(itemgetter(*CUBE_ATTRS)(c) + (self,))) for c in r['cubes']]

    def get_cube(self, cube_id):
        r = self._request(urljoin(self.api_base, 'cubes/' + cube_id)).json()
        return Cube(*(itemgetter(*CUBE_ATTRS)(r) + (self,)))

    # TODO: validate shape of params
    def get_aggregation(self, cube, params):
        qs_params = {
            bo: 'true' if params.get(bo) else 'false'
            for bo in BOOL_OPTS
        }

        if len(params.get('measures', [])) == 0:
            raise Exception("Must provide at least one measure")

        for m in params['measures']:
            qs_params['measures[]'] = [m['name'] for m in params['measures']]

        if len(params.get('drilldown', [])) > 0:
            qs_params['drilldown[]'] = [l['full_name'] for l in params['drilldown']]

        if len(params.get('cut', [])) > 0:
            qs_params['cut[]'] = params['cut']

        if len(params.get('properties', [])) > 0:
            qs_params['properties[]'] = params['properties']

        if len(params.get('caption', [])) > 0:
            qs_params['caption[]'] = params['caption']

        r = self._request(
                urljoin(self.api_base, 'cubes/%s/aggregate' % cube.name),
                qs_params
            )

        return Aggregation(r.json(), cube, r.url, params)

    def get_members(self, cube_id, dimension, level):
        return self._request(
            urljoin(
                self.api_base,
                'cubes/%s/dimensions/%s/levels/%s/members' % (cube_id, dimension, level)
            )
        ).json()

    def get_member(self, cube, member_full_name):
        raise Exception('Not Implemented')

    def _request(self, url, params=None):
        return requests.get(url, params=params)
