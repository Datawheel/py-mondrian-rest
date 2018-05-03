from functools import reduce as reduce_
try:
    from itertools import product, izip, groupby
except ImportError:
    izip = zip

import numpy as np
import pandas as pd

from mondrian-rest.identifier import Identifier

def parse_properties(properties):
    """
    parse an array of property specifications:

    input: ['ISICrev4.Level 2.Level 2 ES', 'ISICrev4.Level 1.Level 1 ES']
    output: {"ISICrev4"=>{"Level 2"=>["Level 2 ES"], "Level 1"=>["Level 1 ES"]}}
    """

    def reducer(h, it):
        k, v = it
        h[k] = dict(map(lambda jt: (jt[0], map(lambda vv: list(vv)[-1].name, jt[1])),
                        groupby(v, key=lambda s: s.segments[1].name)))
        return h

    return reduce(
        reducer,
        groupby(
            sorted(
                map(Identifier.parse,
                    properties),
                key=lambda p: ".".join(map(lambda s: s.name, p[:2]))
            ),
            key=lambda s: s[0].name
        ),
        {}
    )

def get_props(row, pnames, props, dimensions):

    def reducer(h, row):
        ax_i, member = row
        dname = dimensions[ax_i]['name']
        if props.get(dname):
            mmbr_lvl = dimensions[ax_i]['level']
            for p in props[dname].get(mmbr_lvl, []):
                h[p] = member['properties'][p]
            if member.get('ancestors'):
                for l, p in filter(lambda it: it[0] != mmbr_lvl,
                                   props[dname].items()):
                    anc = next(anc for anc in member['ancestors'] if anc['level_name'] == l)
                    for prop in p:
                        h[prop] = anc['properties'][prop]

        return h

    pvalues = reduce(reducer, enumerate(row), {})

    return map(lambda pn: pvalues[pn], pnames)

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
                    range(len(e['members'])))
                for e in data['axes'][1:]]
        values = data['values']

        def build_row(cell):
            cidxs = list(reversed([ c[1] for c in cell ]))

            cm = [
                c[0]
                for c in cell
            ]

            mvalues = [ reduce_(lambda memo, cur: memo[cur],  # navigate to values[coords]
                               cidxs + [mi],
                               values)
                        for mi, m in enumerate(measures) ]

            return cm + mvalues

        self._tidy = {
            'axes': data['axis_dimensions'][1:],
            'measures': measures,
            'data': [ build_row(cell) for cell in product(*prod) ]
        }

        return self._tidy

    def to_pandas(self, filter_empty_measures=True):
        tidy = self.tidy
        columns = []
        table = []
        properties = self._agg_params.get('properties', [])
        measures = self._agg_params['measures']

        props = parse_properties(properties)
        pnames = [Identifier.parse(i).segments[-1].name for i in properties]

        # header row
        if self._agg_params['parents']:
            slices = []
            for dd in tidy['axes']:
                slices.append(dd['level_depth'])
                for ancestor_level in self._cube.dimensions_by_name[dd['name']]['hierarchies'][0]['levels'][1:dd['level_depth']]:
                    columns += ['ID %s' % ancestor_level['caption'], ancestor_level['caption']]
                columns += ['ID %s' % dd['level'], dd['level']]

            # property names
            columns += pnames

            # measure names
            columns += [m['caption'] for m in measures]

            for row in tidy['data']:
                r = []
                for j, cell in enumerate(row[:len(tidy['axes'])]):
                    for ancestor in reversed(cell['ancestors'][:slices[j]-1]):
                        r += [ancestor['key'], ancestor['caption']]
                    r += [cell['key'], cell['caption']]

                r += get_props(row[:-len(measures)],
                               pnames,
                               props,
                               tidy['axes'])

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

                r += get_props(row[:-len(measures)],
                               pnames,
                               props,
                               tidy['axes'])

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
