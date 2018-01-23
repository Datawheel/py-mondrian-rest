# ported from olap4j
# https://github.com/olap4j/olap4j/blob/b8eccd85753ffddb66c9d8b7c2cd7de2bd510ce0/src/org/olap4j/impl/IdentifierParser.java

STATE = type('Enum',
             (),
             dict(START=0, BEFORE_SEG=1, IN_BRACKET_SEG=2, AFTER_SEG=3, IN_SEG=4))

SYNTAX = type('Enum',
             (),
             dict(NAME=0, FIRST_KEY=1, NEXT_KEY=2))

QUOTING = type('Enum',
               (),
               dict(UNQUOTED=0, QUOTED=1, KEY=2))

class Segment(object):
    def __init__(self, name, quoting):
        self._name = name
        self._quoting = quoting

    @property
    def name(self):
        if self._quoting == QUOTING.QUOTED:
            # unquote
            return self._name[1:-1]
        else:
            return self._name

    def __repr__(self):
        return "<Segment \'%s\' quoting=%d>" % (self._name, self._quoting)

class Identifier(object):
    def __init__(self):
        self._segments = []
        self._subsegments = []

    def _flush_subsegments(self):
        if len(self._subsegments) > 0:
            self._segments.append(
                Segment(
                    '&' + '&'.join([ss.name for ss in self._subsegments]),
                    QUOTING.KEY
                )
            )
            self._subsegments = []

    def add_segment(self, string, quoting, syntax):

        segment = Segment(string, quoting)

        if syntax != SYNTAX.NEXT_KEY:
            self._flush_subsegments()

        if syntax == SYNTAX.NAME:
            self._segments.append(segment)
        else:
            self._subsegments.append(segment)

    def __str__(self):
        self._flush_subsegments()
        return '.'.join([s.name for s in self._segments])

    def __getitem__(self, i):
        return self._segments[i]

    @property
    def segments(self):
        self._flush_subsegments()
        return self._segments

    @classmethod
    def parse(cls, string):
        k = len(string)

        pstate = { 'i': 0, 'state': STATE.START, 'start': 0, 'syntax': SYNTAX.NAME }

        ident = cls()

        #ipdb.set_trace()
        def inner():

            c = string[pstate['i']]

            if pstate['state'] in (STATE.START, STATE.BEFORE_SEG):
                if c == '[':
                    pstate.update({
                        'i': pstate['i'] + 1,
                        'start': pstate['i'],
                        'state': STATE.IN_BRACKET_SEG
                    })
                elif c == ' ':
                    pstate['i'] += 1
                elif c in (',', '}', '\0'):
                    return
                elif c == '.':
                    raise ValueError("Unexpected '.'")
                elif c == '&':
                    pstate['i'] += 1
                    if pstate['syntax'] != SYNTAX.NAME:
                        raise ValueError("Unexpected '&'")
                    pstate['syntax'] = SYNTAX.FIRST_KEY
                else:
                    pstate.update({
                        'state': STATE.IN_SEG,
                        'start': pstate['i']
                    })

            elif pstate['state'] == STATE.IN_SEG:
                if c in (',', ')', '}', '\0'):
                    ident.add_segment(string[pstate['start']:pstate['i']].strip(),
                                       QUOTING.UNQUOTED,
                                       pstate['syntax'])
                    pstate['state'] = STATE.AFTER_SEG
                    return
                elif c == '.':
                    ident.add_segment(string[pstate['start']:pstate['i']].strip(),
                                       QUOTING.UNQUOTED,
                                       pstate['syntax'])
                    pstate.update({
                        'syntax': SYNTAX.NAME,
                        'state': STATE.BEFORE_SEG,
                        'i': pstate['i'] + 1
                    })
                elif c == '&':
                    ident.add_segment(string[pstate['start']:pstate['i']].strip(),
                                       QUOTING.UNQUOTED,
                                       pstate['syntax'])
                    pstate.update({
                        'syntax': SYNTAX.NEXT_KEY,
                        'state': STATE.BEFORE_SEG,
                        'i': pstate['i'] + 1
                    })

                else:
                    pstate['i'] += 1

            elif pstate['state'] == STATE.IN_BRACKET_SEG:
                if c == '\0':
                   raise ValueError("Expected ']', in member identifier %s" % string)
                if c == ']':
                    if string[pstate['i']+1] == ']':
                        pstate['i'] += 2
                        # fall through
                    else:
                        ident.add_segment(string[pstate['start']:pstate['i']+1].strip().replace(']]', ']'),
                                           QUOTING.QUOTED,
                                           pstate['syntax'])
                        pstate.update({
                            'i': pstate['i'] + 1,
                            'state': STATE.AFTER_SEG
                        })
                else:
                    pstate['i'] += 1
            elif pstate['state'] == STATE.AFTER_SEG:
                if c == ' ':
                    pstate['i'] += 1
                elif c == '.':
                    pstate.update({
                        'state': STATE.BEFORE_SEG,
                        'syntax': SYNTAX.NAME,
                        'i': pstate['i'] + 1
                    })
                elif c == '&':
                    pstate['state']= STATE.BEFORE_SEG
                    # Roll the syntax - NAME=>FIRST_KEY=>NEXT_KEY=>NEXT_KEY...
                    if pstate['syntax'] == SYNTAX.NAME:
                        pstate['syntax'] = SYNTAX.FIRST_KEY
                    else:
                        pstate['syntax'] = SYNTAX.NEXT_KEY
                    pstate['i'] += 1
                elif c == '\0':
                    pstate['i'] += 1
                    return
            else:
                raise AssertionError("Unexpected state: " + str(pstate))

        # this sucks, but lets me keep the algorithm
        # close to the original
        string += '\0'
        while pstate['i'] < k + 1:
            inner()

        if pstate['state'] == STATE.START:
            pass
        elif pstate['state'] == STATE.BEFORE_SEG:
            raise ValueError("Expected identifier after '.', in member identifier %s" % string)
        elif pstate['state'] == STATE.IN_BRACKET_SEG:
            ValueError("Expected ']' in member identifier %s" % string)

        return ident


if __name__ == '__main__':
    # TODO: port these tests
    # https://github.com/olap4j/olap4j/blob/b8eccd85753ffddb66c9d8b7c2cd7de2bd510ce0/testsrc/org/olap4j/impl/Olap4jUtilTest.java#L358
    print(Identifier.parse('[Date].[Year].[2010]').segments)
    print()
    print(Identifier.parse("[string].[with].[a [bracket]] in it]").segments)
    print()
    print(Identifier.parse("Time.1997.[Q3]").segments)
    print()
    print(Identifier.parse("[Customers].[City].&[San Francisco]&CA&USA.&[cust1234]").segments)
    print()
    print(Identifier.parse("[Date].Year").segments)
    print()
