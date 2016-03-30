#!/usr/bin/python

import re
import sys
import ast
from collections import namedtuple

# BEGIN_COPYRIGHT
#
# Copyright (C) 2008-2015 SciDB, Inc.
# All Rights Reserved.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT

# See include/array/Coordinate.h
MAX_COORDINATE = (2 ** 62) - 1
MIN_COORDINATE = - MAX_COORDINATE

def parse(schema, strict=True, default_nullable=True):
    """Parse a SciDB schema into lists of attributes and dimensions.

    @description
    Returned attribute and dimension lists are namedtuples whose
    elements can be accessed by Python attribute name.  For an
    attr_list element 'attr', you can access:

        attr.name
        attr.type
        attr.nullable
        attr.default

    For a dim_list element 'dim', you can access:

        dim.name
        dim.lo
        dim.hi
        dim.chunk
        dim.overlap

    If the dimension's high value was specified as '*', dim.hi will be
    MAX_COORDINATE.

    IF the value for chunk size is '?', then dim.chunk will be '?'.

    Attributes became nullable by default in 15.12.  When parsing
    schema strings written by pre-15.12 software, set default_nullable
    to False to preserve the intended semantics.

    @param schema array schema to parse
    @param strict input string must be schema only, no extra cruft
    @param default_nullable default nullability of attributes
    @throws ValueError on malformed schema
    @return attr_list, dim_list

    @note The default value for 'strict' should be flipped to False,
          but right now there is no time to thoroughly test the effect
          on calling scripts.  Setting strict=False will let you parse
          the raw output of a show() operator, which is what you
          typically want to do.
    """
    # Start by cracking schema into attributes and dimensions parts.
    if strict:
        m = re.match(r'\s*<([^>]+)>\s*\[([^\]]+)\]\s*$', schema)
    else:
        m = re.search(r'<([^>]+)>\s*\[([^\]]+)\]', schema)
    if not m:
        raise ValueError("bad schema: '%s'" % schema)
    # Parse attributes...
    Attribute = namedtuple('Attribute', ('name', 'type', 'nullable', 'default'))
    attr_descs = map(str.strip, m.group(1).split(','))
    attr_list = []
    rgx_default = '(default)\s+(.+)$'
    rgx_type = '^[^\s]+'
    for desc in attr_descs:
        nm, ty = map(str.strip, desc.split(':',1))
        if not nm or not ty:
            raise ValueError("bad attribute: '%s'" % desc)
        match = re.search(rgx_type,ty)
        if not match:
            raise ValueError("missing attribute type: '%s'" % desc)
        attr_type = match.group().lower()
        ty = ty[len(attr_type):] # ty is now qualifiers only
        match = re.search(rgx_default,ty,re.MULTILINE | re.DOTALL | re.IGNORECASE)
        default_value = match.group(2) if match else None
        saw_null = 'null' in ty.lower()
        saw_notnull = 'not null' in ' '.join(ty.lower().split())
        if default_nullable:
            nullable = not saw_notnull
        else:
            nullable = saw_null and not saw_notnull
        attr_list.append(Attribute(nm, attr_type, nullable, default_value))
    # Parse dimensions.  Each regex match peels off a full dimension
    # spec from the left of the dimensions part.
    dim_list = []
    Dimension = namedtuple('Dimension', ('name', 'lo', 'hi', 'chunk', 'overlap'))
    rgx = r'\s*([^=\s]+)\s*=\s*(\-{,1}\d+)\s*:\s*(\*|\-{,1}\d+)\s*,\s*(\?|\*|\d+)\s*,\s*(\d+)\s*'
    dims = m.group(2)
    if not dims:
        raise ValueError("schema has no dimensions: '%s'" % schema)
    while dims:
        m = re.match(rgx, dims)
        if not m:
            raise ValueError("bad dimension(s): '%s'" % dims)
        high = MAX_COORDINATE if m.group(3) == '*' else long(m.group(3))
        chunk = m.group(4) if m.group(4) in ('?','*') else long(m.group(4))
        dim_list.append(Dimension(m.group(1), long(m.group(2)), high,
                                  chunk, long(m.group(5))))
        if rgx[0] != ',':
            rgx = ',%s' % rgx   # subsequent matches need inter-group comma
        dims = dims[len(m.group(0)):]
    return attr_list, dim_list

def unparse(attrs, dims, default_nullable=True):
    """Inverse of parse: turn attributes and dimensions into a schema string.

    @param attrs list of Attribute namedtuples as returned by #parse
    @param dims list of Dimension namedtuples as returned by #parse
    @param default_nullable default nullability of attributes
    @returns schema string constructed from given attributes and dimensions
    """
    if default_nullable:
        null = ''
        notnull = ' NOT NULL'
    else:
        null = ' NULL'
        notnull = ''
    return "<{0}>[{1}]".format(
        ','.join(["%s:%s%s%s" % (
                    x.name,
                    x.type,
                    (null if x.nullable else notnull),
                    (' DEFAULT ' + x.default if x.default is not None else '')
                    ) for x in attrs]),
        ','.join(["%s=%s:%s,%s,%s" % (
                    y.name, str(y.lo),
                    ('*' if y.hi >= MAX_COORDINATE else str(y.hi)),
                    str(y.chunk), str(y.overlap))
                  for y in dims]))

def main(args=None):
    if args is None:
        args = sys.argv

    for arg in args[1:]:
        print '----', arg
        alist, dlist = parse(arg)
        for i, attr in enumerate(alist):
            print ' '.join(("Attribute[%d]:" % i, attr.name, attr.type,
                            "*" if attr.nullable else ""))
        for i, dim in enumerate(dlist):
            print ' '.join(map(str, ("Dimension[%d]:" % i, dim.name, dim.lo,
                                     "*" if dim.hi == sys.maxsize else dim.hi,
                                     dim.chunk, dim.overlap)))
        adict = dict([(x.name, x) for x in alist])
        if "foo" in adict:
            print "Foo is such a boring name for an attribute."

    return 0

if __name__ == '__main__':
    sys.exit(main())
