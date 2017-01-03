# Copyright (C) 2015 OpenIO SAS

# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3.0 of the License, or (at your option) any later version.
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
# You should have received a copy of the GNU Lesser General Public
# License along with this library.


import glob
import hashlib
import random
import string
from urllib import quote as _quote
from ConfigParser import SafeConfigParser

import os
import codecs
from eventlet import GreenPool


utf8_decoder = codecs.getdecoder('utf-8')
utf8_encoder = codecs.getencoder('utf-8')


def random_string(length=20):
    chars = string.ascii_letters
    return "".join(random.sample(chars, length))


def quote(value, safe='/'):
    if isinstance(value, unicode):
        (value, _len) = utf8_encoder(value, 'replace')
    (valid_utf8_str, _len) = utf8_decoder(value, 'replace')
    return _quote(valid_utf8_str.encode('utf-8'), safe)


class ContextPool(GreenPool):
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        for coroutine in list(self.coroutines_running):
            coroutine.kill()


def env(*vars, **kwargs):
    """Search for the first defined of possibly many env vars
    Returns the first environment variable defined in vars, or
    returns the default defined in kwargs.
    """
    for v in vars:
        value = os.environ.get(v, None)
        if value:
            return value
    return kwargs.get('default', '')


def name2cid(account, ref):
    h = hashlib.sha256()
    for v in [account, '\0', ref]:
        h.update(v)
    return h.hexdigest()


def load_sds_conf(ns):
    def places():
        yield '/etc/oio/sds.conf'
        for f in glob.glob('/etc/oio/sds.conf.d/*'):
            yield f
        yield os.path.expanduser('~/.oio/sds.conf')

    parser = SafeConfigParser({})
    success = parser.read(places())
    if success and parser.has_section(ns):
        return dict(parser.items(ns))
    else:
        return None
