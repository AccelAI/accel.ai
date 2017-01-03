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

from eventlet import patcher
from eventlet.green.httplib import HTTPConnection

requests = patcher.import_patched('requests.__init__')


def http_connect(host, method, path, headers=None):
    conn = HTTPConnection(host)
    conn.path = path
    conn.putrequest(method, path)
    if headers:
        for header, value in headers.iteritems():
            conn.putheader(header, str(value))
    conn.endheaders()
    return conn
