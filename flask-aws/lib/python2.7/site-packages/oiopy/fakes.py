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


from eventlet import Timeout, sleep

from oiopy.directory import DirectoryAPI
from oiopy.object_storage import ObjectStorageAPI
from oiopy.http import requests
from contextlib import contextmanager
import oiopy


class FakeAPI(object):
    def __init__(self, *args, **kwargs):
        pass


class FakeResponse(requests.Response):
    pass


class FakeSession(requests.Session):
    pass


@contextmanager
def set_http_connect(*args, **kwargs):
    old = oiopy.object_storage.http_connect

    new = fake_http_connect(*args, **kwargs)
    try:
        oiopy.object_storage.http_connect = new
        yield new
        unused_status = list(new.status_iter)
        if unused_status:
            raise AssertionError('unused status %r' % unused_status)

    finally:
        oiopy.object_storage.http_connect = old


def fake_http_connect(*status_iter, **kwargs):
    class FakeConn(object):
        def __init__(self, status, body=''):
            if isinstance(status, (Exception, Timeout)):
                raise status
            if isinstance(status, tuple):
                self.expect_status, self.status = status
            else:
                self.expect_status, self.status = (None, status)

            self.body = body

        def getresponse(self, amt=None):
            if isinstance(self.status, (Exception, Timeout)):
                raise self.status
            return self

        def getheaders(self):
            pass

        def read(self, size=None):
            resp = self.body[:size]
            self.body = self.body[size:]
            return resp

        def send(self, data):
            pass

        def close(self):
            pass

    body = kwargs.get('body', None)
    status_iter = iter(status_iter)

    def connect(*args, **ckwargs):
        if kwargs.get("slow_connect", False):
            sleep(1)
        status = status_iter.next()
        return FakeConn(status, body=body)

    connect.status_iter = status_iter

    return connect


def fake_http_request(*status_iter, **kwargs):
    status_iter = iter(status_iter)

    def request(*args, **ckwargs):
        status = status_iter.next()
        body = None
        headers = None
        if isinstance(status, tuple):
            if len(status) is 3:
                status_code, body, headers = status
            else:
                status_code, body = status
        else:
            status_code = status
        if 'callback' in kwargs:
            kwargs['callback'](*args, **ckwargs)
        resp = FakeResponse()
        resp.status_code = status_code
        resp._content = body or ''
        resp.headers = headers
        return resp

    request.status_iter = status_iter
    return request


class FakeTimeoutStream(object):
    def __init__(self, time):
        self.time = time

    def read(self, size):
        sleep(self.time)


class FakeStorageAPI(ObjectStorageAPI):
    pass


class FakeDirectoryAPI(DirectoryAPI):
    pass
