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


from eventlet import Timeout


class CommandError(Exception):
    pass


class OioException(Exception):
    pass


class PreconditionFailed(OioException):
    pass


class EtagMismatch(OioException):
    pass


class MissingContentLength(OioException):
    pass


class MissingData(OioException):
    pass


class MissingName(OioException):
    pass


class FileNotFound(OioException):
    pass


class ContainerNotEmpty(OioException):
    pass


class NoSuchContainer(OioException):
    pass


class NoSuchObject(OioException):
    pass


class NoSuchReference(OioException):
    pass


class ClientReadTimeout(Timeout):
    pass


class ClientException(OioException):
    def __init__(self, http_status, status=None, message=None):
        self.http_status = http_status
        self.message = message or 'n/a'
        self.status = status

    def __str__(self):
        fstring = "%s (HTTP %s)" % (self.message, self.http_status)
        if self.status:
            fstring += " (STATUS %s)" % self.status
        return fstring


class OioTimeout(OioException):
    pass


class ConnectionTimeout(Timeout):
    pass


class SourceReadError(OioException):
    pass


class ChunkWriteTimeout(Timeout):
    pass


class ChunkReadTimeout(Timeout):
    pass


class NotFound(ClientException):
    pass


class Conflict(ClientException):
    pass


_http_status_map = {404: NotFound, 409: Conflict}


def from_response(response, body):
    http_status = response.status_code
    cls = _http_status_map.get(http_status, ClientException)
    if body:
        message = "n/a"
        status = None
        if isinstance(body, dict):
            message = body.get("message")
            status = body.get("status")
        else:
            message = body
        return cls(http_status=http_status, message=message, status=status)
    else:
        return cls(http_status=http_status)
