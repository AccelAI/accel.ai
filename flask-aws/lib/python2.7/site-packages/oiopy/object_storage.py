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


from cStringIO import StringIO
from functools import wraps
import hashlib
import json
import os
from urlparse import urlparse
from urllib import unquote

import requests
from eventlet import Timeout
from eventlet.greenpool import GreenPile
from eventlet.queue import Queue
from requests.exceptions import RequestException

from oiopy.api import API
from oiopy.directory import DirectoryAPI
from oiopy import exceptions as exc
from oiopy import utils
from oiopy.exceptions import ConnectionTimeout, ChunkReadTimeout, \
    ChunkWriteTimeout, ClientReadTimeout, SourceReadError
from oiopy.http import http_connect


CONTAINER_METADATA_PREFIX = "x-oio-container-meta-"
OBJECT_METADATA_PREFIX = "x-oio-content-meta-"
CHUNK_METADATA_PREFIX = "x-oio-chunk-meta-"

CONTAINER_USER_METADATA_PREFIX = CONTAINER_METADATA_PREFIX + 'user-'

WRITE_CHUNK_SIZE = 65536
READ_CHUNK_SIZE = 65536

CONNECTION_TIMEOUT = 2
CHUNK_TIMEOUT = 3
CLIENT_TIMEOUT = 3

PUT_QUEUE_DEPTH = 10

container_headers = {
    "size": "%ssys-m2-usage" % CONTAINER_METADATA_PREFIX,
    "ns": "%ssys-ns" % CONTAINER_METADATA_PREFIX
}

object_headers = {
    "name": "%sname" % OBJECT_METADATA_PREFIX,
    "id": "%sid" % OBJECT_METADATA_PREFIX,
    "policy": "%spolicy" % OBJECT_METADATA_PREFIX,
    "version": "%sversion" % OBJECT_METADATA_PREFIX,
    "size": "%slength" % OBJECT_METADATA_PREFIX,
    "ctime": "%sctime" % OBJECT_METADATA_PREFIX,
    "hash": "%shash" % OBJECT_METADATA_PREFIX,
    "mime_type": "%smime-type" % OBJECT_METADATA_PREFIX,
    "chunk_method": "%schunk-method" % OBJECT_METADATA_PREFIX
}

chunk_headers = {
    "container_id": "%scontainer-id" % CHUNK_METADATA_PREFIX,
    "chunk_id": "%schunk-id" % CHUNK_METADATA_PREFIX,
    "chunk_hash": "%schunk-hash" % CHUNK_METADATA_PREFIX,
    "chunk_size": "%schunk-size" % CHUNK_METADATA_PREFIX,
    "chunk_pos": "%schunk-pos" % CHUNK_METADATA_PREFIX,
    "content_size": "%scontent-size" % CHUNK_METADATA_PREFIX,
    "content_path": "%scontent-path" % CHUNK_METADATA_PREFIX,
    "content_chunksnb": "%scontent-chunksnb" % CHUNK_METADATA_PREFIX,
    "content_hash": "%scontent-hash" % CHUNK_METADATA_PREFIX,
    "content_id": "%scontent-id" % CHUNK_METADATA_PREFIX,
    "content_version": "%scontent-version" % CHUNK_METADATA_PREFIX,
    "content_policy": "%scontent-storage-policy" % CHUNK_METADATA_PREFIX,
    "content_mimetype": "%scontent-mime-type" % CHUNK_METADATA_PREFIX,
    "content_chunkmethod": "%scontent-chunk-method" % CHUNK_METADATA_PREFIX,
}


def handle_container_not_found(fnc):
    @wraps(fnc)
    def _wrapped(self, account, container, *args, **kwargs):
        try:
            return fnc(self, account, container, *args, **kwargs)
        except exc.NotFound as e:
            e.message = "Container '%s' does not exist." % container
            raise exc.NoSuchContainer(e)

    return _wrapped


def handle_object_not_found(fnc):
    @wraps(fnc)
    def _wrapped(self, account, container, obj, *args, **kwargs):
        try:
            return fnc(self, account, container, obj, *args, **kwargs)
        except exc.NotFound as e:
            e.message = "Object '%s' does not exist." % obj
            raise exc.NoSuchObject(e)

    return _wrapped


def _sort_chunks(raw_chunks, rain_security):
    chunks = dict()
    for chunk in raw_chunks:
        raw_position = chunk["pos"].split(".")
        position = int(raw_position[0])
        if rain_security:
            subposition = raw_position[1]
        if position in chunks:
            if rain_security:
                chunks[position][subposition] = chunk
            else:
                chunks[position].append(chunk)
        else:
            if rain_security:
                chunks[position] = dict()
                chunks[position][subposition] = chunk
            else:
                chunks[position] = [chunk]
    if not rain_security:
        for clist in chunks.itervalues():
            clist.sort(lambda x, y: cmp(x.get("score", 0), y.get("score", 0)),
                       reverse=True)
    return chunks


def _make_object_metadata(headers):
    meta = {}
    props = {}

    prefix = OBJECT_METADATA_PREFIX

    for k, v in headers.iteritems():
        k = k.lower()
        if k.startswith(prefix):
            key = k.replace(prefix, "")
            # TODO temporary workaround
            if key.startswith('x-'):
                props[key[2:]] = v
            else:
                meta[key] = v
    meta['properties'] = props
    return meta


class ObjectStorageAPI(API):
    """
    The Object Storage API
    """

    def __init__(self, namespace, endpoint, **kwargs):
        endpoint_v3 = '/'.join([endpoint.rstrip('/'), 'v3.0'])
        super(ObjectStorageAPI, self).__init__(endpoint=endpoint_v3, **kwargs)
        self.directory = DirectoryAPI(
            namespace,
            endpoint,
            session=self.session
        )
        self.namespace = namespace

    def account_create(self, account, headers=None):
        uri = '/v1.0/account/create'
        account_id = utils.quote(account, '')
        params = {'id': account_id}
        resp, resp_body = self._account_request('PUT', uri, params=params,
                                                headers=headers)
        created = (resp.status_code == 201)
        return created

    def account_delete(self, account, headers=None):
        uri = '/v1.0/account/delete'
        account_id = utils.quote(account, '')
        params = {'id': account_id}
        resp, resp_body = self._account_request('POST', uri, params=params,
                                                headers=headers)

    def account_show(self, account, headers=None):
        uri = "/v1.0/account/show"
        account_id = utils.quote(account, '')
        params = {'id': account_id}
        resp, resp_body = self._account_request('GET', uri, params=params,
                                                headers=headers)
        return resp_body

    def account_update(self, account, metadata, to_delete=None, headers=None):
        uri = "/v1.0/account/update"
        account_id = utils.quote(account, '')
        params = {'id': account_id}
        data = json.dumps({"metadata": metadata, "to_delete": to_delete})
        resp, resp_body = self._account_request('POST', uri, params=params,
                                                data=data, headers=headers)

    def account_set_properties(self, account, properties, headers=None):
        self.account_update(account, properties, headers=headers)

    def account_del_properties(self, account, properties, headers=None):
        self.account_update(account, None, properties, headers=headers)

    def container_create(self, account, container, metadata=None,
                         headers=None):
        uri = self._make_uri('container/create')
        params = self._make_params(account, container)

        headers = headers or {}
        headers['x-oio-action-mode'] = 'autocreate'
        if metadata:
            headers_meta = {}
            for k, v in metadata.iteritems():
                headers_meta['%suser-%s' % (CONTAINER_METADATA_PREFIX, k)] = v
            headers.update(headers_meta)
        resp, resp_body = self._request(
            'POST', uri, params=params, headers=headers)
        if resp.status_code not in (204, 201):
            raise exc.from_response(resp, resp_body)
        if resp.status_code == 201:
            return False
        else:
            return True

    @handle_container_not_found
    def container_delete(self, account, container, headers=None):
        uri = self._make_uri('container/destroy')
        params = self._make_params(account, container)
        try:
            resp, resp_body = self._request(
                'POST', uri, params=params, headers=headers)
        except exc.Conflict as e:
            raise exc.ContainerNotEmpty(e)

    def container_list(self, account, limit=None, marker=None,
                       end_marker=None, prefix=None, delimiter=None,
                       headers=None):
        uri = "v1.0/account/containers"
        account_id = utils.quote(account, '')
        params = {"id": account_id, "limit": limit, "marker": marker,
                  "delimiter": delimiter, "prefix": prefix,
                  "end_marker": end_marker}

        resp, resp_body = self._account_request(
            'GET', uri, params=params, headers=headers)
        listing = resp_body['listing']
        del resp_body['listing']
        return listing, resp_body

    @handle_container_not_found
    def container_show(self, account, container, headers=None):
        uri = self._make_uri('container/get_properties')
        params = self._make_params(account, container)
        resp, resp_body = self._request(
            'POST', uri, params=params, headers=headers)
        return resp_body

    def container_update(self, account, container, metadata, clear=False,
                         headers=None):
        if not metadata:
            self.container_del_properties(
                account, container, [], headers=headers)
        else:
            self.container_set_properties(
                account, container, metadata, clear, headers=headers)

    @handle_container_not_found
    def container_set_properties(self, account, container, properties,
                                 clear=False, headers=None):
        params = self._make_params(account, container)

        if clear:
            params.update({'flush': 1})

        uri = self._make_uri('container/set_properties')

        resp, resp_body = self._request(
            'POST', uri, data=json.dumps(properties), params=params,
            headers=headers)

    @handle_container_not_found
    def container_del_properties(self, account, container, properties,
                                 headers=None):
        params = self._make_params(account, container)

        uri = self._make_uri('container/del_properties')

        resp, resp_body = self._request(
            'POST', uri, data=json.dumps(properties), params=params,
            headers=headers)

    @handle_container_not_found
    def object_create(self, account, container, file_or_path=None, data=None,
                      etag=None, obj_name=None, content_type=None,
                      content_encoding=None, content_length=None,
                      metadata=None, policy=None, headers=None):
        if (data, file_or_path) == (None, None):
            raise exc.MissingData()
        src = data if data is not None else file_or_path
        if src is file_or_path:
            if isinstance(file_or_path, basestring):
                if not os.path.exists(file_or_path):
                    raise exc.FileNotFound("File '%s' not found." %
                                           file_or_path)
                file_name = os.path.basename(file_or_path)
            else:
                try:
                    file_name = os.path.basename(file_or_path.name)
                except AttributeError:
                    file_name = None
            obj_name = obj_name or file_name
        if not obj_name:
            raise exc.MissingName(
                "No name for the object has been specified"
            )

        if isinstance(data, basestring):
            content_length = len(data)

        if content_length is None:
            raise exc.MissingContentLength()

        sysmeta = {'mime_type': content_type,
                   'content_encoding': content_encoding,
                   'content_length': content_length,
                   'etag': etag}

        if src is data:
            return self._object_create(
                account, container, obj_name, StringIO(data), sysmeta,
                metadata=metadata, policy=policy, headers=headers)
        elif hasattr(file_or_path, "read"):
            return self._object_create(
                account, container, obj_name, src, sysmeta, metadata=metadata,
                policy=policy, headers=headers)
        else:
            with open(file_or_path, "rb") as f:
                return self._object_create(
                    account, container, obj_name, f, sysmeta,
                    metadata=metadata, policy=policy, headers=headers)

    @handle_object_not_found
    def object_delete(self, account, container, obj, headers=None):
        uri = self._make_uri('content/delete')
        params = self._make_params(account, container, obj)
        resp, resp_body = self._request(
            'POST', uri, params=params, headers=headers)

    @handle_container_not_found
    def object_list(self, account, container, limit=None, marker=None,
                    delimiter=None, prefix=None, end_marker=None,
                    include_metadata=False, headers=None):
        uri = self._make_uri('container/list')
        params = self._make_params(account, container)
        d = {"max": limit,
             "marker": marker,
             "delimiter": delimiter,
             "prefix": prefix,
             "end_marker": end_marker}
        params.update(d)

        resp, resp_body = self._request(
            'GET', uri, params=params, headers=headers)

        if include_metadata:
            meta = {}
            for k, v in resp.headers.iteritems():
                if k.lower().startswith(CONTAINER_USER_METADATA_PREFIX):
                    meta[k[len(CONTAINER_USER_METADATA_PREFIX):]] = unquote(v)
            return meta, resp_body

        return resp_body

    @handle_object_not_found
    def object_analyze(self, account, container, obj, headers=None):
        uri = self._make_uri('content/show')
        params = self._make_params(account, container, obj)
        resp, resp_body = self._request(
            'GET', uri, params=params, headers=headers)
        meta = _make_object_metadata(resp.headers)
        resp_body.sort(lambda x, y: cmp(x.get("score", 0), y.get("score", 0)),
                       reverse=True)
        return meta, resp_body

    def object_fetch(self, account, container, obj, size=None, offset=0,
                     headers=None):
        meta, raw_chunks = self.object_analyze(
            account, container, obj, headers=headers)
        rain_security = len(raw_chunks[0]["pos"].split(".")) == 2
        chunks = _sort_chunks(raw_chunks, rain_security)
        if rain_security:
            stream = self._fetch_stream_rain(
                meta, chunks, size, offset, headers=headers)
        else:
            stream = self._fetch_stream(
                meta, chunks, size, offset, headers=headers)
        return meta, stream

    @handle_object_not_found
    def object_show(self, account, container, obj, headers=None):
        uri = self._make_uri('content/get_properties')
        params = self._make_params(account, container, obj)
        resp, resp_body = self._request(
            'POST', uri, params=params, headers=headers)

        meta = _make_object_metadata(resp.headers)
        meta['properties'] = resp_body
        return meta

    def object_update(self, account, container, obj, metadata, clear=False,
                      headers=None):
        if clear:
            self.object_del_properties(
                account, container, obj, [], headers=headers)
        if metadata:
            self.object_set_properties(
                account, container, obj, metadata, headers=headers)

    @handle_object_not_found
    def object_set_properties(self, account, container, obj, properties,
                              clear=False, headers=None):
        params = self._make_params(account, container, obj)
        if clear:
            params.update({'flush': 1})
        uri = self._make_uri('content/set_properties')
        resp, resp_body = self._request(
            'POST', uri, data=json.dumps(properties), params=params,
            headers=headers)

    @handle_object_not_found
    def object_del_properties(self, account, container, obj, properties,
                              headers=None):
        params = self._make_params(account, container, obj)
        uri = self._make_uri('content/del_properties')
        resp, resp_body = self._request(
            'POST', uri, data=json.dumps(properties), params=params,
            headers=headers)

    def _make_uri(self, action):
        uri = "%s/%s" % (self.namespace, action)
        return uri

    def _make_params(self, account, ref, obj=None):
        params = {'acct': account,
                  'ref': ref}
        if obj:
            params.update({'path': obj})
        return params

    def _get_service_url(self, srv_type):
        uri = self._make_uri('lb/choose')
        params = {'pool': srv_type}
        resp, resp_body = self._request('GET', uri, params=params)
        if resp.status_code == 200:
            instance_info = resp_body[0]
            return 'http://%s/' % instance_info['addr']
        else:
            raise exc.ClientException(
                "could not find account instance url"
            )

    def _account_request(self, method, uri, **kwargs):
        account_url = self._get_service_url('account')
        resp, resp_body = self._request(method, uri, endpoint=account_url,
                                        **kwargs)
        return resp, resp_body

    def _content_prepare(self, account, container, obj_name, size,
                         policy=None, headers=None):
        uri = self._make_uri('content/prepare')
        params = self._make_params(account, container, obj_name)
        args = {'size': size}
        if policy:
            args['policy'] = policy
        headers = headers or {}
        headers['x-oio-action-mode'] = 'autocreate'
        resp, resp_body = self._request(
            'POST', uri, data=json.dumps(args), params=params,
            headers=headers)
        return resp.headers, resp_body

    def _content_create(self, account, container, obj_name, final_chunks,
                        headers=None):
        uri = self._make_uri('content/create')
        params = self._make_params(account, container, obj_name)
        data = json.dumps(final_chunks)
        resp, resp_body = self._request(
            'POST', uri, data=data, params=params, headers=headers)
        return resp.headers, resp_body

    def _object_create(self, account, container, obj_name, src,
                       sysmeta, metadata=None, policy=None, headers=None):
        meta, raw_chunks = self._content_prepare(
            account, container, obj_name, sysmeta['content_length'],
            policy=policy, headers=headers)

        sysmeta['id'] = meta[object_headers['id']]
        sysmeta['version'] = meta[object_headers['version']]
        sysmeta['policy'] = meta[object_headers['policy']]
        sysmeta['mime_type'] = meta[object_headers['mime_type']]
        sysmeta['chunk_method'] = meta[object_headers['chunk_method']]

        rain_security = len(raw_chunks[0]["pos"].split(".")) == 2

        chunks = _sort_chunks(raw_chunks, rain_security)
        if rain_security:
            final_chunks, bytes_transferred, content_checksum = \
                self._put_stream_rain(account, container, obj_name,
                                      src, sysmeta, chunks, headers=headers)
        else:
            final_chunks, bytes_transferred, content_checksum = \
                self._put_stream(account, container, obj_name, src,
                                 sysmeta, chunks, headers=headers)

        etag = sysmeta['etag']
        if etag and etag.lower() != content_checksum.lower():
            raise exc.EtagMismatch(
                "given etag %s != computed %s" % (etag, content_checksum))
        sysmeta['etag'] = content_checksum

        hdrs = {}
        hdrs[object_headers['size']] = bytes_transferred
        hdrs[object_headers['hash']] = sysmeta['etag']
        hdrs[object_headers['version']] = sysmeta['version']
        hdrs[object_headers['id']] = sysmeta['id']
        hdrs[object_headers['policy']] = sysmeta['policy']
        hdrs[object_headers['mime_type']] = sysmeta['mime_type']
        hdrs[object_headers['chunk_method']] = sysmeta['chunk_method']

        if metadata:
            for k, v in metadata.iteritems():
                hdrs['%sx-%s' % (OBJECT_METADATA_PREFIX, k)] = v

        m, body = self._content_create(account, container, obj_name,
                                       final_chunks, headers=hdrs)
        return final_chunks, bytes_transferred, content_checksum

    def _put_stream(self, account, container, obj_name, src, sysmeta, chunks,
                    headers=None):
        global_checksum = hashlib.md5()
        total_bytes_transferred = 0
        content_chunks = []

        def _connect_put(chunk):
            raw_url = chunk["url"]
            parsed = urlparse(raw_url)
            try:
                chunk_path = parsed.path.split('/')[-1]
                hdrs = {}
                hdrs["transfer-encoding"] = "chunked"
                hdrs[chunk_headers["content_id"]] = sysmeta['id']
                hdrs[chunk_headers["content_version"]] = sysmeta['version']
                hdrs[chunk_headers["content_path"]] = utils.quote(obj_name)
                hdrs[chunk_headers["content_size"]] = sysmeta['content_length']
                hdrs[chunk_headers["content_chunkmethod"]] = \
                    sysmeta['chunk_method']
                hdrs[chunk_headers["content_mimetype"]] = sysmeta['mime_type']
                hdrs[chunk_headers["content_policy"]] = sysmeta['policy']
                hdrs[chunk_headers["content_chunksnb"]] = len(chunks)
                hdrs[chunk_headers["container_id"]] = \
                    utils.name2cid(account, container)
                hdrs[chunk_headers["chunk_pos"]] = chunk["pos"]
                hdrs[chunk_headers["chunk_id"]] = chunk_path
                with ConnectionTimeout(CONNECTION_TIMEOUT):
                    conn = http_connect(
                        parsed.netloc, 'PUT', parsed.path, hdrs)
                    conn.chunk = chunk
                return conn
            except (Exception, Timeout):
                pass

        def _send_data(conn):
            while True:
                data = conn.queue.get()
                if not conn.failed:
                    try:
                        with ChunkWriteTimeout(CHUNK_TIMEOUT):
                            conn.send(data)
                    except (Exception, ChunkWriteTimeout):
                        conn.failed = True
                conn.queue.task_done()

        for pos in range(len(chunks)):
            current_chunks = chunks[pos]

            pile = GreenPile(len(current_chunks))

            for current_chunk in current_chunks:
                pile.spawn(_connect_put, current_chunk)

            conns = [conn for conn in pile if conn]

            min_conns = 1

            if len(conns) < min_conns:
                raise exc.OioException("RAWX connection failure")

            bytes_transferred = 0
            total_size = current_chunks[0]["size"]
            chunk_checksum = hashlib.md5()
            try:
                with utils.ContextPool(len(current_chunks)) as pool:
                    for conn in conns:
                        conn.failed = False
                        conn.queue = Queue(PUT_QUEUE_DEPTH)
                        pool.spawn(_send_data, conn)

                    while True:
                        remaining_bytes = total_size - bytes_transferred
                        if WRITE_CHUNK_SIZE < remaining_bytes:
                            read_size = WRITE_CHUNK_SIZE
                        else:
                            read_size = remaining_bytes
                        with ClientReadTimeout(CLIENT_TIMEOUT):
                            try:
                                data = src.read(read_size)
                            except (ValueError, IOError) as e:
                                raise SourceReadError(str(e))
                            if len(data) == 0:
                                for conn in conns:
                                    conn.queue.put('0\r\n\r\n')
                                break
                        chunk_checksum.update(data)
                        global_checksum.update(data)
                        bytes_transferred += len(data)
                        for conn in conns:
                            if not conn.failed:
                                conn.queue.put('%x\r\n%s\r\n' % (len(data),
                                                                 data))
                            else:
                                conns.remove(conn)

                        if len(conns) < min_conns:
                            raise exc.OioException("RAWX write failure")

                    for conn in conns:
                        if conn.queue.unfinished_tasks:
                            conn.queue.join()

                conns = [conn for conn in conns if not conn.failed]

            except SourceReadError:
                raise
            except ClientReadTimeout:
                raise
            except Timeout as e:
                raise exc.OioTimeout(str(e))
            except Exception as e:
                raise exc.OioException(
                    "Exception during chunk write %s" % str(e))

            final_chunks = []
            for conn in conns:
                resp = conn.getresponse(True)
                if resp.status in (200, 201):
                    conn.chunk["size"] = bytes_transferred
                    final_chunks.append(conn.chunk)
                conn.close()
            if len(final_chunks) < min_conns:
                raise exc.OioException("RAWX write failure")

            checksum = chunk_checksum.hexdigest()
            for chunk in final_chunks:
                chunk["hash"] = checksum
            content_chunks += final_chunks
            total_bytes_transferred += bytes_transferred

        content_checksum = global_checksum.hexdigest()

        return content_chunks, total_bytes_transferred, content_checksum

    def _fetch_stream(self, meta, chunks, size, offset, headers=None):
        current_offset = 0
        total_bytes = 0
        if size is None:
            size = int(meta["length"])

        for pos in range(len(chunks)):
            chunk_size = int(chunks[pos][0]["size"])
            if total_bytes >= size:
                break
            if current_offset + chunk_size > offset:
                if current_offset < offset:
                    _offset = offset - current_offset
                else:
                    _offset = 0
                if chunk_size + total_bytes > size:
                    _size = size - total_bytes
                else:
                    _size = chunk_size

                handler = ChunkDownloadHandler(chunks[pos],
                                               _size, _offset)
                stream = handler.get_stream()
                if not stream:
                    raise exc.OioException("Error while downloading")
                for s in stream:
                    total_bytes += len(s)
                    yield s
            current_offset += chunk_size

    def _put_stream_rain(self, account, container, obj_name, src, sysmeta,
                         chunks, headers=None):
        global_checksum = hashlib.md5()
        total_bytes_transferred = 0
        content_chunks = []
        content_length = sysmeta['content_length']

        def _encode_rawxlist(chunks_at_pos):
            res_chunks = []
            for subpos, c in chunks_at_pos.iteritems():
                host = c['url'].split('/')[2]
                chunk_id = c['url'].split('/')[-1]
                res_chunks.append("%s/%s" % (host, chunk_id))
            return '|'.join(res_chunks)

        def _limit_stream(stream, size):
            read_size = 0
            while read_size < size:
                to_read = size - read_size
                if to_read > WRITE_CHUNK_SIZE:
                    to_read = WRITE_CHUNK_SIZE
                data = stream.read(to_read)
                global_checksum.update(data)
                read_size += to_read
                yield data

        def _decode_chunklist(chunklist):
            res = []
            for c in chunklist.split(';'):
                pos, url, size, hash = c.split('|')
                res.append({
                    "url": "http://%s" % url,
                    "pos": pos,
                    "size": int(size),
                    "hash": hash
                })
            return res

        for pos in xrange(len(chunks)):
            rainx_url = self._get_service_url("rainx")

            chunk_size = chunks[pos][str(0)]['size']
            remaining_bytes = content_length - total_bytes_transferred
            if chunk_size > remaining_bytes:
                chunk_size = remaining_bytes

            headers = {}
            headers["X-oio-chunk-meta-content-storage-policy"] = \
                sysmeta['policy']
            headers["X-oio-chunk-meta-rawxlist"] = \
                _encode_rawxlist(chunks[pos])
            headers[chunk_headers["content_id"]] = sysmeta['id']
            headers[chunk_headers["content_version"]] = sysmeta['version']
            headers[chunk_headers["content_path"]] = utils.quote(obj_name)
            headers[chunk_headers["content_size"]] = sysmeta['content_length']
            headers[chunk_headers["content_chunksnb"]] = len(chunks)
            headers[chunk_headers["container_id"]] = \
                utils.name2cid(account, container)
            headers[chunk_headers["chunk_pos"]] = pos
            headers[chunk_headers["chunk_size"]] = chunk_size
            headers[chunk_headers["content_chunkmethod"]] = \
                sysmeta['chunk_method']
            headers[chunk_headers["content_mimetype"]] = sysmeta['mime_type']
            headers[chunk_headers["content_policy"]] = sysmeta['policy']

            resp = self.session.put(rainx_url,
                                    data=_limit_stream(src, chunk_size),
                                    headers=headers)
            resp.raise_for_status()

            content_chunks.extend(_decode_chunklist(resp.headers['chunklist']))

            total_bytes_transferred += chunk_size

        content_checksum = global_checksum.hexdigest()

        return content_chunks, total_bytes_transferred, content_checksum

    def _fetch_stream_rain(self, meta, chunks, size, offset, headers=None):
        def get_rainx_addr():
            return self._get_service_url("rainx")

        downloader = RainDownloader(get_rainx_addr, meta, chunks)
        for d in downloader.get_stream(offset, size):
            yield d


def close_source(source):
    try:
        source.conn.close()
    except Exception:
        pass


class ChunkDownloadHandler(object):
    def __init__(self, chunks, size, offset, headers=None):
        self.chunks = chunks
        self.failed_chunks = []

        headers = headers or {}
        h_range = "bytes=%d-" % offset
        end = None
        if size >= 0:
            end = (size + offset - 1)
            h_range += str(end)
        headers["Range"] = h_range
        self.headers = headers
        self.begin = offset
        self.end = end

    def get_stream(self):
        source = self._get_chunk_source()
        stream = None
        if source:
            stream = self._make_stream(source)
        return stream

    def _fast_forward(self, nb_bytes):
        self.begin += nb_bytes
        if self.end and self.begin > self.end:
            raise Exception('Requested Range Not Satisfiable')
        h_range = 'bytes=%d-' % self.begin
        if self.end:
            h_range += str(self.end)
        self.headers['Range'] = h_range

    def _get_chunk_source(self):
        source = None
        for chunk in self.chunks:
            try:
                with ConnectionTimeout(CONNECTION_TIMEOUT):
                    raw_url = chunk["url"]
                    parsed = urlparse(raw_url)
                    conn = http_connect(parsed.netloc, 'GET', parsed.path,
                                        self.headers)
                source = conn.getresponse(True)
                source.conn = conn

            except (Timeout, Exception):
                self.failed_chunks.append(chunk)
                continue
            if source.status not in (200, 206):
                self.failed_chunks.append(chunk)
                close_source(source)
                source = None
            else:
                break

        return source

    def _make_stream(self, source):
        bytes_read = 0
        try:
            while True:
                try:
                    with ChunkReadTimeout(CHUNK_TIMEOUT):
                        data = source.read(READ_CHUNK_SIZE)
                        bytes_read += len(data)
                except ChunkReadTimeout:
                    self._fast_forward(bytes_read)
                    new_source = self._get_chunk_source()
                    if new_source:
                        close_source(source)
                        source = new_source
                        bytes_read = 0
                        continue
                    else:
                        raise
                if not data:
                    break
                yield data
        except ChunkReadTimeout:
            # error while reading chunk
            raise
        except GeneratorExit:
            # client premature stop
            pass
        except Exception:
            # error
            raise
        finally:
            close_source(source)


class RainDownloader(object):
    def __init__(self, get_rainx_addr, meta, chunks):
        self.get_rainx_addr = get_rainx_addr
        self.meta = meta
        self._meta_extract_k()
        self.chunks = chunks
        self.session = requests.session()

    def _meta_extract_k(self):
        coding, params = self.meta["chunk-method"].split('?')
        for param in params.split('&'):
            key, value = param.split('=')
            if key == 'k':
                self.k = int(value)

    def _get_sorted_chunks_at_metapos(self, metapos):
        chunks_at_pos = self.chunks[metapos]
        res = []
        for subpos in xrange(len(chunks_at_pos)):
            data_chunk = chunks_at_pos.get(str(subpos))
            if data_chunk is None:
                break
            res.append(data_chunk)
        for subpos in xrange(len(chunks_at_pos)):
            parity_chunk = chunks_at_pos.get("p%d" % subpos)
            if parity_chunk is None:
                break
            res.append(parity_chunk)
        return res

    def get_metachunk_size(self, metapos):
        metachunk_size = 0
        for subpos, subchunk in self.chunks[metapos].iteritems():
            if subpos.startswith("p"):
                continue
            metachunk_size += subchunk["size"]
        return metachunk_size

    def get_stream(self, offset, size):
        current_offset = 0
        total_bytes = 0
        if size is None:
            size = int(self.meta["length"])

        for metapos in range(len(self.chunks)):
            metachunk_size = self.get_metachunk_size(metapos)

            if total_bytes >= size:
                break
            if current_offset + metachunk_size > offset:
                if current_offset < offset:
                    _offset = offset - current_offset
                else:
                    _offset = 0
                if metachunk_size + total_bytes > size:
                    _size = size - total_bytes
                else:
                    _size = metachunk_size

                for s in self.get_metachunk_stream(metapos, _size, _offset):
                    total_bytes += len(s)
                    yield s
            current_offset += metachunk_size

    def get_metachunk_stream(self, metapos, size, offset):
        skip = 0

        for chunk in self._get_sorted_chunks_at_metapos(metapos):
            if ".p" in chunk["pos"]:
                break  # all data chunks have been processed
            if size == 0:
                break  # no more data wanted
            chunk_offset = offset - skip
            if chunk_offset <= chunk["size"]:
                chunk_to_read = chunk["size"] - chunk_offset
                if chunk_to_read > size:
                    chunk_to_read = size
                headers = {
                    "Range": "bytes=%d-%d" % (chunk_offset,
                                              chunk_offset + chunk_to_read - 1)
                }

                try:
                    resp = self.session.get(chunk["url"],
                                            headers=headers, stream=True)
                    resp.raise_for_status()
                    for data in resp.iter_content(READ_CHUNK_SIZE):
                        yield data
                        offset += len(data)
                        size -= len(data)
                except RequestException:
                    for data in self.get_rebuilt_stream(metapos, offset, size,
                                                        broken_chunk=chunk):
                        yield data
                    return

            skip += chunk["size"]

    def _gen_rawx_list(self, chunks):
        res_chunks = []
        for c in chunks:
            host = c['url'].split('/')[2]
            chunk_id = c['url'].split('/')[-1]
            res_chunks.append("%s/%s" % (host, chunk_id))
        return '|'.join(res_chunks)

    def _gen_spare_rawx_list(self, broken_chunks):
        res = []
        for c in broken_chunks:
            if ".p" in c["pos"]:
                broken_idx = self.k + int(c["pos"].split("p")[-1])
            else:
                broken_idx = int(c["pos"].split(".")[-1])
            res.append("%s|%d|%s" % (c["url"], broken_idx, c["hash"]))
        return ';'.join(res)

    def get_rebuilt_stream(self, metapos, offset, size, broken_chunk=None):
        chunks = self._get_sorted_chunks_at_metapos(metapos)
        broken_chunks = []
        for c in chunks:
            if broken_chunks is not None and c["pos"] == broken_chunk["pos"]:
                broken_chunks.append(c)
                continue
            status = True
            try:
                resp = self.session.head(c["url"])
                if resp.status_code != 200:
                    status = False
            except Exception:
                status = False
            if not status:
                broken_chunks.append(c)

        headers = {}
        headers["X-oio-chunk-meta-content-storage-policy"] = \
            self.meta["policy"]
        headers["X-oio-chunk-meta-rawxlist"] = \
            self._gen_rawx_list(chunks)
        headers["X-oio-chunk-meta-sparerawxlist"] = \
            self._gen_spare_rawx_list(broken_chunks)
        headers[chunk_headers["content_id"]] = 64 * '0'
        headers[chunk_headers["content_version"]] = 0
        headers[chunk_headers["content_path"]] = "xxx"
        headers[chunk_headers["content_size"]] = 0
        headers[chunk_headers["content_chunksnb"]] = 0
        headers[chunk_headers["container_id"]] = 64 * '0'
        headers[chunk_headers["chunk_pos"]] = 0
        headers["X-oio-chunk-meta-chunk-size"] = \
            self.get_metachunk_size(metapos)

        rainx_addr = "%s%s" % (self.get_rainx_addr(), "on-the-fly")
        resp = self.session.get(rainx_addr, headers=headers, stream=True)
        if resp.status_code != 200:
            raise exc.OioException("Error while rebuilding chunk on the fly")

        cur_pos = 0
        iter_data = resp.iter_content(READ_CHUNK_SIZE)
        for data in iter_data:  # skip data to offset
            if offset > cur_pos + len(data):
                cur_pos += len(data)
                continue  # skip data already read
            data_offset = offset - cur_pos
            d = data[data_offset:data_offset + size]
            offset += len(d)
            size -= len(d)
            yield d
            cur_pos += len(data)
