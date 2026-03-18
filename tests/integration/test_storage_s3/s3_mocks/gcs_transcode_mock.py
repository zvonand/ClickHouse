import gzip
import sys

from bottle import request, response, route, run

DATA = b'{"id":1}\n{"id":2}\n{"id":3}\n'
COMPRESSED = gzip.compress(DATA)


@route("/")
def ping():
    response.content_type = "text/plain"
    response.set_header("Content-Length", 2)
    return "OK"


@route("/<_bucket>/<_path:path>", method="HEAD")
def head(_bucket, _path):
    response.content_type = "application/octet-stream"
    # No Content-Length: simulates GCS decompressive transcoding
    response.set_header("ETag", '"abc123"')
    response.set_header("Last-Modified", "Fri, 13 Mar 2026 10:54:50 GMT")
    return ""


@route("/<_bucket>/<_path:path>", method="GET")
def get(_bucket, _path):
    response.content_type = "application/octet-stream"
    # No Content-Length on GET either
    response.set_header("ETag", '"abc123"')
    response.set_header("Last-Modified", "Fri, 13 Mar 2026 10:54:50 GMT")
    return DATA


run(host="0.0.0.0", port=int(sys.argv[1]))
