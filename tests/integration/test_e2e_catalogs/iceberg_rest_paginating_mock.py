"""Paginating mock Iceberg REST catalog server.

Used by ``test_pagination.py`` to validate that ClickHouse's ``RestCatalog``
honors the ``next-page-token`` continuation contract from the Iceberg REST
OpenAPI spec — i.e. that ``RestCatalog::getNamespaces`` and
``RestCatalog::getTables`` continue paging until the catalog server runs out
of items.

Implements the minimum surface that ``DatabaseDataLake`` exercises during
``SHOW TABLES``:

  ``GET /v1/config?warehouse=<name>``         catalog config (no prefix)
  ``GET /v1/namespaces[?parent=<ns>]
        [&pageToken=<token>]``                paginated namespace listing
  ``GET /v1/namespaces/{ns}/tables
        [?pageToken=<token>]``                paginated table listing

Pagination uses the Iceberg REST OpenAPI v1 contract:

  request:   ``?pageToken=<token>``
  response:  ``{"namespaces": [...], "next-page-token": "<token>"}``
             ``{"identifiers": [...], "next-page-token": "<token>"}``

When the response omits ``next-page-token``, iteration ends. We use the
decimal offset as the token, which is convenient for tests.

Usage:

    python3 iceberg_rest_paginating_mock.py <port>
            [<namespaces_page_size> [<tables_page_size>]]

Defaults: namespaces_page_size=1, tables_page_size=3, which together with
the inventory below force genuine multi-page listings.

The fixed inventory mirrors what ``test_pagination.py`` expects.
"""

import sys

from bottle import request, response, route, run


NAMESPACES = ["ns_alpha", "ns_beta"]
TABLES = {
    "ns_alpha": ["t1", "t2", "t3", "t4", "t5", "t6", "t7"],
    "ns_beta":  ["s1", "s2", "s3", "s4"],
}

# Set from argv in __main__.
NAMESPACES_PAGE_SIZE = 1
TABLES_PAGE_SIZE = 3


def _paginate(items, page_size, page_token):
    """Slice ``items`` starting at offset ``int(page_token)``.

    Returns ``(page, next_page_token-or-None)``. When there are no more items,
    the next-page-token is omitted entirely (matching the Iceberg REST spec
    which says the field is OPTIONAL).
    """
    try:
        offset = int(page_token) if page_token else 0
    except ValueError:
        offset = 0
    if offset < 0 or offset >= len(items):
        return [], None
    end = offset + page_size
    return items[offset:end], (str(end) if end < len(items) else None)


@route("/")
def ping():
    """Health probe used by ``start_mock_servers``."""
    response.content_type = "text/plain"
    response.set_header("Content-Length", 2)
    return "OK"


@route("/v1/config")
def config():
    """Return empty defaults / overrides — no ``prefix``.

    With no ``prefix``, the catalog hits ``/v1/namespaces`` and
    ``/v1/namespaces/{ns}/tables`` directly, exactly as a vanilla
    Iceberg REST server would.
    """
    response.content_type = "application/json"
    return {"defaults": {}, "overrides": {}}


@route("/v1/namespaces")
def list_namespaces():
    response.content_type = "application/json"
    # ``parent=...`` filter: we have no sub-namespaces; return empty.
    if request.query.get("parent"):
        return {"namespaces": []}
    page, next_token = _paginate(
        NAMESPACES,
        NAMESPACES_PAGE_SIZE,
        request.query.get("pageToken", ""),
    )
    body = {"namespaces": [[ns] for ns in page]}
    if next_token is not None:
        body["next-page-token"] = next_token
    return body


@route("/v1/namespaces/<namespace>/tables")
def list_tables(namespace):
    response.content_type = "application/json"
    tables = TABLES.get(namespace, [])
    page, next_token = _paginate(
        tables,
        TABLES_PAGE_SIZE,
        request.query.get("pageToken", ""),
    )
    body = {"identifiers": [{"namespace": [namespace], "name": t} for t in page]}
    if next_token is not None:
        body["next-page-token"] = next_token
    return body


if __name__ == "__main__":
    port = int(sys.argv[1])
    if len(sys.argv) >= 3:
        NAMESPACES_PAGE_SIZE = int(sys.argv[2])
    if len(sys.argv) >= 4:
        TABLES_PAGE_SIZE = int(sys.argv[3])
    run(host="0.0.0.0", port=port)
