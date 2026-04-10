---
description: 'Built-in web interface for executing ClickHouse queries in the browser'
sidebar_label: 'Web Interface'
sidebar_position: 19
slug: /interfaces/web
title: 'Web Interface'
doc_type: 'reference'
---

# Web Interface {#web-interface}

ClickHouse includes a built-in web interface for executing SQL queries directly in the browser.
It is available at `http://localhost:8123/play` (adjust the host and port for your configuration).

The web interface supports:

- Syntax highlighting via a built-in SQL lexer.
- Autocompletion of ClickHouse keywords, functions, table names, and column names.
- Table, chart, and raw text output modes.
- Query parameters with `{name:Type}` placeholder syntax.
- Dark mode.
- Query history stored in the browser's local storage.
- Keyboard shortcuts: **Ctrl+Enter** (or **Cmd+Enter** on macOS) to run a query, **Ctrl+Shift+Enter** to run all queries.

## Multi-query mode {#multi-query-mode}

When the textarea contains more than one SQL statement (separated by semicolons), the interface enters multi-query mode.
Two buttons appear: **Run one** (executes the query under the cursor) and **Run all** (executes every query).

In multi-query mode:

- Multiple consecutive `SELECT`-like queries (`SELECT`, `SHOW`, `DESCRIBE`, `EXPLAIN`, `WITH`) are executed in parallel.
- Other statement types (`CREATE`, `INSERT`, `ALTER`, etc.) act as barriers: they are executed sequentially, and parallel execution resumes only after the barrier completes.
- Each query produces its own result table or chart.
- The progress bar aggregates progress across all concurrent queries.

### Text selection {#text-selection}

When text is selected in multi-query mode, the **Run one** button changes to **Run selected** and only the queries intersecting the selection are executed.
If the selection covers the entire textarea, **Run all** is hidden since **Run selected** already covers the same scope.

### Query under cursor {#query-under-cursor}

The query under the cursor is highlighted with a faint backdrop.
**Ctrl+Enter** always runs the query under the cursor (or the selected queries if there is a selection).

## Configuration {#configuration}

The web interface is served by the HTTP server.
It is enabled by default on port 8123.
To change the port, modify the `http_port` setting in the server configuration.

To disable the web interface while keeping the HTTP API available, set `play_path` to an empty string:

```xml
<play_path></play_path>
```

## URL parameters {#url-parameters}

The web interface preserves query text and connection parameters in the URL, so links can be shared.
For example:

```text
http://localhost:8123/play?user=default#SELECT%201
```
