# Prometheus Metrics Documentation

[Prometheus](https://prometheus.io/) is an open-source monitoring and alerting
toolkit designed for reliability and scalability. It is widely used for
collecting and storing metrics as time series data, allowing users to query and
visualize this data effectively. Prometheus operates on a pull model, where it
scrapes metrics from configured endpoints at specified intervals. It provides
powerful querying capabilities through its query language, PromQL, enabling
users to extract insights from their metrics data.

This document outlines the Prometheus metrics exposed by ॐ, available at the
`/metrics` endpoint when serving the web UI. They can also be optionally exposed
on the HTTP port specified by the `--metrics-port` argument or the
`AUM_METRICS_PORT` environment variable during data indexing.

## Metrics Overview

### HTTP Metrics

- **http_request_total**:
  - **Type**: Counter
  - **Description**: Total number of HTTP requests.
  - **Labels**: `method`, `endpoint`
- **http_request_latency_bucket**:
  - **Type**: Histogram
  - **Description**: Procesing time of HTTP requests.
  - **Labels**: `method`, `endpoint`
- **http_exceptionstotal**:
  - **Type**: Counter
  - **Description**: Total exceptions raised during processing of HTTP requests.
  - **Labels**: `method`, `endpoint`, `exception_type`
- **http_request_in_progress_gauge**:
  - **Type**: Gauge
  - **Description**: Current number of in-progress requests.
  - **Labels**: `method`, `endpoint`
- **aum_search_query_total**:
  - **Type**: Counter
  - **Description**: Total search queries executed per index.
  - **Labels**: `index_name`

### Apache Tika Metrics

- **tika_extraction_request_total**:
  - **Type**: Counter
  - **Description**: Total text extraction requests.
  - **Labels**: `index_name`, `tika_url`
- **tika_extraction_content_type_total**:
  - **Type**: Counter
  - **Description**: Text extractions by detected content type of the document.
  - **Labels**: `index_name`, `tika_url`, `content_type`
- **tika_extraction_duration_seconds**:
  - **Type**: Histogram
  - **Description**: Processing time of text extraction requests (seconds).
  - **Labels**: `index_name`, `tika_url`
- **tika_extraction_in_flight**:
  - **Type**: Gauge
  - **Description**: Current number of text extractions in progress.
  - **Labels**: `index_name`, `tika_url`
- **tika_extraction_error_total**:
  - **Type**: Counter
  - **Description**: Total text extraction errors.
  - **Labels**: `index_name`, `tika_url`, `exception_type`

### Meilisearch Metrics

- **meilisearch_index_create_total**:
  - **Type**: Counter
  - **Description**: Total index creation requests.
  - **Labels**: `index_name`
- **meilisearch_index_delete_total**:
  - **Type**: Counter
  - **Description**: Total index deletion requests.
  - **Labels**: `index_name`
- **meilisearch_document_index_total**:
  - **Type**: Counter
  - **Description**: Total documents ingested.
  - **Labels**: `index_name`
- **meilisearch_search_request_total**:
  - **Type**: Counter
  - **Description**: Total search requests.
  - **Labels**: `index_name`
- **meilisearch_exception_total**:
  - **Type**: Counter
  - **Description**: Total exceptions raised for Meilisearch requests.
  - **Labels**: `index_name`, `exception_total`
- **meilisearch_search_duration_seconds**:
  - **Type**: Histogram
  - **Description**: Processing time of search requests (seconds).
  - **Labels**: `index_name`
- **meilisearch_document_index_duration_seconds**:
  - **Type**: Histogram
  - **Description**: Processing time of ingest tasks per document batch
    (seconds).
  - **Labels**: `index_name`

### Sonic Metrics

- **sonic_index_delete_total**:
  - **Type**: Counter
  - **Description**: Total index deletion requests.
  - **Labels**: `index_name`
- **sonic_document_index_total**:
  - **Type**: Counter
  - **Description**: Total documents ingested.
  - **Labels**: `index_name`
- **sonic_search_request_total**:
  - **Type**: Counter
  - **Description**: Total search requests.
  - **Labels**: `index_name`
- **sonic_exception_total**:
  - **Type**: Counter
  - **Description**: Total exceptions raised for Sonic requests.
  - **Labels**: `index_name`, `exception_type`
- **sonic_search_duration_seconds**:
  - **Type**: Histogram
  - **Description**: Processing time of search requests (seconds).
  - **Labels**: `index_name`
- **sonic_document_index_duration_seconds**:
  - **Type**: Histogram
  - **Description**: Processing time of document ingesting tasks (seconds).
  - **Labels**: `index_name`
