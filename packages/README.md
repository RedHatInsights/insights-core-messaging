Distributions
=============

The source code lives in a single `insights_messaging/` directory at the
repository root. It is split across several distributions defined under
`packages/`, each shipping a subset of the source files. Every `.py` file
(excluding tests) must belong to exactly one distribution — the CI job
`check-dist-files` enforces this.

Because the source tree is shared, each distribution uses hatchling's
`force-include` to list individual files. Glob patterns and directory-level
includes are only used when **all** files in a subdirectory belong to the
same distribution (e.g. `formats/` and `watchers/` in core). See the
`force-include` section in each `pyproject.toml` for the full mapping.

Packages
--------

### insights-core-messaging-base (`packages/base`)

The core library. Contains the engine, application builder, configuration
template, base consumers (CLI), base downloaders (local filesystem), base
publishers (CLI), requeuers base, utilities, formats, and watchers.

**Dependencies:** `insights-core`, `logstash_formatter`, `pyyaml`

### insights-core-messaging-kafka (`packages/kafka`)

Kafka consumer, Kafka requeuer, and retry utility.

**Dependencies:** `insights-core-messaging-base`, `confluent-kafka`,
`prometheus_client`

### insights-core-messaging-http (`packages/http`)

HTTP downloader.

**Dependencies:** `insights-core-messaging-base`, `requests`

### insights-core-messaging (`packages/all-in-one`)

The all-in-one distribution. Depends on all the above packages and
directly includes the RabbitMQ consumer, publisher, and requeuer, as well
as the S3 downloader.

**Dependencies:** `insights-core-messaging-base`,
`insights-core-messaging-kafka`, `insights-core-messaging-http`, `pika`,
`s3fs`
