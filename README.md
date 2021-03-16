Construct an insights archive processing application by providing a
configuration file that specifies its components. The building blocks
are described below.

Engine
------
An engine encapsulates the process of evaluating an archive with insights. It
has parameters for its result formatter, a subset of the loaded components to
evaluate, an archive extraction timeout, and a working directory for it to use
during analysis. A consumer feeds an engine one archive at a time and uses
a publisher to publish the results.

Consumer
--------
A consumer retrieves a message at a time from a source, extracts a url from it,
downloads an archive using the configured downloader, and passes the file to an
internal engine for processing.  It retrieves results from the engine and
publishes them with the configured publisher.
```python
import logging
from . import Consumer

log = logging.getLogger(__name__)


class Interactive(Consumer):
    def run(self):
        while True:
            input_msg = input("Input Archive Name: ")
            if not input_msg:
                break
            self.process(input_msg)

    def get_url(self, input_msg):
        return input_msg
```

Requeuer
--------
A requerer allows a consumer to raise a `Requeue` exception to indicate that
it couldn't handle the input and would like the requerer to do something with
it. What the requerer does is open ended: it could put the message onto a
different topic, send it to a different message broker, store it in a
database, etc.


Publisher
---------
A publisher stores results in some way. For example, it could publish them to
a database, post them to a message queue, or display them. It is given the
original request and the raw result string from the configured formatter.
```python
from . import Publisher


class StdOut(Publisher):
    def publish(self, input_msg, results):
        print(results)
```

Format
------
A format is used by the engine to monitor insights components during analysis,
capture interesting information about them, and convert the results into a string
that will make up the body of the application's response.

Downloader
----------
A downloader is used by a consumer to download archives. The project provides
downloaders for http endpoints, S3 buckets, and the local file system.
```python
import os
import shutil

from contextlib import contextmanager
from tempfile import NamedTemporaryFile

from s3fs import S3FileSystem


class S3Downloader(object):
    def __init__(self, tmp_dir=None, chunk_size=16 * 1024, **kwargs):
        self.tmp_dir = tmp_dir
        self.chunk_size = chunk_size
        self.fs = S3FileSystem(**kwargs)

    @contextmanager
    def get(self, src):
        with self.fs.open(src) as s:
            with NamedTemporaryFile(dir=self.tmp_dir) as d:
                shutil.copyfileobj(s, d, length=self.chunk_size)
                d.flush()
                yield d.name
```

Here's one for the local file system.
```python
import os
from contextlib import contextmanager


class LocalFS(object):
    @contextmanager
    def get(self, src):
        path = os.path.realpath(os.path.expanduser(src))
        yield path
```

Watchers
--------
Watchers monitor events from the consumer or engine. A consumer watcher might track
the total number of archives, the number that succeeded, and the number that failed.
An engine watcher might track component execution times. Look at the [watchers](https://github.com/RedHatInsights/insights-core-messaging/blob/master/insights_messaging/watchers/__init__.py)
package for the possible callbacks.
```python
from pprint import pprint

from insights import dr
from insights_messaging.watcher import EngineWatcher


class LocalStatWatcher(EngineWatcher):
    def __init__(self):
        self.archives = 0

    def on_engine_complete(self, broker):
        self.archives += 1
        times = {dr.get_name(k): v for k, v in broker.exec_times.items() if k in broker}
        pprint({"times": times, "archives": self.archives})
```

Logging
-------
Standard [logging configuration](https://docs.python.org/3.7/library/logging.config.html#logging-config-dictschema) can be specified under the `logging` key.

You can programmatically modify the logging configuration specified above by
providing a `logging_configurator` key. It should specify a function that
returns another function. The returned function must accept the existing log
configuration above and return a modified version of it.
```yaml
service:
    logging_configurator:
        name: insights_messaging.tests.test_get_logging_config.custom_log_config
        args: []
        kwargs: {}
```
Example function:
```python
def custom_log_config(*args, **kwargs):
    def inner(config):
        config["custom_log_stuff"] = "custom config here"
        return config
    return inner
```
Environment Variable Substitution
---------------------------------
Environment variables may be used in any value position. They can be
specified in one of three ways:

```yaml
foo: $SOME_ENV
foo: ${SOME_ENV}
foo: ${SOME_ENV:<default value>}
```

If the environment variable isn't defined, the string value is not modified unless
a default has been specified. If the environment variable is defined, it will be
substituted even if its value is nothing.

The default value is everything from the first colon (:) to the first closing
bracket. Closing brackets can not be escaped.

We first try to convert the value to a boolean if it is "true" or "false" (case
insensitive), then an int, then a float. If all conversions fail, it's returned
as a string.

Example Configuration
---------------------
The plugins section of the configuration is standard insights configs. The
service section contains the components that make up the application.

The consumer, publisher, downloader, and watchers must contain a full component
name, and they may contain a list called `args` and a dictionary called
`kwargs`. If provided, the args and kwargs are used to construct the component.
In the case of a consumer, they are provided after the standard args of
publisher, downloader, and engine.

The `format` value is the class name of the result formatter the engine should
use.  It defaults to `insights.formats.text.HumanReadableFormat`.

The `target_components` list can be used to constrain which loaded components
are executed. The dependency graph of components whose full names start with
any element will be executed. If it is empty or the `target_components` key
doesn't exist, all loaded componets are evaluated.

`extract_tmp_dir` is where the engine will extract archives for analysis. It
will use `/tmp` if no path is provided.

`extract_timeout` is the number of seconds the engine will attempt to extract
an archive. It raises an exception if the timeout is exceeded or tries forever
if no timeout is specified.

The `engine` section specifies which engine class to use. If it exists, it
takes default configuration from `format`, `target_components`,
`extract_tmp_dir`, and `extract_timeout` at the same level as the `engine`
key. If it has a `kwargs` key, any values there override those defaults. If the
`engine` section doesn't exist, `insights_messaging.engine.Engine` is used and
takes the default configs.

Custom Engine Config
--------------------
```yaml
# insights.parsers.redhat_release must be loaded and enabled for
# insights_messaging.formats.rhel_stats.Stats to collect product and version
# info. This is also true for insights.formats._json.JsonFormat.
plugins:
    default_component_enabled: true
    packages:
        - insights.specs.default
        - insights.specs.insights_archive
        - insights.parsers.redhat_release
        - examples.rules
    configs:
        - name: examples.rules.bash_version.report
          enabled: true
service:
    engine:
        name: examples.engine.CustomEngine
        kwargs:
            format: insights_stats_worker.rhel_stats.Stats
            target_components:
                - foo.bar.rules
            extract_timeout: 10
            extract_tmp_dir: ${TMP_DIR:/tmp}
    consumer:
        name: insights_stats_worker.consumer.Consumer
        kwargs:
            queue: test_job
            conn_params:
                host: ${CONSUMER_HOST:localhost}
                port: ${CONSUMER_PORT:5672}
    requeuer:
        name: example.requeuer.Requeuer
        kwargs:
            queue: retry
            conn_params:
                host: ${CONSUMER_HOST:localhost}
                port: ${CONSUMER_PORT:5672}
    publisher:
        name: insights_messaging.publishers.rabbitmq.RabbitMQ
        kwargs:
            queue: test_job_response
            conn_params:
                host: localhost
                port: 5672
    downloader:
        name: insights_messaging.downloaders.localfs.LocalFS
    watchers:
        - name: insights_messaging.watchers.stats.LocalStatWatcher
    logging:
        version: 1
        disable_existing_loggers: false
        loggers:
            "":
                level: WARN
```

Default Engine Config
---------------------
```yaml
# insights.parsers.redhat_release must be loaded and enabled for
# insights_messaging.formats.rhel_stats.Stats to collect product and version
# info. This is also true for insights.formats._json.JsonFormat.
plugins:
    default_component_enabled: true
    packages:
        - insights.specs.default
        - insights.specs.insights_archive
        - insights.parsers.redhat_release
        - examples.rules
    configs:
        - name: examples.rules.bash_version.report
          enabled: true
service:
    format: insights_stats_worker.rhel_stats.Stats
    target_components:
        - foo.bar.rules
    extract_timeout: 10
    extract_tmp_dir: ${TMP_DIR:/tmp}
    consumer:
        name: insights_stats_worker.consumer.Consumer
        kwargs:
            queue: test_job
            conn_params:
                host: ${CONSUMER_HOST:localhost}
                port: ${CONSUMER_PORT:5672}
    requeuer:
        name: example.requeuer.Requeuer
        kwargs:
            queue: retry
            conn_params:
                host: ${CONSUMER_HOST:localhost}
                port: ${CONSUMER_PORT:5672}
    publisher:
        name: insights_messaging.publishers.rabbitmq.RabbitMQ
        kwargs:
            queue: test_job_response
            conn_params:
                host: localhost
                port: 5672
    downloader:
        name: insights_messaging.downloaders.localfs.LocalFS
    watchers:
        - name: insights_messaging.watchers.stats.LocalStatWatcher
    logging:
        version: 1
        disable_existing_loggers: false
        loggers:
            "":
                level: WARN
    logging_configurator:
        name: insights_messaging.tests.test_get_logging_config.custom_log_config
        args: []
        kwargs: {}
```
