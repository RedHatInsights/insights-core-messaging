Construct an insights archive processing application by providing a
configuration file that specifies its components. The component types
are described below.

Consumer
--------
A consumer retrieves a message at a time from a source, extracts a url from it,
downloads an archive using the configured downloader, and passes the file to
an internal engine for processing.  It retrieves from the results from the
engine publishes them with the configured publisher.
```python
import logging
from . import Consumer

log = logging.getLogger(__name__)


class Interactive(Consumer):
    def run(self):
        while True:
            input_msg = input("Input Archive Name: ")
            if not msg:
                break
            self.process(input_msg)

    def get_url(self, input_msg):
        return input_msg
```

Publisher
---------
A publisher stores archive analysis results in some way. For example, it could
publish them to a database, post them to a message queue, or display them. It is
given the original analysis request and the raw result string from the configured
formatter. It can use both to construct the final response.
```python
from . import Publisher


class StdOut(Publisher):
    def publish(self, input_msg, results):
        print(results)
```

Format
------
A format is used by the engine to monitor insights components during an analysis,
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
    def __init__(self, tmp_dir=None, **kwargs):
        key = kwargs.pop("key", os.environ.get("AWS_ACCESS_KEY_ID"))
        secret = kwargs.pop("secret", os.environ.get("AWS_SECRET_ACCESS_KEY"))

        self.tmp_dir = tmp_dir
        self.fs = S3FileSystem(key=key, secret=secret, **kwargs)

    @contextmanager
    def get(self, src):
        with self.fs.open(src) as s:
            with NamedTemporaryFile(dir=self.tmp_dir) as d:
                shutil.copyfileobj(s, d)
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
An engine watcher might track component execution times. Look at the [watchers](https://github.com/csams/insights-core-messaging/blob/master/insights_messaging/watchers/__init__.py)
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

Example Configuration
---------------------
The plugins and configs section of the configuration are standard insights
configs. The service section contains the components that make up the
application.

The consumer, publisher, downloader, and watchers must contain a full component
name, and they may contain a list called `args` and a dictionary called
`kwargs`. If provided, the args and kwargs are used to construct the component.
In the case of a consumer, they are provided after the standard args of
publisher, downloader, and engine.

The `target_components` list can be used to constrain which loaded components
are executed. The dependency graph of components whose full names start with
any element will be executed.

`extract_tmp_dir` is where the engine will extract archives for analysis. It
will use `/tmp` if no path is provided.

`extract_timeout` is the number of sections the engine will attempt to
extract an archive. It raises an exception if the timeout is exceeded or will
try forever if no timeout is specified.

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
        - examples.rules.bash_version
configs:
    - name: examples.rules.bash_version.report
      enabled: true
service:
    extract_timeout:
    extract_tmp_dir:
    format: insights_messaging.formats.rhel_stats.Stats
    target_components:
        - examples.rules.bash_version.report
        - insights.parsers.redhat_release
    consumer:
        name: insights_messaging.consumers.cli.Interactive
    publisher:
        name: insights_messaging.publishers.cli.StdOut
    downloader:
        name: insights_messaging.downloaders.localfs.LocalFS
    watchers:
        - name: insights_messaging.watchers.stats.LocalStatWatcher
```
