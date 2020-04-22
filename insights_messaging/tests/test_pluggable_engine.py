"""
This test module ensures default and custom engines load correctly from
configuration.
"""
import yaml
from insights_messaging.appbuilder import AppBuilder
from insights_messaging.engine import Engine


class CustomEngine(Engine):
    pass


class MockFS:
    pass


class MockFormat:
    pass


CONFIG1 = yaml.load(
    """
plugins:
    default_component_enabled: true
    packages: []
    configs: []
service:
    engine:
        name: insights_messaging.tests.test_pluggable_engine.CustomEngine
        kwargs:
            format: insights_messaging.tests.test_pluggable_engine.MockFormat
            extract_timeout: 20
            extract_tmp_dir: ${TMP_DIR:/tmp}
            target_components: []
    consumer:
        name: insights_messaging.consumers.Consumer
    publisher:
        name: insights_messaging.publishers.Publisher
    downloader:
        name: insights_messaging.tests.test_pluggable_engine.MockFS
""",
    Loader=yaml.SafeLoader,
)


CONFIG2 = yaml.load(
    """
plugins:
    default_component_enabled: true
    packages: []
    configs: []
service:
    extract_timeout: 20
    extract_tmp_dir: ${TMP_DIR:/tmp}
    format: insights_messaging.tests.test_pluggable_engine.MockFormat
    target_components: []
    consumer:
        name: insights_messaging.consumers.Consumer
    publisher:
        name: insights_messaging.publishers.Publisher
    downloader:
        name: insights_messaging.tests.test_pluggable_engine.MockFS
""",
    Loader=yaml.SafeLoader,
)


def test_configs_engine():
    """ Should use the specified CustomEngine """
    app = AppBuilder(CONFIG1).build_app()
    assert isinstance(app.engine, CustomEngine)
    assert app.engine.Formatter is MockFormat
    assert app.engine.extract_timeout == 20
    assert app.engine.extract_tmp_dir == "/tmp"


def test_config1_engine():
    """ Should use the default insights_messaging.engine.Engine """
    app = AppBuilder(CONFIG2).build_app()
    assert isinstance(app.engine, Engine) and not isinstance(app.engine, CustomEngine)
    assert app.engine.Formatter is MockFormat
    assert app.engine.extract_timeout == 20
    assert app.engine.extract_tmp_dir == "/tmp"
