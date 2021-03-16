from insights_messaging.appbuilder import AppBuilder

config = """
plugins:
service:
    logging_configurator:
        name: insights_messaging.tests.test_get_logging_config.custom_log_config
    logging:
        version: 1
        disable_existing_loggers: false
        loggers:
            "":
                level: WARN
""".strip()


def custom_log_config():
    def inner(config):
        config["custom_log_stuff"] = "custom config here"
        return config
    return inner


def test_log_configurator():
    app = AppBuilder(config)
    conf = app._get_log_config()
    assert "custom_log_stuff" in conf
    assert "version" in conf
