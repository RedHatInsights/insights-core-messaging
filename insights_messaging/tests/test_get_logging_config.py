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
    """AppBuilder must apply the logging_configurator function to the log config.

    The logging_configurator is a factory function specified in the YAML
    config.  It returns a function that receives the base logging dict
    and can modify it (e.g. add custom handlers, change levels).  This
    allows deployments to programmatically customize logging beyond what
    static YAML supports.
    """
    app = AppBuilder(config)
    conf = app._get_log_config()
    assert "custom_log_stuff" in conf
    assert "version" in conf
