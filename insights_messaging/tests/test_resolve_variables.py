import yaml

from insights_messaging.appbuilder import resolve_variables


CONF = yaml.safe_load("""
plugins:
    default_component_enabled: true
    packages:
        - consumers
        - publishers
service:
    extract_timeout: 10
    extract_tmp_dir:
    consumer:
        name: consumers.process_report.ProcessReport
        kwargs:
          incoming_topic: ${KAFKA_SYNCED_TOPIC:com.default-topic}
          group_id: processing_service
          bootstrap_server: $BOOTSTRAP_URL
          bootstrap_servers:
            - "kafka:9092"
          security_protocol: ${KAFKA_SECURITY_PROTOCOL:SSL}
          ssl_cafile: ${KAFKA_SSL_CERT_PATH:/mnt/cert.crt}
    publisher:
        name: publishers.kafka_publisher.KafkaPublisher
        kwargs:
          outgoing_topic: ${KAFKA_FEATURES_TOPIC:com.default-archive.features}
          bootstrap_server: $BOOTSTRAP_URL
          bootstrap_servers:
            - "kafka:9092"
          security_protocol: ${KAFKA_SECURITY_PROTOCOL:SSL}
          ssl_cafile: ${KAFKA_SSL_CERT_PATH:/mnt/cert.crt}
    downloader:
        name: s3io.s3_downloader.S3Downloader
        kwargs:
          endpoint: "${SOURCE_S3_URL:http://minio-service:9000}"
          access_key: $SOURCE_S3_ACCESS_KEY
          secret_key: $SOURCE_S3_SECRET_KEY
          bucket: ${SOURCE_S3_BUCKET:ceph}
    watchers:
        - name: watchers.consumer_watcher.FeaturesConsumerWatcher
          kwargs:
            prometheus_port: ${PROMETHEUS_PORT:8002}
    logging:
        version: 1
        disable_existing_loggers: false
        handlers:
            default:
               level: DEBUG
               class: logging.StreamHandler
               stream: ext://sys.stdout
               formatter: default
        formatters:
            default:
               format: '%(asctime)s %(levelname)-8s %(message)s'
               datefmt: '%Y-%m-%d %H:%M:%S'
        root:
            handlers:
                - default
            level: WARNING
        loggers:
            consumers:
                level: DEBUG
            publishers:
                level: DEBUG
            s3io:
                level: DEBUG
            triggers:
                level: DEBUG
            utils:
                level: DEBUG
            insights_messaging:
                level: DEBUG
            insights:
                level: WARNING
""")


def test_resolve_variables():
    res = resolve_variables(CONF)
    assert res["plugins"]["default_component_enabled"] is True
    assert res["service"]["extract_timeout"] == 10
    assert res["service"]["consumer"]["kwargs"]["bootstrap_server"] == "$BOOTSTRAP_URL"
    assert res["service"]["consumer"]["kwargs"]["security_protocol"] == "SSL"
    assert res["service"]["consumer"]["kwargs"]["ssl_cafile"] == "/mnt/cert.crt"
    assert res["service"]["downloader"]["kwargs"]["endpoint"] == "http://minio-service:9000"
    assert res["service"]["watchers"][0]["kwargs"]["prometheus_port"] == 8002
