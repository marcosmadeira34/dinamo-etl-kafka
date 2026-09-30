# src/dinamo_web/observability/structured_logger.py

import logging
import sys
from pythonjsonlogger import jsonlogger


def configure_structured_logging(
    level: str = "INFO",
    service_name: str = "dinamo-etl"
):

    class DinamoJsonFormatter(jsonlogger.JsonFormatter):

        def add_fields(self, log_record, record, message_dict):

            super().add_fields(log_record, record, message_dict)

            log_record["service"] = service_name
            log_record["level"] = record.levelname
            log_record["logger"] = record.name
            log_record["timestamp"] = record.created

    formatter = DinamoJsonFormatter()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()

    root_logger.handlers.clear()
    root_logger.addHandler(handler)

    root_logger.setLevel(
        getattr(logging, level.upper(), logging.INFO)
    )

    # Silencia libs verbosas
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("confluent_kafka").setLevel(logging.WARNING)

    logging.getLogger("dinamo").info(
        "Structured logging configured",
        extra={
            "service_name": service_name,
            "log_level": level,
        }
    )