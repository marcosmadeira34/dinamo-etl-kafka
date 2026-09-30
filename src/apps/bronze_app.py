# Cada SparkApplication será responsável por iniciar apenas uma camada.
# BronzeLayerStream será responsável por ler os dados do kafka e gravar no bronze.

import sys
import logging
import signal

ROOT_DIR = '/opt/spark/app'
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from spark.session import create_spark_session
from config.config_manager import ConfigManager
from streaming.stream_config import StreamConfig
from streaming.bronze_stream import BronzeStream

from observability.structured_logger import configure_structured_logging
from observability.metrics import start_metrics_server

logger = logging.getLogger("DINAMO_BRONZE")


class BronzeSparkApplication:

    def __init__(self):
        
        configure_structured_logging(
            level       = "INFO",
            service_name = "dinamo-bronze-etl"
        )

        self.config_manager = ConfigManager()
        main_config = self.config_manager.main_config

        self.spark = create_spark_session(
                                        "DINAMO_BRONZE",
                                        main_config
                                        )

        self.stream_config = StreamConfig()
        self.bucket_name = main_config["s3"]["bucket_name"]

        self.query = None

        signal.signal(signal.SIGTERM, self.shutdown)
        signal.signal(signal.SIGINT, self.shutdown)

    def start_bronze_stream(self):
        
        start_metrics_server(port=8000)

        bronze = BronzeStream(
            spark       = self.spark,
            config      = self.stream_config,
            bucket_name = self.bucket_name
        )

        self.query = bronze.start()
        logger.info('Bronze stream started')

        self.query.awaitTermination()

        # Em modo available_now (efemero/KEDA), awaitTermination() retorna
        # sozinho quando a query termina (nao precisa de SIGTERM). Garante
        # que o processo encerra limpo em vez de ficar pendurado.
        logger.info("Bronze stream finished — shutting down")
        self.spark.stop()
    
    def shutdown(self, *args):
        logger.info("Stopping Bronze Stream")
        if self.query:
            self.query.stop()

        self.spark.stop()       


if __name__ == "__main__":
    BronzeSparkApplication().start_bronze_stream()