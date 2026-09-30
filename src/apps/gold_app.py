import logging
import signal
import sys

ROOT_DIR = "/opt/spark/app"
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from spark.session import create_spark_session
from config.config_manager import ConfigManager
from streaming.stream_config import StreamConfig
from streaming.gold_stream import GoldStream
from gold_layer.gold_builder import GoldStreamingBuilder
from observability.structured_logger import configure_structured_logging
from observability.metrics import start_metrics_server


logger = logging.getLogger("DINAMO_GOLD")


class GoldApplication:

    def __init__(self):

        configure_structured_logging(
            level="INFO",
            service_name="dinamo-gold"
        )

        self.config_manager = ConfigManager()

        main_config = self.config_manager.main_config

        self.spark = create_spark_session(
            "DINAMO_GOLD",
            main_config
        )

        self.stream_config = StreamConfig()

        self.query = None

        signal.signal(signal.SIGTERM, self.shutdown)
        signal.signal(signal.SIGINT, self.shutdown)

    def start(self):

        start_metrics_server(port=8000)

        gold = GoldStream(
            spark=self.spark,
            config=self.stream_config,
            gold_builder_class=GoldStreamingBuilder
        )

        self.query = gold.start()

        logger.info("Gold stream started")

        self.query.awaitTermination()

        # Em modo available_now (efemero/KEDA), awaitTermination() retorna
        # sozinho quando a query termina (nao precisa de SIGTERM). Garante
        # que o processo encerra limpo em vez de ficar pendurado.
        logger.info("Gold stream finished — shutting down")
        self.spark.stop()

    def shutdown(self, *args):

        logger.info("Stopping Gold Stream")

        if self.query:
            self.query.stop()

        self.spark.stop()


if __name__ == "__main__":

    GoldApplication().start()