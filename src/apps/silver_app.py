import logging
import signal
import sys

ROOT_DIR = "/opt/spark/app"
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from spark.session import create_spark_session
from config.config_manager import ConfigManager
from streaming.stream_config import StreamConfig
from streaming.silver_stream import SilverStream
from silver_layer.silver_builder import SilverSpedBuilder
from observability.structured_logger import configure_structured_logging
from observability.metrics import start_metrics_server


logger = logging.getLogger("DINAMO_SILVER")


class SilverApplication:

    def __init__(self):

        configure_structured_logging(
            level="INFO",
            service_name="dinamo-silver"
        )

        self.config_manager = ConfigManager()

        main_config = self.config_manager.main_config

        self.spark = create_spark_session(
            "DINAMO_SILVER",
            main_config
        )

        self.stream_config = StreamConfig()

        self.query = None

        signal.signal(signal.SIGTERM, self.shutdown)
        signal.signal(signal.SIGINT, self.shutdown)

    def start(self):

        start_metrics_server(port=8000)

        tax_regime_config = (
            self.config_manager.tax_regime_config["regime_tributario"]
        )

        cfop_config = (
            self.config_manager.cfop_config["tabela_cfop"]
        )

        silver_builder = SilverSpedBuilder(
            self.spark,
            regime_config=tax_regime_config,
            tabela_cfop_config=cfop_config
        )

        silver = SilverStream(
            spark=self.spark,
            config=self.stream_config,
            silver_builder=silver_builder
        )

        self.query = silver.start()

        logger.info("Silver stream started")

        self.query.awaitTermination()

        # Em modo available_now (efemero/KEDA), awaitTermination() retorna
        # sozinho quando a query termina (nao precisa de SIGTERM). Garante
        # que o processo encerra limpo em vez de ficar pendurado.
        logger.info("Silver stream finished — shutting down")
        self.spark.stop()

    def shutdown(self, *args):

        logger.info("Stopping Silver Stream")

        if self.query:
            self.query.stop()

        self.spark.stop()


if __name__ == "__main__":

    SilverApplication().start()