# Cada SparkApplication e responsavel por iniciar apenas uma camada/job.
# SubvencaoStreamApplication inicia so o stream de diagnostico de Subvencao
# Fiscal. Nome deliberadamente distinto de SubvencaoApplication (o
# entrypoint batch em apps/subvencao_app.py) para nao confundir as duas
# classes — sao modos de execucao diferentes (streaming continuo vs job
# unico sob demanda), embora ambas usem SubvencaoGoldBuilder por baixo.
import logging
import signal
import sys

ROOT_DIR = "/opt/spark/app"
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from spark.session import create_spark_session
from config.config_manager import ConfigManager
from src.streaming.stream_config import StreamConfig
from modules.subvencao.streaming.subvencao_stream import SubvencaoStream
from observability.structured_logger import configure_structured_logging
from observability.metrics import start_metrics_server

logger = logging.getLogger("DINAMO_SUBVENCAO_STREAM")


class SubvencaoStreamApplication:

    def __init__(self):
        configure_structured_logging(
            level="INFO",
            service_name="dinamo-subvencao-stream",
        )

        self.config_manager = ConfigManager()
        main_config = self.config_manager.main_config
        self.subvencao_config = self.config_manager.subvencao_config
        self.bucket_name = main_config["s3"]["bucket_name"]

        self.spark = create_spark_session("DINAMO_SUBVENCAO_STREAM", main_config)
        self.stream_config = StreamConfig()

        self.query = None

        signal.signal(signal.SIGTERM, self.shutdown)
        signal.signal(signal.SIGINT, self.shutdown)

    def start(self):
        start_metrics_server(port=8000)

        subvencao = SubvencaoStream(
            spark=self.spark,
            config=self.stream_config,
            bucket_name=self.bucket_name,
            subvencao_config=self.subvencao_config,
        )

        self.query = subvencao.start()
        logger.info("Subvencao stream started")

        self.query.awaitTermination()

        logger.info("Subvencao stream finished — shutting down")
        self.spark.stop()

    def shutdown(self, *args):
        logger.info("Stopping Subvencao Stream")
        if self.query:
            self.query.stop()
        self.spark.stop()


if __name__ == "__main__":
    SubvencaoStreamApplication().start()
