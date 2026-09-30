# dinamo_web/src/dinamo_web/streaming/streaming_pipeline.py
"""
Streaming Pipeline Orchestrator — starts and manages all streaming jobs.
Entry point for the streaming ETL platform.
"""
import logging
import os
import sys
import signal

# Ensure project root is in path
ROOT_DIR = "/opt/spark/app"
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from spark.session import create_spark_session

from config.config_manager import ConfigManager
from kafka.config import KafkaConfig
from streaming.stream_config import StreamConfig
from streaming.bronze_stream import BronzeStream
from streaming.silver_stream import SilverStream
from streaming.gold_stream import GoldStream
# from ingestion.file_watcher import SpedFileWatcher
# from ingestion.object_storage_ingestion import ObjectStorageEventIngestion
from observability.structured_logger import configure_structured_logging
from observability.metrics import start_metrics_server
from gold_layer.gold_builder import GoldStreamingBuilder
from silver_layer.silver_builder import SilverSpedBuilder
import threading
import time


logger = logging.getLogger("DINAMO_STREAMING_PIPELINE")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


class StreamingPipeline:
    """
    Orchestrates all streaming jobs and the file watcher.
    
    Usage:
        pipeline = StreamingPipeline()
        pipeline.start()  # starts all streams + file watcher
    """

    def __init__(self):

        configure_structured_logging(level="INFO", service_name="dinamo-streaming")

        self.config_manager = ConfigManager()
        main_config = self.config_manager.main_config
        logger.info(f"Main config: {main_config}")

        # Spark session with Kafka + streaming support
        self.spark = create_spark_session(
            main_config["spark"]["app_name"] + " [Streaming]",
            main_config
        )

        self.kafka_config = KafkaConfig.from_yaml(
            self.config_manager.load_yaml("kafka_config.yaml")
        )
        self.stream_config = StreamConfig()
        self.bucket_name = main_config["s3"]["bucket_name"]

        self._queries = []
        self._running = True

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

        logger.info("StreamingPipeline initialized")

    def _shutdown(self, signum, frame):
        logger.info("Shutdown signal received. Stopping all streams...")
        self._running = False
        for q in self._queries:
            try:
                q.stop()
            except Exception as e:
                logger.warning(f"Error stopping query: {e}")

    def start_bronze(self):
        """Start the Bronze streaming job."""
        logger.info("Starting Bronze stream...")
        bronze = BronzeStream(
            spark=self.spark,
            config=self.stream_config,
            bucket_name=self.bucket_name,
        )
        query = bronze.start()
        self._queries.append(query)
        return query

    def start_silver(self):
        """Start the Silver streaming job."""
        

        tax_regime_config = self.config_manager.tax_regime_config["regime_tributario"]
        cfop_config = self.config_manager.cfop_config["tabela_cfop"]

        silver_builder = SilverSpedBuilder(
            self.spark,
            regime_config=tax_regime_config,
            tabela_cfop_config=cfop_config,
        )

        logger.info("Starting Silver stream...")
        silver = SilverStream(
            spark=self.spark,
            config=self.stream_config,
            silver_builder=silver_builder,
        )
        query = silver.start()
        self._queries.append(query)
        return query

    def start_gold(self):
        """Start the Gold streaming job."""
        
        logger.info("Starting Gold stream...")
        gold = GoldStream(
            spark=self.spark,
            config=self.stream_config,
            gold_builder_class=GoldStreamingBuilder,
        )
        query = gold.start()
        self._queries.append(query)
        return query

    # def start_file_watcher(self):
    #     """Start the OCI bucket file watcher (runs in background thread)."""
        

    #     watcher = SpedFileWatcher(
    #         spark=self.spark,
    #         bucket_name=self.bucket_name,
    #         kafka_config=self.kafka_config,
    #         watch_prefix="TO_CONVERT/",
    #         poll_interval_sec=30,
    #     )

    #     thread = threading.Thread(target=watcher.start, daemon=True, name="file-watcher")
    #     thread.start()
    #     logger.info("File watcher started in background thread")
    #     return watcher
    

    def start(self):
        """Start all streaming components."""
        logger.info("=" * 60)
        logger.info(" DINAMO ETL Streaming Platform Starting")
        logger.info("=" * 60)

        # Start metrics server
        try:
            start_metrics_server(port=8000)
        except Exception as e:
            logger.warning(f"Could not start metrics server: {e}")

        # Start all streams
        self.start_bronze()
        self.start_silver()
        self.start_gold()

        # Start file watcher
        # self.start_file_watcher()

        logger.info("All streaming components started. Awaiting termination...")

        # Block until all queries terminate
        try:
            for query in self._queries:
                query.awaitTermination()
            logger.info("Todas as queries terminaram. Aguardando 60s antes de finalizar (debug)...")
            time.sleep(60)
        except KeyboardInterrupt:
            logger.info("Interrupted. Shutting down...")
        finally:
            self.stop()

    def stop(self):
        """Stop all streaming queries."""
        for q in self._queries:
            try:
                q.stop()
            except Exception:
                pass
        self.spark.stop()
        logger.info("Streaming platform stopped")


if __name__ == "__main__":
    pipeline = StreamingPipeline()
    pipeline.start()
