# # dinamo_web/src/dinamo_web/ingestion/file_watcher.py
# """
# S3/OCI Bucket File Watcher — polls for new SPED files and publishes
# file arrival events to Kafka.

# Uses existing BucketConnector.list_paths() for file discovery.
# Tracks processed files to ensure idempotent processing.
# """
# import os
# import time
# import json
# import logging
# import hashlib
# import re
# from typing import Set, Optional
# from connectors.bucket_connector import BucketConnector
# from schemas.identify_type_sped import SPEDIdentifier

# from kafka.producer import DinamoProducer
# from kafka.config import KafkaConfig
# from kafka.topics import TopicRegistry

# logger = logging.getLogger("dinamo.ingestion.file_watcher")


# class SpedFileWatcher:
#     """
#     Watches OCI bucket for new SPED files and publishes Kafka events.
    
#     Strategy:
#     - Polls bucket prefix at configurable interval
#     - Tracks processed files in local state file (survives restarts)
#     - Identifies SPED type from filename/path before publishing
#     - Publishes FileArrivedEvent to sped.raw.ingest topic
#     - Moves processed files to 'processed/' prefix
    
#     Usage:
#         watcher = SpedFileWatcher(
#             spark=spark,
#             bucket_name="datafoundation-agtax-evollux-prd",
#             kafka_config=KafkaConfig(),
#             watch_prefix="TO_CONVERT/",
#             poll_interval_sec=30,
#         )
#         watcher.start()  # blocking
#     """

#     STATE_FILE = "/opt/spark/checkpoints/file_watcher_state.json"

#     def __init__(
#         self,
#         spark,
#         bucket_name: str,
#         kafka_config: KafkaConfig,
#         watch_prefix: str = "TO_CONVERT/",
#         poll_interval_sec: int = 30,
#         file_extensions: tuple = (".txt",),
#         move_after_publish: bool = True,
#         processed_prefix: str = "processed/ingested/",
#     ):
#         self.bucket_connector = BucketConnector(spark, bucket_name)
#         self.sped_identifier = SPEDIdentifier()
#         self.producer = DinamoProducer(kafka_config)
#         self.bucket_name = bucket_name
#         self.watch_prefix = watch_prefix
#         self.poll_interval = poll_interval_sec
#         self.file_extensions = file_extensions
#         self.move_after_publish = move_after_publish
#         self.processed_prefix = processed_prefix
#         self._running = True
#         self._processed_files: Set[str] = set()
#         self._load_state()

#         logger.info(
#             f"FileWatcher initialized | bucket={bucket_name} | "
#             f"prefix={watch_prefix} | interval={poll_interval_sec}s"
#         )

#     def _load_state(self):
#         """Load processed files state from disk."""
#         try:
#             if os.path.exists(self.STATE_FILE):
#                 with open(self.STATE_FILE, "r") as f:
#                     state = json.load(f)
#                     self._processed_files = set(state.get("processed", []))
#                 logger.info(f"Loaded {len(self._processed_files)} processed files from state")
#         except Exception as e:
#             logger.warning(f"Could not load state file: {e}")

#     def _save_state(self):
#         """Persist processed files state to disk."""
#         try:
#             os.makedirs(os.path.dirname(self.STATE_FILE), exist_ok=True)
#             with open(self.STATE_FILE, "w") as f:
#                 json.dump({"processed": list(self._processed_files)}, f)
#         except Exception as e:
#             logger.warning(f"Could not save state file: {e}")

#     def _discover_new_files(self) -> list:
#         """Discover new files in the watched prefix."""
#         try:
#             all_files = self.bucket_connector.list_paths(self.watch_prefix)
#             new_files = [
#                 f for f in all_files
#                 if any(f.endswith(ext) for ext in self.file_extensions)
#                 and f not in self._processed_files
#             ]
#             return new_files
#         except Exception as e:
#             logger.error(f"Error discovering files: {e}")
#             return []

#     def _publish_file_event(self, file_path: str):
#         """Publish a file arrival event to Kafka."""
#         file_name = os.path.basename(file_path)
#         sped_type = self.sped_identifier.identify_type_sped_fast(file_path)

#         event = {
#             "file_path": file_path,
#             "file_name": file_name,
#             "file_size_bytes": 0,  # Could fetch from S3 head_object
#             "sped_type": sped_type,
#             "detected_cnpj": None,
#             "bucket_name": self.bucket_name,
#             "event_timestamp_ms": int(time.time() * 1000),
#             "source": "file_watcher",
#         }

#         # Use CNPJ from path as partition key, or file hash
#         partition_key = hashlib.md5(file_path.encode()).hexdigest()[:8]

#         self.producer.send(
#             topic=TopicRegistry.RAW_INGEST.name,
#             key=partition_key,
#             value=event,
#             headers={"sped_type": sped_type or "unknown"},
#         )

#         logger.info(f"Published file event | file={file_name} | type={sped_type}")

#     def _process_file(self, file_path: str):
#         """Process a single discovered file."""
#         try:
#             self._publish_file_event(file_path)
#             self._processed_files.add(file_path)
#             logger.info(f'Publish event in Kafka.')

#         except Exception as e:
#             logger.error(f"Error processing file {file_path}: {e}")

#     def poll_once(self) -> int:
#         """Execute a single poll cycle. Returns count of new files found."""
#         new_files = self._discover_new_files()
#         if new_files:
#             logger.info(f"Discovered {len(new_files)} new files")
#             for f in new_files:
#                 self._process_file(f)
#             self.producer.flush()
#             self._save_state()
#         return len(new_files)

#     def start(self):
#         """Start the polling loop (blocking)."""
#         logger.info(f"FileWatcher started | polling every {self.poll_interval}s")
#         try:
#             while self._running:
#                 self.poll_once()
#                 time.sleep(self.poll_interval)
#         except KeyboardInterrupt:
#             logger.info("FileWatcher interrupted")
#         finally:
#             self.stop()

#     def stop(self):
#         """Graceful shutdown."""
#         self._running = False
#         self._save_state()
#         self.producer.close()
#         logger.info("FileWatcher stopped")


# class ObjectStorageEventIngestion:
#     def __init__(self):
#         self.producer = DinamoProducer(KafkaConfig())

#     def extract_metadata(self, object_name: str):
#         metadata = re.search(r'(\d{14})/(\d{6})/', object_name)
#         if not metadata:
#             return None, None
        
#         return metadata.group(1), metadata.group(2)

#     def process_event(self, event):
#         object_name = event['data']['resourceName']
#         cnpj, periodo = self.extract_metadata(object_name)

#         payload = {
#             'file_path'         : object_name,
#             'cnpj'              : cnpj,
#             'periodo'           : periodo,
#             'event_timestamp_ms': int(time.time() * 1000),
#             'source'            : "oci_event",
#         }

#         self.producer.send(
#             topic   = TopicRegistry.RAW_INGEST.name,
#             key     = cnpj,
#             value   = payload,
#         )        

