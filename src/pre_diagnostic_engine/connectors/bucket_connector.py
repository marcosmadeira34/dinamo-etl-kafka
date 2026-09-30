 # ./srcdinamo_web/connectors/bucket_connector.py
from pyspark.sql import SparkSession, DataFrame
from .base_connector import BaseConnector
import logging
import boto3
from typing import List, Generator, Optional
import os
import io
import xml.etree.ElementTree as ET
import hashlib
import base64
from botocore.exceptions import ClientError
from botocore.config import Config
from dotenv import load_dotenv
    
load_dotenv()

logger = logging.getLogger(__name__)

class BucketConnector:
    """
    Conector simples e compatível com OCI, MinIO e AWS.
    Toda a navegação do bucket é feita via boto3, nunca via Spark.
    """

    def __init__(self, spark: SparkSession, bucket_name: str):
        self.spark = spark
        self.bucket_name = bucket_name

        endpoint = os.getenv("AWS_ENDPOINT_URL").strip()    
        access_key = os.getenv("AWS_ACCESS_KEY_ID").strip()
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY").strip()
        # region = os.getenv("AWS_DEFAULT_REGION", "").strip()

        config = Config(signature_version="s3v4")


        if not endpoint:
            raise RuntimeError("AWS_ENDPOINT_URL não configurado.")
        if not access_key or not secret_key:
            raise RuntimeError("AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY não configurados.")

        # print(f"REGION DEBUG -> '{region}'")

        self.s3 = boto3.client(
            "s3",   
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",
            config=config
            
        )
        
    def path_exists(self, full_path: str) -> bool:
        """
        Verifica se um objeto específico (arquivo) existe no S3.
        Mais eficiente e direto do que list_paths para verificar um único arquivo.
        """
        try:
            # Remove o prefixo s3a:// ou s3:// se presente
            if full_path.startswith("s3a://") or full_path.startswith("s3://"):
                full_path = full_path.split("://", 1)[1]
            
            bucket, key = full_path.split("/", 1)
            # Tenta obter os metadados do objeto. Se não existir, levanta um erro 404.
            self.s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                # O arquivo não foi encontrado
                return False
            else:
                # Outro erro (ex: permissão negada), propaga a exceção
                logger.error(f"Erro ao verificar existência do caminho {full_path}: {e}")
                raise

    def move_file(self, source_key: str, dest_key: str) -> None:
        """
        Move um arquivo dentro do bucket de source_key para dest_key.
        """
        copy_source = {
            'Bucket': self.bucket_name,
            'Key': source_key
        }
        self.s3.copy_object(CopySource=copy_source, Bucket=self.bucket_name, Key=dest_key)
        self.s3.delete_object(Bucket=self.bucket_name, Key=source_key)
        logger.info(f"Arquivo movido de {source_key} para {dest_key}")

    def delete_prefix(self, prefix: str):
        paginator = self.s3.get_paginator('list_objects_v2')

        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            if 'Contents' not in page:
                continue
            
            for obj in page['Contents']:
                key = obj['Key']
                self.s3.delete_object(
                    Bucket=self.bucket_name,
                    Key=key
                )
                logger.info(f"Deletado: s3://{self.bucket_name}/{key}")

        logger.info(f"Prefixo deletado: {prefix}")

    def download_file(self, key: str, local_path: str) -> None:
        self.s3.download_file(Bucket=self.bucket_name, Key=key, Filename=local_path)
        logger.info(f"Arquivo baixado: s3://{self.bucket_name}/{key} -> {local_path}")

    def _get_full_path(self, path: str) -> str:
        """Método auxiliar para construir a URI completa do bucket"""
        clean_path = path.lstrip('/')
        return f"s3a://{self.bucket_name}/{clean_path}"
    
    def mkdir(self, prefix: str) -> None:
        prefix = prefix.strip("/") + "/"
        self.s3.put_object(Bucket=self.bucket_name, Key=prefix, Body=b"")
        logger.info(f"Pasta criada: s3://{self.bucket_name}/{prefix}")

    def upload_file(self, local_path: str, key: str):
        """
        Envia um arquivo local para o bucket.
        Exemplo:
            upload_file("dados/arquivo.txt", "raw/arquivo.txt")
        """
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Arquivo local não encontrado: {local_path}")

        self.s3.upload_file(
            Filename=local_path,
            Bucket=self.bucket_name,
            Key=key
        )
        logger.info(f"Upload concluído: {local_path} -> s3://{self.bucket_name}/{key}")

    def upload_bytes(self, data: bytes, key: str) -> None:
        bio = io.BytesIO(data)
        self.s3.upload_fileobj(bio, self.bucket, key)
        logger.info(f"Upload de bytes -> s3://{self.bucket}/{key}")
         
    def list_paths(self, prefix: str = "") -> List[str]:
        """
        Lista todas as chaves sob um prefix específico (modo simples).
        """
        keys = []
        continuation_token = None

        while True:
            response = (self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix, ContinuationToken=continuation_token)
                if continuation_token else self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            )

            for obj in response.get("Contents", []):
                keys.append(obj["Key"])

            if response.get("IsTruncated"):
                continuation_token = response.get("NextContinuationToken")
            else:
                break

        return keys
    
    def walk(self, prefix: str = "") -> Generator[tuple, None, None]:
        paginator = self.s3.get_paginator("list_objects_v2")
        prefix = prefix.rstrip("/") + "/" if prefix else ""

        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix, Delimiter="/"):

            prefixes = [p["Prefix"] for p in page.get("CommonPrefixes", [])]
            files = [c["Key"] for c in page.get("Contents", [])]

            yield prefix, prefixes, files

            for sub in prefixes:
                yield from self.walk(sub)

    def exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except:
            return False

    def read_csv(self, path: str, header: bool = True, infer_schema: bool = True,
                 delimiter: str = ";") -> DataFrame:
        """Lê os arquivos CSV de um caminho no bucket"""
        full_path = self._get_full_path(path)
        logger.info(f"Lendo arquivo CSV do Bucket em: {full_path}")
        return self.spark.read.csv(full_path, header=header, inferSchema=infer_schema, sep=delimiter)
    
    def read_parquet(self, path: str) -> DataFrame:
        """Lê os arquivos Parquet de um bucket"""
        full_path = self._get_full_path(path)
        logger.info(f"Lendo arquivo Parquet do Bucket em {full_path}")
        return self.spark.read.parquet(full_path)
    
    def write_parquet(self, df: DataFrame, path: str, mode: str = "overwrite", partition_by: list = None):
        """
        Escreve um DataFrame em formato parquet no bucket
        """
        full_path = self._get_full_path(path)
        logger.info(f"Escrevendo arquivo Parquet no bucket em {full_path}")
        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.parquet(full_path)

    def read_text_file(self, key: str) -> str:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read().decode("utf-8")
    
    def download_file_bytes(self, key: str) -> bytes:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read()
    
