# identify_type_sped.py

from __future__ import annotations

import logging
from typing import Optional, TYPE_CHECKING, Dict, List, Optional, Tuple
import re
import unicodedata
import os
from collections import defaultdict

# Type hint condicional para Spark para evitar erro em ambientes sem PySpark
if TYPE_CHECKING:
    from pyspark.sql import DataFrame
     
else:
    # Fallback para desenvolvimento local (sem Spark)
    DataFrame = object
    F = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)


class SPEDIdentifier:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
    """
    Classe auxiliar para identificar qual tipo de arquivo 
    SPED foi carregado do bucket OCI para aplicar o schema 
    correto após a leitura e parse do arquivo
    """

    def normalize(self, text: str) -> str:
        text = text.lower()
        text = unicodedata.normalize("NFD", text)
        text = "".join(c for c in text if unicodedata.category(c) != "Mn")
        return text

    def identify_type_sped_fast(self, path: str) -> Optional[str]:
        self.logger.info(f"Identificando rapidamente: {path}")

        filename = os.path.basename(path)
        fn_norm = self.normalize(filename)
        path_n = self.normalize(path)

        # 🔥 PRIORIDADE 1 — nome do arquivo (mais confiável)
        if "spedecf" in fn_norm:
            return "ECF"
        elif fn_norm.startswith("lecd") or "sped_ecd" in fn_norm:
            return "ECD"
        elif fn_norm.startswith("pisconfins"):
            return "EFD_CONTRIB"

        # 🔥 PRIORIDADE 2 — path (ordem correta)
        if re.search(r"/ecf/", path_n):
            return "ECF" 
        elif re.search(r"/ecd/", path_n):
            return "ECD"
        elif re.search(r"/efd[\s_-]?(contribuicoes|contribuicao)/", path_n):
            return "EFD_CONTRIB"
        elif re.search(r"/efd[\s_-]?fiscal/", path_n):
            return "EFD_FISCAL"

        return None

    def identify_type_sped(self, df_raw: DataFrame, path: str) -> Optional[str]:
        from pyspark.sql import functions as F

        self.logger.info(f"Identificando tipo de SPED para o arquivo: {path}")
        """
        Identifica o tipo do SPED usando uma estratégia hierárquica:
        1. Nome do arquivo (ex: LECD_..., PISCOFINS_...)
        2. Caminho do arquivo (ex: /ecd/, /efd_contribuicoes/)
        3. conteudo do arquivo (Codigo no registro 0000 - Campo 2)
        
        Retorna:
         - "ECD"
         - "EFD_CONTRIB"
         - "EFD_FISCAL"
         - None (se nao for possível identificar)
        """
        filename = os.path.basename(path)

        # --- PRIORIDADE 1: VERIFICAÇÃO PELO NOME DO ARQUIVO ---
        self.logger.info(f"Verificando tipo de SPED pelo nome do arquivo: {filename}")
        fn_norm = self.normalize(filename)
        
        if fn_norm.startswith("pisconfins"):
            self.logger.info("SPED identificado como EFD_CONTRIB pelo nome do arquivo.")
            return "EFD_CONTRIB"
        elif fn_norm.startswith("lecd") or "sped_ecd" in fn_norm or "escrituracaocontabil" in fn_norm:
            self.logger.info("SPED identificado como ECD pelo nome do arquivo.")
            return "ECD"

        # --- PRIORIDADE 2: VERIFICAÇÃO PELO CAMINHO (FALLBACK) ---
        self.logger.info(f"Nome do arquivo nao conclusivo. Verificando pelo caminho: {path}")
        path_n = self.normalize(path)
        
        if re.search(r"/efd[\s_-]?(contribuicoes|contribuicao)/", path_n):
            self.logger.info("SPED identificado como EFD_CONTRIB pelo caminho.")
            return "EFD_CONTRIB"
        elif re.search(r"/efd[\s_-]?fiscal/", path_n):
            self.logger.info("SPED identificado como EFD_FISCAL pelo caminho.")
            return "EFD_FISCAL"
        elif re.search(r"/(ecf|ECF)/", path_n):
            self.logger.info("SPED identificado como ECF pelo caminho.")
            return "ECF"
        elif re.search(r"/(ecd|ECD)/", path_n):
            self.logger.info("SPED identificado como ECD pelo caminho.")
            return "ECD"

        # --- PRIORIDADE 3: VERIFICAÇÃO PELO conteudo (ÚLTIMO RECURSO) ---
        self.logger.warning("Nome e caminho nao conclusivos. Tentando identificar pelo conteudo (registro 0000).")
        try:
            df_0000 = df_raw.filter(F.col("value").startswith("|0000|"))
            sample = df_0000.limit(1).collect()

            if sample:
                line = sample[0]["value"]
                fields = line.split("|")
                
                # O campo 2 (índice 2) do registro 0000 guarda o Codigo identificador do layout
                # Exemplo: |0000|LECD|... -> fields[2] == 'LECD'
                if len(fields) > 2:
                    cod_layout = fields[2].strip()
                    self.logger.info(f"DEBUG: Linha do registro 0000: {line}")
                    self.logger.info(f"DEBUG: Codigo do layout detectado no campo 2: '{cod_layout}'")
                    if cod_layout == "LECF":
                        self.logger.info("SPED identificado como ECF pelo conteudo.")
                        return "ECF"
                    elif cod_layout == "LECD":
                        self.logger.info("SPED identificado como ECD pelo conteudo.")
                        return "ECD"
                    # Mantendo a lógica antiga de fallback para EFDs, embora olhar o Codigo seja melhor
                    elif len(fields) == 16:
                        self.logger.info("SPED identificado como EFD_CONTRIB pelo conteudo (qtd campos).")
                        return "EFD_CONTRIB"
                    elif len(fields) == 17:
                        self.logger.info("SPED identificado como EFD_FISCAL pelo conteudo (qtd campos).")
                        return "EFD_FISCAL"
                else:
                    self.logger.warning("Registro 0000 nao possui campos suficientes para análise.")

        except Exception as e:
            self.logger.error(f"Erro ao tentar identificar pelo conteudo: {e}")

        # --- SE TUDO FALHAR ---
        self.logger.error(f"Nao foi possivel identificar o tipo do SPED para o arquivo: {path}")
        return None

    def is_valid_sped_path(self, path: str) -> bool:
        self.logger.info(f"Verificando se o caminho contém um padrão conhecido de SPED: {path}")
        """
        Verifica se o caminho contém um padrão conhecido de SPED.
        Este método pode ser mantido para o filtro inicial, se desejar.
        """
        path_n = self.normalize(path)
        patterns = [
            r"/efd[\s_-]?fiscal/",
            r"/efd[\s_-]?(contribuicoes|contribuicao)/",
            r"/(ecd|escrituracao[\s_-]?contabil)/", 
            r"/(ecf|ECF)/",
        ]   
        return any(re.search(p, path_n) for p in patterns)
        
    def get_sped_key(self, path: str) -> str | None:
        """
        Extrai a chave lógica do SPED ignorando Original/Retificadora.
        """

        filename = os.path.basename(path)

        pattern = (
            r"^(.*?)_"                  # tipo
            r"(\d{8})_"                 # dt inicial
            r"(\d{8})_"                 # dt final
            r"(\d{14})_"                # cnpj
            r"(original|retificadora)" # tipo envio
        )

        match = re.search(pattern, filename, re.IGNORECASE)

        if not match:
            return None

        tipo = match.group(1)
        dt_ini = match.group(2)
        dt_fim = match.group(3)
        cnpj = match.group(4)

        return f"{tipo}_{dt_ini}_{dt_fim}_{cnpj}".lower()

    def is_retificadora(self, path: str) -> bool:
        filename = os.path.basename(path).lower()
        return "_retificadora_" in filename

    def filter_retificadoras(self, paths: List[str]) -> List[str]:
        """
        Se existir arquivo retificador para a mesma competência,
        mantém apenas o retificador e ignora o original.
        Caso contrário, mantém o original.
        """

        grouped = defaultdict(list)
        final_files = []

        for path in paths:
            key = self.get_sped_key(path)

            # Arquivos fora do padrão continuam no processamento
            if not key:
                final_files.append(path)
                continue

            grouped[key].append(path)

        for files in grouped.values():

            retificadoras = [
                file for file in files
                if self.is_retificadora(file)
            ]

            if retificadoras:
                final_files.extend(retificadoras)
            else:
                final_files.extend(files)

        return final_files
        