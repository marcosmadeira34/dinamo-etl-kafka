    # dinamo_web/config/config_manager.py
import os
import yaml
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class ConfigManager:
    def __init__(self, base_path: Optional[str] = None):
        self.base_path = base_path or os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

        self.configs = {}
        self.logger = logger

    def load_yaml(self, filename: str):
        self.logger.info(f"Carregando configuracoes do arquivo {filename}")
        path = os.path.join(self.base_path, 'src', 'config', filename)
        self.logger.info(f"Carregando configuracoes do arquivo {filename} (path resolvido: {path})")
        if path not in self.configs:
            if not os.path.exists(path):
                self.logger.error(
                    f"Arquivo de config nao encontrado em {path} -- "
                    f"conteudo de {os.path.dirname(path)}: "
                    f"{os.listdir(os.path.dirname(path)) if os.path.isdir(os.path.dirname(path)) else 'DIRETORIO INEXISTENTE'}"
                )
            with open(path, 'r') as yaml_file:
                self.configs[path] = yaml.safe_load(yaml_file)
        return self.configs[path]


    @property
    def main_config(self):
        self.logger.info("Carregando configuracoes principais")
        file_config = self.load_yaml('main_config.yaml')
        if not file_config:
            raise ValueError("Arquivo de configuracoes principal nao encontrado")        
        return file_config

    @property
    def fiscal_config(self):
        self.logger.info("Carregando arquivo de configuracoes SPED FISCAL")
        file_config = self.load_yaml('fiscal_config.yaml')
        if not file_config:
            raise ValueError("Arquivo de configuracoes SPED FISCAL nao encontrado")        
        return file_config

    @property
    def contrib_config(self):
        self.logger.info("Carregando arquivo de configuracoes SPED CONTRIBUICOES")
        file_config = self.load_yaml('sped_contrib_config.yaml')
        if not file_config:
            raise ValueError("Arquivo de configuracoes SPED CONTRIBUIÇÕES nao encontrado")        
        return file_config

    @property
    def tax_regime_config(self):
        self.logger.info("Carregando arquivo de configuracoes REGIME TRIBUTARIO")
        file_config = self.load_yaml('tax_regime_config.yaml')
        if not file_config:
            raise ValueError("Arquivo de configuracoes REGIME TRIBUTARIO nao encontrado")        
        return file_config
            
    @property
    def cfop_config(self):
        self.logger.info("Carregando arquivo de configuracoes TABELA CFOP")
        file_config = self.load_yaml('tabela_cfop_config.yaml')
        if not file_config:
            raise ValueError("Arquivo de configuracoes TABELA CFOP nao encontrado")        
        return file_config

    @property
    def parent_child_config(self):
        self.logger.info("Carregando arquivo de configuracoes PAIS FILHOS")
        file_config = self.load_yaml('parent_child_config.yaml')
        if not file_config:
            raise ValueError("Arquivo de configuracoes PAIS FILHOS nao encontrado")        
        return file_config

    @property
    def inheritance_config(self):
        self.logger.info("Carregando arquivo de configuracoes HERANCA")
        file_config = self.load_yaml('inheritance_config.yaml')
        if not file_config:
            raise ValueError("Arquivo de configuracoes HERANCA nao encontrado")        
        return file_config

    @property
    def subvencao_config(self):
        self.logger.info("Carregando arquivo de configuracoes subvencao")
        file_config = self.load_yaml('subvencao_config.yaml')
        if not file_config:
            raise ValueError("Arquivo de configuracoes subvencao nao encontrado")        
        return file_config