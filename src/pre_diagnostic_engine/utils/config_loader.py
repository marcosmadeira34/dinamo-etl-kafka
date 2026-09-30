import yaml
import os
from typing import Optional, Dict


def load_config(config_name: Optional[str] = None) -> Dict:
    """
    Carrega arquivo YAML em src/pre_diagnostic_engine/config/.
    Se nenhum nome for passado, usa 'main_config.yaml'.
    Retorna dicionário de configuração.
    """
    if config_name is None:
        config_name = "main_config.yaml"

    if not config_name.endswith(".yaml"):
        config_name += ".yaml"

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "config", config_name)

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Arquivo de configuração não encontrado: {config_path}")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    return config