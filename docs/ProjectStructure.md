# Estrutura Organizacional do Código (ProjectStructure.md)

Este documento descreve a estrutura de diretórios, a responsabilidade de cada módulo de código e o fluxo de chamadas entre os componentes da plataforma **DINAMO**. Todas as informações estão alinhadas com as descobertas registradas no [Discovery_Report.md](file:///${HOME}/dinamo_etl_project/Discovery_Report.md).

---

## 1. Árvore de Diretórios

Abaixo é apresentada a organização de pastas e os principais arquivos do projeto DINAMO:

```
dinamo_etl_project/
├── Discovery_Report.md               # Compilação de evidências da arquitetura
├── README.MD                         # Manual inicial e arquitetura GOLD
├── docker-compose.yml                # Configuração simplificada (se aplicável)
├── requirements.txt                  # Dependências Python primárias
├── docs/                             # Documentação técnica oficial (esta pasta)
│   ├── Architecture.md
│   ├── DataPlatform.md
│   ├── Infrastructure.md
│   ├── SchemaArchitecture.md
│   ├── ADR.md
│   ├── TechnicalAssessment.md
│   └── ProjectStructure.md
├── infrastructure/                   # Infraestrutura Docker distribuída
│   ├── docker-compose.yml            # Zookeeper, Kafka, Schema Registry, Spark, Prometheus
│   ├── spark/                        # Dockerfile do Apache Spark customizado
│   └── [configs e scripts]/          # Grafana dashboards, configs de Prometheus e Kafka
└── dinamo_web/                       # Código principal da aplicação
    ├── config/                       # Arquivos de configurações estruturados
    │   ├── main_config.yaml          # Parametrizações de S3 e Spark Cluster
    │   ├── business_rules.yaml       # CFOP, regimes e mappings fiscais comuns
    │   ├── sped_contrib_config.yaml  # Configurações de colunas e joins da Gold Contrib
    │   ├── sped_fiscal_config.yaml   # Configurações de colunas e joins da Gold Fiscal
    │   ├── sped_ecd_config.yaml      # Configurações de colunas e joins da Gold ECD
    │   └── sped_ecf_config.yaml      # Configurações de colunas e joins da Gold ECF
    ├── main_contrib_v2.py            # Orquestrador do pipeline EFD Contribuições
    ├── main_fiscal_v2.py             # Orquestrador do pipeline EFD Fiscal
    ├── main_ecd_v2.py                # Orquestrador do pipeline ECD
    ├── main_ecf_v2.py                # Orquestrador do pipeline ECF
    └── src/
        └── dinamo_web/               # Módulos empacotados da aplicação
            ├── connectors/           # Conectores para Buckets S3/OCI e DBs
            │   ├── bucket_connector.py
            │   └── db_connector.py
            ├── extractors/           # Leitura e parsing de arquivos texto brutos
            │   └── bucket_extractor.py
            ├── silver_sped_builder/  # Construção da camada Silver e UIDs
            │   └── silver_layer.py
            ├── gold_builder/         # Geração de tabelas de BI analíticas
            │   ├── base_gold_builder.py
            │   ├── gold_layer.py
            │   ├── gold_contrib_builder.py
            │   ├── gold_fiscal_builder.py
            │   ├── gold_ecd_builder.py
            │   └── gold_ecf_builder.py
            ├── schemas/              # Definições estáticas de StructTypes e mapas
            │   ├── register_type_sped.py
            │   ├── contrib_schema.py
            │   ├── fiscal_schema.py
            │   ├── ecd_schema.py
            │   └── ecf_schema.py
            └── utils/                # Funções utilitárias auxiliares
                └── spark_session.py  # Inicializador da sessão Spark customizada
```

---

## 2. Responsabilidade de Cada Módulo

### A. Entry Points (Orquestradores)
* **Localização:** Raiz de `dinamo_web/` (ex: `main_contrib_v2.py`, `main_fiscal_v2.py`).
* **Responsabilidade:** Carregar as configurações YAML, iniciar a sessão Spark, verificar arquivos processados na tabela Delta de checkpoint, executar o loop de leitura, disparar as transformações Silver e acionar a geração da camada Gold correspondente.

### B. Connectors (Conectores)
* **Localização:** `src/dinamo_web/connectors/`
* **Responsabilidade:** Abstrair o acesso a sistemas de arquivos em nuvem (S3/OCI) e bancos de dados externos (PostgreSQL/MySQL), fornecendo listagens de arquivos e instâncias Spark adequadas.

### C. Extractors (Extratores)
* **Localização:** `src/dinamo_web/extractors/`
* **Responsabilidade:** Implementar a lógica de leitura bruta do SPED (`read_txt`), separação física de linhas (`parse_sped_records`) e casting esquemático (`apply_sped_schema`).

### D. Silver Builder (Construção da Silver)
* **Localização:** `src/dinamo_web/silver_sped_builder/`
* **Responsabilidade:** Resolver a hierarquia de registros usando ordenação física por partição e máquina de estados para gerar os identificadores exclusivos (`_row_id` e `_parent_uid_final`).

### E. Gold Builder (Agregadores de BI)
* **Localização:** `src/dinamo_web/gold_builder/`
* **Responsabilidade:** Aplicar as regras estruturadas no YAML correspondente para realizar joins, projeções, aliasing e agrupamentos de negócio fiscais (Documento, Item e Total).

### F. Schemas (Modelagem Estática)
* **Localização:** `src/dinamo_web/schemas/`
* **Responsabilidade:** Guardar os tipos físicos do Spark (`StructType`) e mapas de delimitador posicional (`FIELDS_MAP`) de todos os registros governamentais suportados.

---

## 3. Fluxo de Chamadas entre Componentes

```
[Orquestrador (main)]
   │
   ├───> 1. Inicia Spark ───> [utils/spark_session.py]
   │
   ├───> 2. Lista Arquivos ──> [connectors/bucket_connector.py]
   │
   ├───> 3. Lê e Parseia TXT ─> [extractors/bucket_extractor.py]
   │
   ├───> 4. Resolve Hierarquia ─> [silver_sped_builder/silver_layer.py]
   │
   ├───> 5. Grava Silver Delta
   │
   └───> 6. Executa Gold Builder ──> [gold_builder/gold_*_builder.py]
```
