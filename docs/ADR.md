# Architectural Decision Records (ADR.md)

Este documento registra formalmente as decisões arquiteturais da plataforma **DINAMO**, inferidas a partir do código, arquivos de infraestrutura e logs documentados no [Discovery_Report.md](file:///Ubuntu/home/marcosmadeira/dinamo_etl_project/Discovery_Report.md).

---

# ADR-001: Utilização do Apache Spark vs. Pandas

* **Status:** Aceito.
* **Contexto:** Os arquivos TXT originais do SPED podem atingir gigabytes de tamanho, contendo milhões de linhas (registros de itens de notas fiscais). Ferramentas tradicionais de processamento em memória em um único nó (como o Pandas em Python) gerariam estouros de memória e lentidão inaceitável.
* **Alternativas Consideradas:** Pandas, Dask, processamento puro em Python.
* **Decisão:** Adotar Apache Spark (PySpark) como motor principal de processamento distribuído.
* **Benefícios:** Processamento distribuído real, escalabilidade horizontal e paralelismo nativo.
* **Consequências:** Necessidade de gerenciar a infraestrutura do Spark Cluster (Master e Workers) e complexidade no empacotamento de dependências.
* **Tradeoffs:** Aumento da latência de inicialização do job em troca de capacidade ilimitada de processamento horizontal.
* *Evidência de rastreabilidade: [Discovery_Report.md - Seção 1.A e 4](file:///Ubuntu/home/marcosmadeira/dinamo_etl_project/Discovery_Report.md#a-camada-bronze-leitura-bruta).*

---

# ADR-002: Adoção do Delta Lake para a Camada Silver

* **Status:** Aceito.
* **Contexto:** Arquivos do SPED sofrem atualizações frequentes e mudanças de layout ao longo do tempo. É imperativo que a base histórica de dados aceite evoluções de schema e possua controle transacional.
* **Alternativas Consideradas:** Parquet puro, CSV, Bancos de Dados Relacionais Tradicionais.
* **Decisão:** Armazenar os dados estruturados da camada Silver no formato Delta Lake com a evolução de schema habilitada (`option("mergeSchema", "true")`).
* **Benefícios:** Garantia de transações ACID, versionamento de dados (*time-travel*) e evolução de schema sem a necessidade de reprocessamento completo do histórico.
* **Consequências:** Aumento do overhead de gravação do Spark devido ao gerenciamento do log de transações do Delta Lake.
* **Tradeoffs:** Pequena perda de desempenho de gravação em troca de robustez e integridade nos metadados.
* *Evidência de rastreabilidade: [Discovery_Report.md - Seção 5.B](file:///Ubuntu/home/marcosmadeira/dinamo_etl_project/Discovery_Report.md#b-escolha-do-delta-lake-como-formato-da-camada-silver).*

---

# ADR-003: Arquitetura Medallion para Fluxo de Dados

* **Status:** Aceito.
* **Contexto:** Dados brutos do SPED precisam passar por limpezas e validações profundas antes de estarem prontos para o BI, mas o histórico original deve ser guardado para auditorias fiscais futuras.
* **Alternativas Consideradas:** ETL clássico de uma etapa direto para banco relacional.
* **Decisão:** Separar o fluxo de dados em três estágios lógicos: Bronze (leitura de texto original sem transformação), Silver (dados estruturados, validados e enriquecidos com UIDs) e Gold (agregados fiscais).
* **Benefícios:** Desacoplamento de estágios, facilidade de auditoria e reprocessamento a partir de qualquer camada.
* **Consequências:** Aumento do custo de armazenamento de dados brutos e redundantes nas camadas iniciais.
* **Tradeoffs:** Custo adicional de storage em troca de rastreabilidade e flexibilidade analítica.
* *Evidência de rastreabilidade: [Discovery_Report.md - Seção 1](file:///Ubuntu/home/marcosmadeira/dinamo_etl_project/Discovery_Report.md#1-arquitetura-medallion-camadas-bronze-silver-e-gold).*

---

# ADR-004: Estratégia de Parent UID / UID para Vínculos Hierárquicos

* **Status:** Aceito.
* **Contexto:** Os arquivos do SPED utilizam ordem física sequencial de linhas para vincular filhos a pais. O Spark lê arquivos em paralelo distribuído, o que por padrão quebra essa ordem sequencial.
* **Alternativas Consideradas:** Joins relacionais posicionais complexos.
* **Decisão:** Agrupar os dados por arquivo original (`_source_file`), ordenar por `_row_id` dentro das partições do Spark (`sortWithinPartitions`) e executar um iterador linear customizado via `mapPartitions` para computar os IDs de pai e filhos usando uma máquina de estados local.
* **Benefícios:** Resolução precisa e deterministicamente correta de relacionamentos pai-filho sem a necessidade de joins pesados entre partições do Spark.
* **Consequências:** Limita o processamento paralelo de um único arquivo à capacidade de processamento de um único nó do cluster.
* **Tradeoffs:** Risco de lentidão (*skew*) em arquivos gigantescos em troca da garantia absoluta de corretude de vínculo.
* *Evidência de rastreabilidade: [Discovery_Report.md - Seção 2 e 5.A](file:///Ubuntu/home/marcosmadeira/dinamo_etl_project/Discovery_Report.md#2-estratgia-de-parent-uid--uid).*

---

# ADR-005: Processamento Baseado em Metadados (Metadata-Driven Processing)

* **Status:** Aceito.
* **Contexto:** O DINAMO precisa gerar dezenas de tabelas Gold analíticas diferentes a partir dos mesmos arquivos Silver, dependendo da obrigação (ECD, ECF, Fiscal, Contribuições). Codificar cada relatório de forma estática geraria redundância de código e custos elevados de manutenção.
* **Alternativas Consideradas:** Scripts Spark estáticos para cada relatório.
* **Decisão:** Construir um motor genérico (`BaseGoldBuilder` e `ReportBuilder`) que lê as regras de negócio declaradas em arquivos YAML, gerando joins e filtros de forma dinâmica.
* **Benefícios:** A adição de novos relatórios ou alteração de colunas é feita editando arquivos YAML, sem a necessidade de alterar código Python ou reimplantar a aplicação.
* **Consequências:** Dificuldade em depurar erros em tempo de compilação ou de forma estática no Spark.
* **Tradeoffs:** Perda de verificação estática de tipos no editor de código em troca de flexibilidade e agilidade operacional.
* *Evidência de rastreabilidade: [Discovery_Report.md - Seção 3.B](file:///Ubuntu/home/marcosmadeira/dinamo_etl_project/Discovery_Report.md#3-padres-arquiteturais-identificados).*

---

# ADR-006: Parametrização Baseada em YAML (YAML Driven Configuration)

* **Status:** Aceito.
* **Contexto:** Constantes do pipeline (como endpoints, buckets e credenciais) e mapeamento hierárquico SPED variam entre ambientes de desenvolvimento e produção.
* **Alternativas Consideradas:** Variáveis de ambiente exclusivas, arquivos de propriedades Java.
* **Decisão:** Manter configurações estruturadas em arquivos YAML locais (`sped_contrib_config.yaml`, `sped_fiscal_config.yaml`, etc.) carregados via módulo Python `yaml`.
* **Benefícios:** Legibilidade humana excelente e suporte a estruturas aninhadas complexas.
* **Consequências:** Risco de erros de indentação ou parsing de strings inválidas.
* **Tradeoffs:** Complexidade marginal de parsing em troca de clareza nas configurações estruturadas.
* *Evidência de rastreabilidade: [Discovery_Report.md - Seção 1 (Módulos Fiscais SPED Processados)](file:///Ubuntu/home/marcosmadeira/dinamo_etl_project/Discovery_Report.md#1-mdulos-fiscais-sped-processados).*

---

# ADR-007: Cluster de Mensageria Distribuído com Apache Kafka

* **Status:** Aceito.
* **Contexto:** O processamento em tempo real ou quase real de eventos de arquivos fiscais provenientes de diversas fontes externas requer desacoplamento físico e retenção robusta dos eventos.
* **Alternativas Consideradas:** Filas RabbitMQ, Filas AWS SQS.
* **Decisão:** Implantar uma infraestrutura distribuída contendo 3 brokers Kafka ativos com Zookeeper para coordenação interna.
* **Benefícios:** Alta disponibilidade, persistência durável em disco, escalabilidade por partições e capacidade de reprocessar mensagens (*offset reset*).
* **Consequências:** Complexidade de gerenciamento de infraestrutura (Zookeeper, logs, brokers, partições).
* **Tradeoffs:** Alto custo operacional e infraestrutura robusta em troca de consistência, ordenação e altíssima taxa de vazão (*throughput*).
* *Evidência de rastreabilidade: [Discovery_Report.md - Seção 4](file:///Ubuntu/home/marcosmadeira/dinamo_etl_project/Discovery_Report.md#4-arquitetura-kafka--schema-registry).*

---

# ADR-008: Adoção do Confluent Schema Registry

* **Status:** Aceito.
* **Contexto:** Eventos de SPED trafegados no cluster Kafka precisam manter conformidade de schema sem quebra de pipelines de consumo a jusante (*downstream*).
* **Alternativas Consideradas:** Validação manual de JSON nos consumidores.
* **Decisão:** Utilizar o Schema Registry da Confluent configurado em nível de compatibilidade reversa (`BACKWARD`).
* **Benefícios:** Validação automática de contratos de schema e evolução segura de layouts fiscais.
* **Consequências:** Introdução de uma dependência síncrona obrigatória durante a publicação e consumo de tópicos.
* **Tradeoffs:** Complexidade acrescida no ciclo de deploy de produtores e consumidores em troca de segurança e governança de contratos de dados.
* *Evidência de rastreabilidade: [Discovery_Report.md - Seção 4.A](file:///Ubuntu/home/marcosmadeira/dinamo_etl_project/Discovery_Report.md#a-servios-da-stack-de-mensageria).*
