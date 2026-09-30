from pyspark.sql.types import *

company_tax_profile_schema = StructType([
    StructField("cnpj", StringType(), False),
    StructField("razao_social", StringType(), True),
    StructField("ano_calendario", IntegerType(), False),
    StructField("regime_history", ArrayType(StructType([
        StructField("ano", IntegerType(), True),
        StructField("regime", StringType(), True),
        StructField("points", IntegerType(), True),
        StructField("eligible", BooleanType(), True)
    ])), True),
    StructField("patrimonio_history", StructType([
        StructField("patrimonio_liquido", DoubleType(), True),
        StructField("evolucao_patrimonial", DoubleType(), True),
        StructField("capacidade_jcp", DoubleType(), True)
    ]), True),
    StructField("contabil_history", StructType([
        StructField("lucro_liquido", DoubleType(), True),
        StructField("prejuizo_contabil", DoubleType(), True),
        StructField("recorrencia_lucro_anos", IntegerType(), True),
        StructField("recorrencia_prejuizo_anos", IntegerType(), True)
    ]), True),
    StructField("fiscal_history", StructType([
        StructField("lucro_fiscal", DoubleType(), True),
        StructField("prejuizo_fiscal", DoubleType(), True),
        StructField("base_calculo", DoubleType(), True),
        StructField("recorrencia_fiscal_anos", IntegerType(), True)
    ]), True),
    StructField("retentions", StructType([
        StructField("irpj_retido", DoubleType(), True),
        StructField("csll_retido", DoubleType(), True),
        StructField("divergencia_retencao", BooleanType(), True),
        StructField("potencial_compensacao", DoubleType(), True)
    ]), True),
    StructField("darf_payments", ArrayType(StructType([
        StructField("periodo_apuracao", StringType(), True),
        StructField("codigo_receita", StringType(), True),
        StructField("valor_principal", DoubleType(), True)
    ])), True),
    StructField("identified_opportunities", ArrayType(StructType([
        StructField("opportunity_id", StringType(), True),
        StructField("description", StringType(), True),
        StructField("reference_code", StringType(), True),
        StructField("estimated_credit", DoubleType(), True),
        StructField("score_impact", DoubleType(), True)
    ])), True),
    StructField("score", DoubleType(), True),
    StructField("classification", StringType(), True),
    StructField("go_no_go", StringType(), True),
    StructField("estimated_recovery", DoubleType(), True),
    StructField("commercial_priority", StringType(), True)
])
