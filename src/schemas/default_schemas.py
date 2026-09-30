# dinamo_web/src/dinamo_web/schemas/contrib_schema.py
from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, 
        DecimalType, DateType, TimestampType, DecimalType, DateType
    )
from pyspark.sql import functions as F
from functools import reduce
    

class SpedContribSchemas:
    """
    Mapeamento e definição dos schemas e mapas de campos dos registros do SPED Contribuicoes
    
    """
    SPED_CONTRIB_SCHEMAS = {
        
        '0000': StructType([
            StructField('REG', StringType(), True),
            StructField('COD_VER', StringType(), True),
            StructField('TIPO_ESCRIT', StringType(), True),
            StructField('IND_SIT_ESP', StringType(), True),
            StructField('NUM_REC_ANTERIOR', StringType(), True),
            StructField('DT_INI', StringType(), True),
            StructField('DT_FIN', StringType(), True),
            StructField('NOME', StringType(), True),
            StructField('CNPJ', StringType(), True),
            StructField('UF', StringType(), True),
            StructField('COD_MUN', StringType(), True),
            StructField('SUFRAMA', StringType(), True),
            StructField('IND_NAT_PJ', StringType(), True),
            StructField('IND_ATIV', StringType(), True),
            
        ]),
        "0110": StructType([
            StructField('REG', StringType(), True),
            StructField('COD_INC_TRIB', StringType(), True),
            StructField('IND_APRO_CRED', StringType(), True),
            StructField('COD_TIPO_CONT', StringType(), True),
            StructField('IND_REG_CUM', StringType(), True),
        ]),
        "0140": StructType([
            StructField('REG', StringType(), True),
            StructField('COD_ESTABELECIMENTO', StringType(), True),
            StructField('NOME_ESTABELECIMENTO', StringType(), True),
            StructField('CNPJ_ESTABELECIMENTO', StringType(), True),
            StructField('UF_ESTABELECIMENTO', StringType(), True),
            StructField('IE_ESTABELECIMENTO', StringType(), True),
            StructField('COD_MUN_ESTABELECIMENTO', StringType(), True),
            StructField('IM_ESTABELECIMENTO', StringType(), True),
            StructField('SUFRAMA_ESTABELECIMENTO', StringType(), True),

        ]),
        "0150": StructType([
            StructField('REG', StringType(), True),
            StructField('COD_PART', StringType(), True),
            StructField('NOME_PARTICIPANTE', StringType(), True),
            StructField('COD_PAIS', StringType(), True),
            StructField('CNPJ_PARTICIPANTE', StringType(), True),
            StructField('CPF_PARTICIPANTE', StringType(), True),
            StructField('IE_PARTICIPANTE', StringType(), True),
            StructField('COD_MUN_PARTICIPANTE', StringType(), True),
            StructField('SUFRAMA_PARTICIPANTE', StringType(), True),
            StructField('END_PARTICIPANTE', StringType(), True),
            StructField('NUM_PARTICIPANTE', StringType(), True),
            StructField('COMPL_PARTICIPANTE', StringType(), True),
            StructField('BAIRRO_PARTICIPANTE', StringType(), True)
        ]),
        "0200": StructType([
            StructField('REG', StringType(), True),
            StructField('COD_ITEM', StringType(), True),
            StructField('DESCR_ITEM', StringType(), True),
            StructField('COD_BARRA', StringType(), True),
            StructField('COD_ANT_ITEM', StringType(), True),
            StructField('UNID_INV', StringType(), True),
            StructField('TIPO_ITEM', StringType(), True),
            StructField('COD_NCM', StringType(), True),
            StructField('EX_IPI', StringType(), True),
            StructField('COD_GEN', StringType(), True),
            StructField('COD_LST', StringType(), True),
            StructField('ALIQ_ICMS', StringType(), True),
        ]),
        "0500": StructType([
            StructField('REG', StringType(), True),
            StructField('DT_ALT', StringType(), True),
            StructField('COD_NAT_CC', StringType(), True),
            StructField('IND_CTA', StringType(), True),
            StructField('NIVEL', StringType(), True),
            StructField('COD_CTA', StringType(), True),
            StructField('NOME_CTA', StringType(), True),
            StructField('COD_CTA_REF', StringType(), True),
            StructField('CNPJ_EST', StringType(), True)
        ]),
        'C010': StructType([
            StructField('REG', StringType(), True),
            StructField('CNPJ_C010', StringType(), True),
            StructField('IND_ESCRI', StringType(), True),
        ]),
        'C100': StructType([
            StructField('REG', StringType(), True), StructField('IND_OPER', StringType(), True),
            StructField('IND_EMIT', StringType(), True), StructField('COD_PART', StringType(), True),
            StructField('COD_MOD', StringType(), True), StructField('COD_SIT', StringType(), True),
            StructField('SER', StringType(), True), StructField('NUM_DOC', StringType(), True),
            StructField('CHV_NFE', StringType(), True), StructField('DT_DOC', StringType(), True),
            StructField('DT_E_S', StringType(), True), StructField('VL_DOC', StringType(), True),
            StructField('IND_PGTO', StringType(), True), StructField('VL_DESC', StringType(), True), 
            StructField('VL_ABAT_NT', StringType(), True), StructField('VL_MERC', StringType(), True),
            StructField('IND_FRT', StringType(), True), StructField('VL_FRT', StringType(), True),
            StructField('VL_SEG', StringType(), True), StructField('VL_OUT_DA', StringType(), True),
            StructField('VL_BC_ICMS', StringType(), True), StructField('VL_ICMS', StringType(), True),
            StructField('VL_BC_ICMS_ST', StringType(), True), StructField('VL_ICMS_ST', StringType(), True), 
            StructField('VL_IPI', StringType(), True), StructField('VL_PIS', StringType(), True), 
            StructField('VL_COFINS', StringType(), True), StructField('VL_PIS_ST', StringType(), True),  
            StructField('VL_COFINS_ST', StringType(), True),
        ]),
        'C170': StructType([
            StructField('REG', StringType(), True), StructField('NUM_ITEM', StringType(), True),
            StructField('COD_ITEM', StringType(), True), StructField('DESCR_COMPL', StringType(), True),
            StructField('QTD', StringType(), True), StructField('UNID', StringType(), True),
            StructField('VL_ITEM', StringType(), True), StructField('VL_DESC', StringType(), True),
            StructField('IND_MOV', StringType(), True), StructField('CST_ICMS', StringType(), True),
            StructField('CFOP', StringType(), True), StructField('COD_NAT', StringType(), True),
            StructField('VL_BC_ICMS_ITEM', StringType(), True), StructField('ALIQ_ICMS_ITEM', StringType(), True),
            StructField('VL_ICMS_ITEM', StringType(), True), StructField('VL_BC_ICMS_ST_ITEM', StringType(), True),
            StructField('ALIQ_ST', StringType(), True), StructField('VL_ICMS_ST_ITEM', StringType(), True),
            StructField('IND_APUR', StringType(), True), StructField('CST_IPI', StringType(), True),
            StructField('COD_ENQ', StringType(), True), StructField('VL_BC_IPI', StringType(), True),
            StructField('ALIQ_IPI', StringType(), True), StructField('VL_IPI', StringType(), True),
            StructField('CST_PIS', StringType(), True), StructField('VL_BC_PIS', StringType(), True),
            StructField('ALIQ_PIS', StringType(), True), StructField('QUANT_BC_PIS', StringType(), True),
            StructField('ALIQ_PIS_QUANT', StringType(), True), StructField('VL_PIS_ITEM', StringType(), True),
            StructField('CST_COFINS', StringType(), True),  StructField('VL_BC_COFINS', StringType(), True),
            StructField('ALIQ_COFINS', StringType(), True), StructField('QUANT_BC_COFINS', StringType(), True),
            StructField('ALIQ_COFINS_QUANT', StringType(), True), StructField('VL_COFINS_ITEM', StringType(), True),
            StructField('COD_CTA', StringType(), True)
        ]),
        'C190': StructType([
            # REGISTRO ANALÍTICO DO DOCUMENTO (CÓDIGO 01, 1B, 04, 55 e 65)
            StructField('REG', StringType(), True), StructField('CST_ICMS', StringType(), True),
            StructField('CFOP', StringType(), True), StructField('ALIQ_ICMS', StringType(), True),
            StructField('VL_OPR', StringType(), True), StructField('VL_BC_ICMS', StringType(), True),
            StructField('VL_ICMS_TOTAL', StringType(), True), StructField('VL_BC_ICMS_ST', StringType(), True),
            StructField('VL_ICMS_ST', StringType(), True), StructField('VL_RED_BC', StringType(), True),
            StructField('VL_IPI', StringType(), True), StructField('COD_OBS', StringType(), True) 
        ]),
        'C191': StructType([
            # Detalhamento da Consolidação PIS
            StructField("REG", StringType(), True),
            StructField("CNPJ_CPF_PART", StringType(), True),
            StructField("CST_PIS", StringType(), True),
            StructField("CFOP", StringType(), True),
            StructField("VL_ITEM", StringType(), True),
            StructField("VL_DESC", StringType(), True),
            StructField("VL_BC_PIS", StringType(), True),
            StructField("VL_PIS", StringType(), True),
            StructField("ALIQ_PIS", StringType(), True),
            StructField("ALIQ_PIS_QUANT", StringType(), True),
            StructField("QUANT_BC_PIS", StringType(), True),
            StructField("COD_CTA", StringType(), True),
        ]),
        'C195': StructType([
            # Detalhamento da Consolidação COFINS
            StructField("REG", StringType(), True),
            StructField("CNPJ_CPF_PART", StringType(), True),
            StructField("CST_COFINS", StringType(), True),
            StructField("CFOP", StringType(), True),
            StructField("VL_ITEM", StringType(), True),
            StructField("VL_DESC", StringType(), True),
            StructField("VL_BC_COFINS", StringType(), True),
            StructField("ALIQ_COFINS", StringType(), True),
            StructField("QUANT_BC_COFINS", StringType(), True),
            StructField("ALIQ_COFINS_QUANT", StringType(), True),
            StructField("VL_COFINS", StringType(), True),
            StructField("COD_CTA", StringType(), True),
        ]),
        'C180': StructType([
            StructField("REG", StringType(), True),
            StructField("COD_MOD", StringType(), True),
            StructField("DT_DOC_INI", StringType(), True),
            StructField("DT_DOC_FIN", StringType(), True),
            StructField("COD_ITEM", StringType(), True),
            StructField("COD_NCM", StringType(), True),
            StructField("EX_IPI", StringType(), True),
            StructField("VL_TOT_ITEM", StringType(), True), 
        ]),
        'C181': StructType([
            StructField("REG", StringType(), True),
            StructField("CST_PIS", StringType(), True),
            StructField("CFOP", StringType(), True),
            StructField("VL_ITEM", StringType(), True),
            StructField("VL_DESC", StringType(), True),
            StructField("VL_BC_PIS", StringType(), True),
            StructField("ALIQ_PIS", StringType(), True),
        ]),
        'C185': StructType([
            StructField("REG", StringType(), True),
            StructField("CST_COFINS", StringType(), True),
            StructField("CFOP", StringType(), True),
            StructField("VL_ITEM", StringType(), True),
            StructField("VL_DESC", StringType(), True),
            StructField("VL_BC_COFINS", StringType(), True),
            StructField("ALIQ_COFINS", StringType(), True),
        ]),
        'C500': StructType([
            StructField("REG", StringType(), True),
            StructField("COD_PART", StringType(), True),
            StructField("COD_MOD", StringType(), True),
            StructField("COD_SIT", StringType(), True),
            StructField("SER", StringType(), True),
            StructField("SUB", StringType(), True),
            StructField("NUM_DOC", StringType(), True),
            StructField("DT_DOC", StringType(), True),
            StructField("DT_ENT", StringType(), True),
            StructField("VL_DOC", StringType(), True),
            StructField("VL_ICMS", StringType(), True),
            StructField("COD_INF", StringType(), True),
            StructField("VL_PIS", StringType(), True),
            StructField("VL_COFINS", StringType(), True),
            StructField("CHV_DOCE", StringType(), True),
        ]),
        'C501': StructType([
            StructField("REG", StringType(), True),
            StructField("CST_PIS", StringType(), True),
            StructField("VL_ITEM", StringType(), True),
            StructField("VL_BC_PIS", StringType(), True),
            StructField("VL_PIS", StringType(), True),
            StructField("NAT_BC_CRED", StringType(), True),
            StructField("ALIQ_PIS", StringType(), True),
            StructField("COD_CTA", StringType(), True),    
        ]),
        'C505': StructType([
            StructField("REG", StringType(), True),
            StructField("CST_COFINS", StringType(), True),
            StructField("VL_ITEM", StringType(), True),
            StructField("NAT_BC_CRED", StringType(), True),
            StructField("VL_BC_COFINS", StringType(), True),
            StructField("ALIQ_COFINS", StringType(), True),
            StructField("VL_COFINS", StringType(), True),
            StructField("COD_CTA", StringType(), True),
        ]),
        'D010': StructType([
            StructField("REG", StringType(), True),
            StructField("CNPJ_D010", StringType(), True),
        ]),
        'D100': StructType([
            StructField("REG", StringType(), True),
            StructField("IND_OPER", StringType(), True),
            StructField("IND_EMIT", StringType(), True),
            StructField("COD_PART", StringType(), True),
            StructField("COD_MOD", StringType(), True),
            StructField("COD_SIT", StringType(), True),
            StructField("SER", StringType(), True),
            StructField("SUB", StringType(), True),
            StructField("NUM_DOC", StringType(), True),
            StructField("CHV_CTE", StringType(), True),
            StructField("DT_DOC", StringType(), True),
            StructField("DT_A_P", StringType(), True),
            StructField("TP_CT_E", StringType(), True),
            StructField("CHV_CTE_REF", StringType(), True),
            StructField("VL_DOC", StringType(), True),
            StructField("VL_DESC", StringType(), True),
            StructField("IND_FRT", StringType(), True),
            StructField("VL_SERV", StringType(), True),
            StructField("VL_BC_ICMS", StringType(), True),
            StructField("VL_ICMS", StringType(), True),
            StructField("VL_NT", StringType(), True), 
            StructField("COD_INF", StringType(), True),
            StructField("COD_CTA", StringType(), True),
        ]),
        'D101': StructType([
            StructField("REG", StringType(), True), 
            StructField("IND_NAT_FRT_PIS", StringType(), True),
            StructField("VL_ITEM_PIS", StringType(), True),
            StructField("CST_PIS", StringType(), True),
            StructField("NAT_BC_CRED_PIS", StringType(), True),
            StructField("VL_BC_PIS", StringType(), True),
            StructField("ALIQ_PIS", StringType(), True),
            StructField("VL_PIS", StringType(), True),
            StructField("COD_CTA_PIS", StringType(), True),
        ]),
        'D105': StructType([
            StructField("REG", StringType(), True),
            StructField("IND_NAT_FRT_COFINS", StringType(), True),
            StructField("VL_ITEM_COFINS", StringType(), True),
            StructField("CST_COFINS", StringType(), True),
            StructField("NAT_BC_CRED_COFINS", StringType(), True),
            StructField("VL_BC_COFINS", StringType(), True),
            StructField("ALIQ_COFINS", StringType(), True),
            StructField("VL_COFINS", StringType(), True),
            StructField("COD_CTA_COFINS", StringType(), True),
        ]),
        'F010': StructType([
            StructField('REG', StringType(), True),
            StructField('CNPJ_F010', StringType(), True)
        ]),
        'F100': StructType([
            StructField("REG", StringType(), True),
            StructField("IND_OPER", StringType(), True),
            StructField("COD_PART", StringType(), True),
            StructField("COD_ITEM", StringType(), True),
            StructField("DT_OPER", StringType(), True),
            StructField("VL_OPER", StringType(), True),
            StructField("CST_PIS", StringType(), True),
            StructField("VL_BC_PIS", StringType(), True),
            StructField("ALIQ_PIS", StringType(), True),
            StructField("VL_PIS", StringType(), True),
            StructField("CST_COFINS", StringType(), True),
            StructField("VL_BC_COFINS", StringType(), True),
            StructField("ALIQ_COFINS", StringType(), True),
            StructField("VL_COFINS", StringType(), True),
            StructField("NAT_BC_CRED", StringType(), True),
            StructField("IND_ORIG_CRED", StringType(), True),
            StructField("COD_CTA", StringType(), True),
            StructField("COD_CCUS", StringType(), True),
            StructField("DESC_DOC_OPER", StringType(), True),     
        ]),
        'F120': StructType([
            StructField("REG", StringType(), True),
            StructField("NAT_BC_CRED", StringType(), True),
            StructField("IDENT_BEM_IMOB", StringType(), True),
            StructField("IND_ORIG_CRED", StringType(), True),
            StructField("IND_UTIL_BEM_IMOB", StringType(), True),
            StructField("VL_OPER_DEP", StringType(), True),
            StructField("PARC_OPER_NAO_BC_CRED", StringType(), True),
            StructField("CST_PIS", StringType(), True),
            StructField("VL_BC_PIS", StringType(), True),
            StructField("ALIQ_PIS", StringType(), True),
            StructField("VL_PIS", StringType(), True),
            StructField("CST_COFINS", StringType(), True),
            StructField("VL_BC_COFINS", StringType(), True),
            StructField("ALIQ_COFINS", StringType(), True),
            StructField("VL_COFINS", StringType(), True),
            StructField("COD_CTA", StringType(), True),
            StructField("COD_CCUS", StringType(), True),
            StructField("DESC_BEM_IMOB", StringType(), True),
        ]),
        'F130': StructType([
            StructField("REG", StringType(), True),
            StructField("NAT_BC_CRED", StringType(), True),
            StructField("IDENT_BEM_IMOB", StringType(), True),
            StructField("IND_ORIG_CRED", StringType(), True),
            StructField("IND_UTIL_BEM_IMOB", StringType(), True),
            StructField("MES_OPER_AQUIS", StringType(), True),
            StructField("VL_OPER_AQUIS", StringType(), True),
            StructField("PARC_OPER_NAO_BC_CRED", StringType(), True),
            StructField("VL_BC_CRED", StringType(), True),
            StructField("IND_NR_PARC", StringType(), True),
            StructField("CST_PIS", StringType(), True),
            StructField("VL_BC_PIS", StringType(), True),
            StructField("ALIQ_PIS", StringType(), True),
            StructField("VL_PIS", StringType(), True),
            StructField("CST_COFINS", StringType(), True),
            StructField("VL_BC_COFINS", StringType(), True),
            StructField("ALIQ_COFINS", StringType(), True),
            StructField("VL_COFINS", StringType(), True),
            StructField("COD_CTA", StringType(), True),
            StructField("COD_CCUS", StringType(), True),
            StructField("DESC_BEM_IMOB", StringType(), True),
        ]),
        'F550': StructType([
            StructField('REG', StringType(), True),
            StructField('VL_REC_COMP', StringType(), True),
            StructField('CST_PIS', StringType(), True),
            StructField('VL_DESC_PIS', StringType(), True),
            StructField('VL_BC_PIS', StringType(), True),
            StructField('ALIQ_PIS', StringType(), True),
            StructField('VL_PIS', StringType(), True),
            StructField('CST_COFINS', StringType(), True),
            StructField('VL_DESC_COFINS', StringType(), True),
            StructField('VL_BC_COFINS', StringType(), True),
            StructField('ALIQ_COFINS', StringType(), True),
            StructField('VL_COFINS', StringType(), True),
            StructField('COD_MOD', StringType(), True),
            StructField('CFOP', StringType(), True),
            StructField('COD_CTA', StringType(), True),
            StructField('INFO_COMPL', StringType(), True),
        ]),
        'F600': StructType([
            StructField('REG', StringType(), True),
            StructField('IND_NAT_RET', StringType(), True),
            StructField('DT_RET', StringType(), True),
            StructField('VL_BC_RET', StringType(), True),
            StructField('VL_RET', StringType(), True),
            StructField('COD_REC', StringType(), True),
            StructField('IND_NAT_REC', StringType(), True),
            StructField('CNPJ', StringType(), True),
            StructField('VL_RET_PIS', StringType(), True),
            StructField('VL_RET_COFINS', StringType(), True),
            StructField('IND_DEC', StringType(), True),
        ]),
        'A010': StructType([
            StructField('REG', StringType(), True),            
            StructField('CNPJ_A010', StringType(), True),
        ]),
        'A100': StructType([
            # Documento - Nota Fiscal de Serviço
            StructField('REG', StringType(), True),
            StructField('IND_OPER', StringType(), True),
            StructField('IND_EMIT', StringType(), True),
            StructField('COD_PART', StringType(), True),
            StructField('COD_SIT', StringType(), True),
            StructField('SER', StringType(), True),
            StructField('SUB', StringType(), True),
            StructField('NUM_DOC', StringType(), True),
            StructField('CHV_NFSE', StringType(), True),
            StructField('DT_DOC', StringType(), True),
            StructField('DT_EXE_SERV', StringType(), True),
            StructField('VL_DOC', StringType(), True),
            StructField('IND_PGTO', StringType(), True),
            StructField('VL_DESC', StringType(), True),
            StructField('VL_BC_PIS', StringType(), True),
            StructField('VL_PIS', StringType(), True),
            StructField('VL_BC_COFINS', StringType(), True),
            StructField('VL_COFINS', StringType(), True),
            StructField('VL_PIS_RET', StringType(), True),
            StructField('VL_COFINS_RET', StringType(), True),
            StructField('VL_ISS', StringType(), True),
        ]),
        'A170': StructType([
            # Complemento do Documento - Itens do Documento
            StructField('REG', StringType(), True),
            StructField('NUM_ITEM', StringType(), True),
            StructField('COD_ITEM', StringType(), True),
            StructField('DESCR_COMPL', StringType(), True),
            StructField('VL_ITEM', StringType(), True),
            StructField('VL_DESC', StringType(), True),
            StructField('NAT_BC_CRED', StringType(), True),
            StructField('IND_ORIG_CRED', StringType(), True),
            StructField('CST_PIS', StringType(), True),
            StructField('VL_BC_PIS_ITEM', StringType(), True),
            StructField('ALIQ_PIS', StringType(), True),
            StructField('VL_PIS_ITEM', StringType(), True),
            StructField('CST_COFINS', StringType(), True),
            StructField('VL_BC_COFINS_ITEM', StringType(), True),
            StructField('ALIQ_COFINS', StringType(), True),
            StructField('VL_COFINS_ITEM', StringType(), True),
            StructField('COD_CTA', StringType(), True),
            StructField('COD_CCUS', StringType(), True),     
        ]),
        'I100': StructType([
            # Complemento do Documento - Itens do Documento
            StructField('REG', StringType(), True),
            StructField('VL_REC', StringType(), True),
            StructField('CST_PIS_COFINS', StringType(), True),
            StructField('VL_TOT_DED_GER', StringType(), True),
            StructField('VL_TOT_DED_ESP', StringType(), True),
            StructField('VL_BC_PIS', StringType(), True),
            StructField('ALIQ_PIS', StringType(), True),
            StructField('VL_PIS', StringType(), True),
            StructField('VL_BC_COFINS', StringType(), True),
            StructField('ALIQ_COFINS', StringType(), True),
            StructField('VL_COFINS', StringType(), True),
            StructField('INFO_COMPL', StringType(), True),          
        ]),
    'M100': StructType([
            StructField('REG', StringType(), True),
            StructField('COD_CRED', StringType(), True),
            StructField('IND_CRED_ORI', StringType(), True),
            StructField('VL_BC_PIS', StringType(), True),
            StructField('ALIQ_PIS', StringType(), True),
            StructField('QUANT_BC_PIS', StringType(), True),
            StructField('ALIQ_PIS_QUANT', StringType(), True),
            StructField('VL_CRED', StringType(), True),
            StructField('VL_AJUS_ACRES', StringType(), True),
            StructField('VL_AJUS_REDUC', StringType(), True),
            StructField('VL_CRED_DIF', StringType(), True),
            StructField('VL_CRED_DISP', StringType(), True),
            StructField('IND_DESC_CRED', StringType(), True),
            StructField('VL_CRED_DESC', StringType(), True),
            StructField('SLD_CRED', StringType(), True),

        ]),
        'M105': StructType([
            StructField('REG', StringType(), True),
            StructField('NAT_BC_CRED', StringType(), True),
            StructField('CST_PIS', StringType(), True),
            StructField('VL_BC_PIS_TOT', StringType(), True),
            StructField('VL_BC_PIS_CUM', StringType(), True),
            StructField('VL_BC_PIS_NC', StringType(), True),
            StructField('VL_BC_PIS', StringType(), True),    
            StructField('QUANT_BC_PIS_TOT', StringType(), True),
            StructField('QUANT_BC_PIS', StringType(), True),
            StructField('DESC_CRED', StringType(), True),
        ]),
        'M110': StructType([
            StructField('REG', StringType(), True),
            StructField('IND_AJ', StringType(), True),
            StructField('VL_AJ', StringType(), True),
            StructField('COD_AJ', StringType(), True),
            StructField('NUM_DOC', StringType(), True),
            StructField('DESCR_AJ', StringType(), True),
            StructField('DT_REF', StringType(), True),
        ]),
        'M115': StructType([
            StructField('REG', StringType(), True),
            StructField('DET_VALOR_AJ', StringType(), True),
            StructField('CST_PIS_M115', StringType(), True),
            StructField('DET_BC_CRED', StringType(), True),
            StructField('DET_ALIQ', StringType(), True),
            StructField('DT_OPER_AJ', StringType(), True),
            StructField('DESC_AJ', StringType(), True),
            StructField('COD_CTA', StringType(), True),
            StructField('INFO_COMPL', StringType(), True),
        ]),
        'M200': StructType([
            StructField('REG', StringType(), True),
            StructField('VL_TOT_CONT_NC_PER', StringType(), True),
            StructField('VL_TOT_CRED_DESC', StringType(), True),
            StructField('VL_TOT_CRED_DESC_ANT', StringType(), True),
            StructField('VL_TOT_CONT_NC_DEV', StringType(), True),
            StructField('VL_RET_NC', StringType(), True),
            StructField('VL_OUT_DED_NC', StringType(), True),
            StructField('VL_CONT_NC_REC', StringType(), True),
            StructField('VL_TOT_CONT_CUM_PER', StringType(), True),
            StructField('VL_RET_CUM', StringType(), True),
            StructField('VL_OUT_DED_CUM', StringType(), True),
            StructField('VL_CONT_CUM_REC', StringType(), True),
            StructField('VL_TOT_CONT_REC', StringType(), True),
            
        ]),
        'M205': StructType([
            StructField('REG', StringType(), True),
            StructField('NUM_CAMPO', StringType(), True),
            StructField('COD_REC', StringType(), True),
            StructField('VL_DEBITO', StringType(), True),
        ]),
        'M210': StructType([
            StructField('REG', StringType(), True),
            StructField('COD_CONT', StringType(), True),
            StructField('VL_REC_BRT', StringType(), True),
            StructField('VL_BC_CONT', StringType(), True),
            StructField('ALIQ_PIS', StringType(), True),
            StructField('QUANT_BC_PIS', StringType(), True),
            StructField('ALIQ_PIS_QUANT', StringType(), True),
            StructField('VL_CONT_APUR', StringType(), True),
            StructField('VL_AJUS_ACRES', StringType(), True),
            StructField('VL_AJUS_REDUC', StringType(), True),
            StructField('VL_CONT_DIFER', StringType(), True),
            StructField('VL_CONT_DIFER_ANT', StringType(), True),
            StructField('VL_CONT_PER', StringType(), True)
        ]),
        'M220': StructType([
            StructField('REG', StringType(), True),
            StructField('IND_AJ', StringType(), True),
            StructField('VL_AJ', StringType(), True),
            StructField('COD_AJ', StringType(), True),
            StructField('NUM_DOC', StringType(), True),
            StructField('DESCR_AJ', StringType(), True),
            StructField('DT_REF', StringType(), True),
        ]),
        'M400': StructType([
            StructField('REG', StringType(), True),
            StructField('CST_PIS', StringType(), True),
            StructField('VL_TOT_REC', StringType(), True),
            StructField('COD_CTA', StringType(), True),
            StructField('DESC_COMPL', StringType(), True),

        ]),
        'M410': StructType([
            StructField('REG', StringType(), True),
            StructField('NAT_REC', StringType(), True),
            StructField('VL_REC', StringType(), True),
            StructField('COD_CTA', StringType(), True),
            StructField('DESC_COMPL', StringType(), True)
        ]),
        'M500': StructType([
            StructField('REG', StringType(), True),
            StructField('COD_CRED', StringType(), True),
            StructField('IND_CRED_ORI', StringType(), True),
            StructField('VL_BC_COFINS', StringType(), True),
            StructField('ALIQ_COFINS', StringType(), True),
            StructField('QUANT_BC_COFINS', StringType(), True),
            StructField('ALIQ_COFINS_QUANT', StringType(), True),
            StructField('VL_CRED', StringType(), True),
            StructField('VL_AJUS_ACRES', StringType(), True),
            StructField('VL_AJUS_REDUC', StringType(), True),
            StructField('VL_CRED_DIFER', StringType(), True),
            StructField('VL_CRED_DISP', StringType(), True),
            StructField('IND_DESC_CRED', StringType(), True),
            StructField('VL_CRED_DESC', StringType(), True),
            StructField('SLD_CRED', StringType(), True),

        ]),
        'M505': StructType([
            StructField('REG', StringType(), True),
            StructField('NAT_BC_CRED', StringType(), True),
            StructField('CST_COFINS', StringType(), True),
            StructField('VL_BC_COFINS_TOT', StringType(), True),
            StructField('VL_BC_COFINS_CUM', StringType(), True),
            StructField('VL_BC_COFINS_NC', StringType(), True),
            StructField('VL_BC_COFINS', StringType(), True),
            StructField('QUANT_BC_COFINS_TOT', StringType(), True),
            StructField('QUANT_BC_COFINS', StringType(), True),
            StructField('DESC_CRED', StringType(), True),

        ]),
        'M510': StructType([
            StructField('REG', StringType(), True),
            StructField('IND_AJ', StringType(), True),
            StructField('VL_AJ', StringType(), True),
            StructField('COD_AJ', StringType(), True),
            StructField('NUM_DOC', StringType(), True),
            StructField('DESCR_AJ', StringType(), True),
            StructField('DT_REF', StringType(), True),
        ]),
        'M515': StructType([
            StructField('REG', StringType(), True),
            StructField('DET_VALOR_AJ', StringType(), True),
            StructField('CST_COFINS', StringType(), True),
            StructField('DET_BC_CRED', StringType(), True),
            StructField('DET_ALIQ', StringType(), True),
            StructField('DT_OPER_AJ', StringType(), True),
            StructField('DESC_AJ', StringType(), True),
            StructField('COD_CTA', StringType(), True),
            StructField('INFO_COMPL', StringType(), True),

        ]),
        'M600': StructType([
            StructField('REG', StringType(), True),
            StructField('VL_TOT_CRED_DESC', StringType(), True),
            StructField('VL_TOT_CRED_DESC_ANT', StringType(), True),
            StructField('VL_TOT_CONT_NC_DEV', StringType(), True),
            StructField('VL_RET_NC', StringType(), True),
            StructField('VL_OUT_DED_NC', StringType(), True),
            StructField('VL_CONT_NC_REC', StringType(), True),
            StructField('VL_TOT_CONT_CUM_PER', StringType(), True),
            StructField('VL_RET_CUM', StringType(), True),
            StructField('VL_OUT_DED_CUM', StringType(), True),
            StructField('VL_CONT_CUM_REC', StringType(), True),
            StructField('VL_TOT_CONT_REC', StringType(), True),
        ]),
        'M605': StructType([
            StructField('REG', StringType(), True),
            StructField('NUM_CAMPO', StringType(), True),
            StructField('COD_REC', StringType(), True),
            StructField('VL_DEBITO', StringType(), True),

        ]),
        'M610': StructType([
            StructField('REG', StringType(), True),
            StructField('COD_CONT', StringType(), True),
            StructField('VL_REC_BRT', StringType(), True),
            StructField('VL_BC_CONT', StringType(), True),
            StructField('ALIQ_COFINS', StringType(), True),
            StructField('QUANT_BC_COFINS', StringType(), True),
            StructField('ALIQ_COFINS_QUANT', StringType(), True),
            StructField('VL_CONT_APUR', StringType(), True),
            StructField('VL_AJUS_ACRES', StringType(), True),
            StructField('VL_AJUS_REDUC', StringType(), True),
            StructField('VL_CONT_DIFER', StringType(), True),
            StructField('VL_CONT_DIFER_ANT', StringType(), True),
            StructField('VL_CONT_PER', StringType(), True),

        ]),
        'M620': StructType([
            StructField('REG', StringType(), True),
            StructField('IND_AJ', StringType(), True),
            StructField('VL_AJ', StringType(), True),
            StructField('COD_AJ', StringType(), True),
            StructField('NUM_DOC', StringType(), True),
            StructField('DESCR_AJ', StringType(), True),
            StructField('DT_REF', StringType(), True),

        ]),
        'M800': StructType([
            StructField('REG', StringType(), True),
            StructField('CST_COFINS', StringType(), True),
            StructField('VL_TOT_REC', StringType(), True),
            StructField('COD_CTA', StringType(), True),
            StructField('DESC_COMPL', StringType(), True),
        ]),
        'M810': StructType([
            StructField('REG', StringType(), True),
            StructField('NAT_REC', StringType(), True),
            StructField('VL_REC', StringType(), True),
            StructField('COD_CTA', StringType(), True),
            StructField('DESC_COMPL_M810', StringType(), True),
        ]),
        '1500': StructType([
            StructField('REG', StringType(), True),
            StructField('PER_APU_CRED', StringType(), True),
            StructField('ORIG_CRED', StringType(), True),
            StructField('CNPJ_SUC', StringType(), True),
            StructField('COD_CRED', StringType(), True),
            StructField('VL_CRED_APU', StringType(), True),
            StructField('VL_CRED_EXT_APU', StringType(), True),
            StructField('VL_TOT_CRED_APU', StringType(), True),
            StructField('VL_CRED_DESC_PA_ANT', StringType(), True),
            StructField('VL_CRED_PER_PA_ANT', StringType(), True),
            StructField('VL_CRED_DCOMP_PA_ANT', StringType(), True),
            StructField('SD_CRED_DISP_EFD', StringType(), True),
            StructField('VL_CRED_DESC_EFD', StringType(), True),
            StructField('VL_CRED_PER_EFD', StringType(), True),
            StructField('VL_CRED_DCOMP_EFD', StringType(), True),
            StructField('VL_CRED_TRANS', StringType(), True),
            StructField('VL_CRED_OUT', StringType(), True),
            StructField('SLD_CRED_FIM', StringType(), True),
            
        ]),
        '1100': StructType([
            StructField('REG', StringType(), True),
            StructField('PER_APU_CRED', StringType(), True),
            StructField('ORIG_CRED', StringType(), True),
            StructField('CNPJ_SUC', StringType(), True),
            StructField('COD_CRED', StringType(), True),
            StructField('VL_CRED_APU', StringType(), True),
            StructField('VL_CRED_EXT_APU', StringType(), True),
            StructField('VL_TOT_CRED_APU', StringType(), True),
            StructField('VL_CRED_DESC_PA_ANT', StringType(), True),
            StructField('VL_CRED_PER_PA_ANT', StringType(), True),
            StructField('VL_CRED_DCOMP_PA_ANT', StringType(), True),
            StructField('SD_CRED_DISP_EFD', StringType(), True),
            StructField('VL_CRED_DESC_EFD', StringType(), True),
            StructField('VL_CRED_PER_EFD', StringType(), True),
            StructField('VL_CRED_DCOMP_EFD', StringType(), True),
            StructField('VL_CRED_TRANS', StringType(), True),
            StructField('VL_CRED_OUT', StringType(), True),
            StructField('SLD_CRED_FIM', StringType(), True),
        ]),
        '1300': StructType([
            StructField('REG', StringType(), True),
            StructField('IND_NAT_RET', StringType(), True),
            StructField('PR_REC_RET', StringType(), True),
            StructField('VL_RET_APU', StringType(), True),
            StructField('VL_RET_DED', StringType(), True),
            StructField('VL_RET_PER', StringType(), True),
            StructField('VL_RET_DCOMP', StringType(), True),
            StructField('SLD_RET', StringType(), True),

        ]),
        '1700': StructType([
            StructField('REG', StringType(), True),
            StructField('IND_NAT_RET', StringType(), True),
            StructField('PR_REC_RET', StringType(), True),
            StructField('VL_RET_APU', StringType(), True),
            StructField('VL_RET_DED', StringType(), True),
            StructField('VL_RET_PER', StringType(), True),
            StructField('VL_RET_DCOMP', StringType(), True),
            StructField('SLD_RET', StringType(), True),
        ])
    
    }
    SPED_CONTRIB_FIELDS_MAP = {

        '0000': {
            'REG': 1, 'COD_VER': 2, 'TIPO_ESCRIT': 3, 'IND_SIT_ESP': 4, 'NUM_REC_ANTERIOR': 5,
            'DT_INI': 6, 'DT_FIN': 7, 'NOME': 8, 'CNPJ': 9, 'UF': 10, 'COD_MUN': 11,
            'SUFRAMA': 12, 'IND_NAT_PJ': 13, 'IND_ATIV': 14
        },
        '0110': {
            'REG': 1, 'COD_INC_TRIB': 2, 'IND_APRO_CRED': 3, 'COD_TIPO_CONT': 4, 'IND_REG_CUM': 5
        },
        '0140': {
            'REG': 1, 'COD_ESTABELECIMENTO': 2, 'NOME_ESTABELECIMENTO': 3, 'CNPJ_ESTABELECIMENTO': 4,
            'UF_ESTABELECIMENTO': 5, 'IE_ESTABELECIMENTO': 6, 'COD_MUN_ESTABELECIMENTO': 7, 'IM_ESTABELECIMENTO': 8,
            'SUFRAMA_ESTABELECIMENTO': 9

        },
        '0150': {
            'REG': 1, 'COD_PART': 2, 'NOME_PARTICIPANTE': 3, 'COD_PAIS': 4, 'CNPJ_PARTICIPANTE': 5, 'CPF_PARTICIPANTE': 6,
            'IE_PARTICIPANTE': 7, 'COD_MUN_PARTICIPANTE': 8, 'SUFRAMA_PARTICIPANTE': 9, 'END_PARTICIPANTE': 10, 'NUM_PARTICIPANTE': 11, 'COMPL_PARTICIPANTE': 12,
            'BAIRRO_PARTICIPANTE': 13
        },
        '0200': {
            'REG': 1, 'COD_ITEM': 2, 'DESCR_ITEM': 3, 'COD_BARRA': 4, 'COD_ANT_ITEM': 5,
            'UNID_INV': 6, 'TIPO_ITEM': 7, 'COD_NCM': 8, 'EX_IPI': 9, 'COD_GEN': 10,
            'COD_LST': 11, 'ALIQ_ICMS': 12
        },
        '0500': {
            'REG': 1, 'DT_ALT': 2, 'COD_NAT_CC': 3, 'IND_CTA': 4, 'NIVEL': 5,
            'COD_CTA': 6, 'NOME_CTA': 7, 'COD_CTA_REF': 8, 'CNPJ_EST': 9
        },
        'C010': {
            'REG': 1, 'CNPJ_C010': 2, 'IND_ESCRI': 3
        },
        'C100': { # Documento - Nota Fiscal (código 01, 1B, 04 e 55)
            'REG': 1, 'IND_OPER': 2, 'IND_EMIT': 3, 'COD_PART': 4, 'COD_MOD': 5,
            'COD_SIT': 6, 'SER': 7, 'NUM_DOC': 8, 'CHV_NFE': 9, 'DT_DOC': 10, 
            'DT_E_S': 11, 'VL_DOC': 12, 'IND_PGTO': 13, 'VL_DESC': 14, 'VL_ABAT_NT': 15,
            'VL_MERC': 16, 'IND_FRT': 17, 'VL_FRT': 18, 'VL_SEG': 19, 'VL_OUT_DA': 20,
            'VL_BC_ICMS': 21, 'VL_ICMS': 22, 'VL_BC_ICMS_ST': 23, 'VL_ICMS_ST': 24, 
            'VL_IPI': 25, 'VL_PIS': 26, 'VL_COFINS': 27,  'VL_PIS_ST': 28, 'VL_COFINS_ST': 29
        },
        'C170': { # Itens do Documento
            'REG': 1, 'NUM_ITEM': 2, 'COD_ITEM': 3, 'DESCR_COMPL': 4, 'QTD': 5,
            'UNID': 6, 'VL_ITEM': 7, 'VL_DESC': 8, 'IND_MOV': 9, 'CST_ICMS': 10,
            'CFOP': 11, 'COD_NAT': 12, 'VL_BC_ICMS_ITEM': 13, 'ALIQ_ICMS_ITEM': 14,
            'VL_ICMS_ITEM': 15, 'VL_BC_ICMS_ST_ITEM': 16, 'ALIQ_ST': 17, 'VL_ICMS_ST_ITEM': 18,
            'IND_APUR': 19, 'CST_IPI': 20, 'COD_ENQ': 21, 'VL_BC_IPI': 22,
            'ALIQ_IPI': 23, 'VL_IPI': 24, 'CST_PIS': 25, 'VL_BC_PIS': 26, 
            'ALIQ_PIS': 27, 'QUANT_BC_PIS': 28, 'ALIQ_PIS_QUANT': 29, 'VL_PIS_ITEM': 30,
            'CST_COFINS': 31, 'VL_BC_COFINS': 32, 'ALIQ_COFINS': 33, 'QUANT_BC_COFINS': 34,
            'ALIQ_COFINS_QUANT': 35, 'VL_COFINS_ITEM': 36, 'COD_CTA': 37
        },
        'C190': { # Complemento do Item (ICMS/IPI)
            'REG': 1, 'CST_ICMS': 2, 'CFOP': 3, 'ALIQ_ICMS': 4, 'VL_OPR': 5, 'VL_BC_ICMS': 6,
            'VL_ICMS_TOTAL': 7, 'VL_BC_ICMS_ST': 8, 'VL_ICMS_ST': 9, 'VL_RED_BC': 10, 'VL_IPI': 11,
            'COD_OBS': 12
        },
        'C191': {
            'REG': 1, 'CNPJ_CPF_PART': 2, 'CST_PIS': 3, 'CFOP': 4, 'VL_ITEM': 5, 'VL_DESC': 6,
            'VL_BC_PIS': 7, 'VL_PIS': 8, 'ALIQ_PIS': 9, 'ALIQ_PIS_QUANT': 10, 'QUANT_BC_PIS': 11,
            'COD_CTA': 12, 
        },
        'C195': {
            'REG': 1, 'CNPJ_CPF_PART': 2, 'CST_COFINS': 3, 'CFOP': 4, 'VL_ITEM': 5, 'VL_DESC': 6,
            'VL_BC_COFINS': 7, 'ALIQ_COFINS': 8, 'QUANT_BC_COFINS': 9, 'ALIQ_COFINS_QUANT': 10,
            'VL_COFINS': 11, 'COD_CTA': 12, 
        },
        'C180': {
            'REG': 1, 'COD_MOD': 2, 'DT_DOC_INI': 3, 'DT_DOC_FIN': 4, 'COD_ITEM': 5, 'COD_NCM': 6,
            'EX_IPI': 7, 'VL_TOT_ITEM': 8, 
        },
        'C181': {
            'REG': 1, 'CST_PIS': 2, 'CFOP': 3, 'VL_ITEM': 4, 'VL_DESC': 5, 'VL_BC_PIS': 6,
            'ALIQ_PIS': 7
        },
        'C185': {
            'REG': 1, 'CST_COFINS': 2, 'CFOP': 3, 'VL_ITEM': 4, 'VL_DESC': 5, 'VL_BC_COFINS': 6,
            'ALIQ_COFINS': 7
        },
        'C500': {
            'REG': 1, 'COD_PART': 2, 'COD_MOD': 3, 'COD_SIT': 4, 'SER': 5, 'SUB': 6, 'NUM_DOC': 7,
            'DT_DOC': 8, 'DT_ENT': 9, 'VL_DOC': 10, 'VL_ICMS': 11, 'COD_INF': 12, 'VL_PIS': 13, 
            'VL_COFINS': 14, 'CHV_DOCE': 15
        },
        'C501': {
            'REG': 1, 'CST_PIS': 2, 'VL_ITEM': 3, 'VL_BC_PIS': 4, 'VL_PIS': 5, 'NAT_BC_CRED': 6,
            'ALIQ_PIS': 7, 'COD_CTA': 8
        },
        'C505': {
            'REG': 1, 'CST_COFINS': 2, 'VL_ITEM': 3, 'NAT_BC_CRED': 4, 'VL_BC_COFINS': 5,
            'ALIQ_COFINS': 6, 'VL_COFINS': 7, 'COD_CTA':8
        },
        'D010': {
            'REG': 1, 'CNPJ_D010': 2
        },
        'D100': {
            'REG': 1, 'IND_OPER': 2, 'IND_EMIT': 3, 'COD_PART': 4, 'COD_MOD': 5, 'COD_SIT': 6,
            'SER': 7, 'SUB': 8, 'NUM_DOC': 9, 'CHV_CTE': 10, 'DT_DOC': 11, 'DT_A_P': 12,
            'TP_CT_E': 13, 'CHV_CTE_REF': 14, 'VL_DOC': 15, 'VL_DESC': 16, 'IND_FRT': 17,
            'VL_SERV': 18, 'VL_BC_ICMS': 19, 'VL_ICMS': 20, 'VL_NT': 21, 'COD_INF': 22,
            'COD_CTA': 23
        },
        'D101': {
            'REG': 1, 'IND_NAT_FRT_PIS': 2, 'VL_ITEM_PIS': 3, 'CST_PIS': 4, 'NAT_BC_CRED_PIS': 5,
            'VL_BC_PIS': 6, 'ALIQ_PIS': 7, 'VL_PIS': 8, 'COD_CTA_PIS': 9
        },
        'D105': {
            'REG': 1, 'IND_NAT_FRT_COFINS': 2, 'VL_ITEM_COFINS': 3, 'CST_COFINS': 4, 'NAT_BC_CRED_COFINS': 5,
            'VL_BC_COFINS': 6, 'ALIQ_COFINS': 7, 'VL_COFINS': 8, 'COD_CTA_COFINS': 9
        },
        'F010': {
            'REG': 1, 'CNPJ_F010': 2
        },
        'F100': {
            'REG': 1, 'IND_OPER': 2, 'COD_PART': 3, 'COD_ITEM': 4, 'DT_OPER': 5, 'VL_OPER': 6,
            'CST_PIS': 7, 'VL_BC_PIS': 8, 'ALIQ_PIS': 9, 'VL_PIS': 10, 'CST_COFINS': 11,
            'VL_BC_COFINS': 12, 'ALIQ_COFINS': 13, 'VL_COFINS': 14, 'NAT_BC_CRED': 15,
            'IND_ORIG_CRED': 16, 'COD_CTA': 17, 'COD_CCUS': 18, 'DESC_DOC_OPER': 19 
        },
        'F120': {
            'REG': 1, 'NAT_BC_CRED': 2, 'IDENT_BEM_IMOB': 3, 'IND_ORIG_CRED': 4, 'IND_UTIL_BEM_IMOB': 5,
            'VL_OPER_DEP': 6, 'PARC_OPER_NAO_BC_CRED': 7, 'CST_PIS': 8, 'VL_BC_PIS': 9, 'ALIQ_PIS': 10,
            'VL_PIS': 11, 'CST_COFINS': 12, 'VL_BC_COFINS': 13, 'ALIQ_COFINS': 14, 'VL_COFINS': 15,
            'COD_CTA': 16, 'COD_CCUS': 17, 'DESC_BEM_IMOB': 18
        },
        'F130': {
            'REG': 1, 'NAT_BC_CRED': 2, 'IDENT_BEM_IMOB': 3, 'IND_ORIG_CRED': 4, 'IND_UTIL_BEM_IMOB': 5,
            'MES_OPER_AQUIS': 6, 'VL_OPER_AQUIS': 7, 'PARC_OPER_NAO_BC_CRED': 8, 'VL_BC_CRED': 9,
            'IND_NR_PARC': 10, 'CST_PIS': 11, 'VL_BC_PIS': 12, 'ALIQ_PIS': 13, 'VL_PIS': 14,
            'CST_COFINS': 15, 'VL_BC_COFINS': 16, 'ALIQ_COFINS': 17, 'VL_COFINS': 18,
            'COD_CTA': 19, 'COD_CCUS': 20, 'DESC_BEM_IMOB': 21
        },
        'F550': {
            'REG': 1, 'VL_REC_COMP': 2, 'CST_PIS': 3, 'VL_DESC_PIS': 4, 'VL_BC_PIS': 5,
            'ALIQ_PIS': 6, 'VL_PIS': 7, 'CST_COFINS': 8, 'VL_DESC_COFINS': 9, 'VL_BC_COFINS': 10,
            'ALIQ_COFINS': 11, 'VL_COFINS': 12, 'COD_MOD': 13, 'CFOP': 14, 'COD_CTA': 15,
            'INFO_COMPL': 16
        },
        'F600': {
            'REG': 1, 'IND_NAT_RET': 2, 'DT_RET': 3, 'VL_BC_RET': 4, 'VL_RET': 5,
            'COD_REC': 6, 'IND_NAT_REC': 7, 'CNPJ': 8, 'VL_RET_PIS': 9, 'VL_RET_COFINS': 10,
            'IND_DEC': 11
        },
        'A010': {
            'REG': 1, 'CNPJ_A010': 2
        },
        'A100': {
            'REG': 1, 'IND_OPER': 2, 'IND_EMIT': 3, 'COD_PART': 4,
            'COD_SIT': 5, 'SER': 6, 'SUB': 7, 'NUM_DOC': 8,
            'CHV_NFSE': 9, 'DT_DOC': 10, 'DT_EXE_SERV': 11,
            'VL_DOC': 12, 'IND_PGTO': 13, 'VL_DESC': 14,
            'VL_BC_PIS': 15, 'VL_PIS': 16, 'VL_BC_COFINS': 17,
            'VL_COFINS': 18, 'VL_PIS_RET': 19, 'VL_COFINS_RET': 20,
            'VL_ISS': 21
        },
        'A170': {
            'REG': 1, 'NUM_ITEM': 2, 'COD_ITEM': 3, 'DESCR_COMPL': 4,
            'VL_ITEM': 5, 'VL_DESC': 6, 'NAT_BC_CRED': 7,
            'IND_ORIG_CRED': 8, 'CST_PIS': 9, 'VL_BC_PIS_ITEM': 10,
            'ALIQ_PIS': 11, 'VL_PIS_ITEM': 12, 'CST_COFINS': 13, 'VL_BC_COFINS_ITEM': 14, 
            'ALIQ_COFINS': 15, 'VL_COFINS_ITEM': 16, 
            'COD_CTA': 17, 'COD_CCUS': 18
        },
        'I100': {
            'REG': 1, 'VL_REC': 2, 'CST_PIS_COFINS': 3,
            'VL_TOT_DED_GER': 4, 'VL_TOT_DED_ESP': 5,
            'VL_BC_PIS': 6, 'ALIQ_PIS': 7, 'VL_PIS': 8,
            'VL_BC_COFINS': 9, 'ALIQ_COFINS': 10,
            'VL_COFINS': 11, 'INFO_COMPL': 12
        },
        'M100': {
            'REG': 1, 'COD_CRED': 2, 'IND_CRED_ORI': 3, 'VL_BC_PIS': 4, 'ALIQ_PIS': 5,
            'QUANT_BC_PIS': 6, 'ALIQ_PIS_QUANT': 7, 'VL_CRED': 8, 'VL_AJUS_ACRES': 9,
            'VL_AJUS_REDUC': 10, 'VL_CRED_DIF': 11, 'VL_CRED_DISP': 12, 'IND_DESC_CRED': 13,
            'VL_CRED_DESC': 14, 'SLD_CRED': 15
        },
        'M105': {
            'REG': 1, 'NAT_BC_CRED': 2, 'CST_PIS': 3, 'VL_BC_PIS_TOT': 4, 'VL_BC_PIS_CUM': 5,
            'VL_BC_PIS_NC': 6, 'VL_BC_PIS': 7, 'QUANT_BC_PIS_TOT': 8, 'QUANT_BC_PIS': 9,
            'DESC_CRED': 10 

        },
        'M110': {
            'REG': 1, 'IND_AJ': 2, 'VL_AJ': 3, 'COD_AJ': 4, 'NUM_DOC': 5, 'DESCR_AJ': 6, 'DT_REF': 7

        },
        'M150': {
            'REG': 1, 'DET_VALOR_AJ': 2, 'CST_PIS_M115': 3, 'DET_BC_CRED': 4,
            'DET_ALIQ': 5, 'DT_OPER_AJ': 7, 'DESC_AJ': 8,
            'COD_CTA': 9, 'INFO_COMPL': 10
        },
        'M200': {
            'REG': 1, 'VL_TOT_CONT_NC_PER': 2, 'VL_TOT_CRED_DESC': 3, 'VL_TOT_CRED_DESC_ANT': 4,
            'VL_TOT_CONT_NC_DEV': 5, 'VL_RET_NC': 6, 'VL_OUT_DED_NC': 7, 'VL_CONT_NC_REC': 8,
            'VL_TOT_CONT_CUM_PER': 9, 'VL_RET_CUM': 10, 'VL_OUT_DED_CUM': 11, 'VL_CONT_CUM_REC': 12,
            'VL_TOT_CONT_REC': 13
        },
        'M205': {
            'REG': 1, 'NUM_CAMPO': 2, 'COD_REC': 3, 'VL_DEBITO': 4
        },
        'M210': {
            'REG': 1, 'COD_CONT': 2, 'VL_REC_BRT': 3, 'VL_BC_CONT': 4, 'ALIQ_PIS': 5,
            'QUANT_BC_PIS': 6, 'ALIQ_PIS_QUANT': 7, 'VL_CONT_APUR': 8, 'VL_AJUS_ACRES': 9,
            'VL_AJUS_REDUC': 10, 'VL_CONT_DIFER': 11, 'VL_CONT_DIFER_ANT': 12,
            'VL_CONT_PER': 13
        },
        'M220': {
            'REG': 1, 'IND_AJ': 2, 'VL_AJ': 3, 'COD_AJ': 4, 'NUM_DOC': 5, 'DESCR_AJ': 6, 'DT_REF': 7
        },
        'M400': {
            'REG': 1, 'CST_PIS': 2, 'VL_TOT_REC': 3,
            'COD_CTA': 4, 'DESC_COMPL': 5
        },
        'M410': {
            'REG': 1, 'NAT_REC': 2, 'VL_REC': 3,
            'COD_CTA': 4, 'DESC_COMPL': 5
        },
        'M500': {
            'REG': 1, 'COD_CRED': 2, 'IND_CRED_ORI': 3, 'VL_BC_COFINS': 4,
            'ALIQ_COFINS': 5, 'QUANT_BC_COFINS': 6, 'ALIQ_COFINS_QUANT': 7,
            'VL_CRED': 8, 'VL_AJUS_ACRES': 9, 'VL_AJUS_REDUC': 10,
            'VL_CRED_DIFER': 11, 'VL_CRED_DISP': 12, 'IND_DESC_CRED': 13,
            'VL_CRED_DESC': 14, 'SLD_CRED': 15
        },
        'M505': {
            'REG': 1, 'NAT_BC_CRED': 2, 'CST_COFINS': 3, 'VL_BC_COFINS_TOT': 4,
            'VL_BC_COFINS_CUM': 5, 'VL_BC_COFINS_NC': 6, 'VL_BC_COFINS': 7,
            'QUANT_BC_COFINS_TOT': 8, 'QUANT_BC_COFINS': 9,'DESC_CRED': 10
        },
        'M510': {
            'REG': 1, 'IND_AJ': 2, 'VL_AJ': 3, 'COD_AJ':4,
            'NUM_DOC': 5, 'DESCR_AJ': 6, 'DT_REF': 7
        },
        'M515': {
            'REG': 1, 'DET_VALOR_AJ': 2, 'CST_COFINS': 3,
            'DET_BC_CRED': 4, 'DET_ALIQ': 5, 'DT_OPER_AJ': 6,
            'DESC_AJ': 7, 'COD_CTA': 8, 'INFO_COMPL': 9
        },
        'M600': {
            'REG': 1, 'VL_TOT_CRED_DESC': 2, 'VL_TOT_CRED_DESC_ANT': 3,
            'VL_TOT_CONT_NC_DEV': 4, 'VL_RET_NC': 5, 'VL_OUT_DED_NC': 6,
            'VL_CONT_NC_REC': 7, 'VL_TOT_CONT_CUM_PER': 8, 'VL_RET_CUM': 9,
            'VL_OUT_DED_CUM': 10, 'VL_CONT_CUM_REC': 11, 'VL_TOT_CONT_REC': 12
        },
        'M605': {
            'REG': 1, 'NUM_CAMPO': 2, 'COD_REC': 3, 'VL_DEBITO': 4
        },
        'M610': {
            'REG': 1, 'COD_CONT': 2, 'VL_REC_BRT': 3, 'VL_BC_CONT': 4,
            'ALIQ_COFINS': 5, 'QUANT_BC_COFINS': 6, 'ALIQ_COFINS_QUANT': 7,
            'VL_CONT_APUR': 8, 'VL_AJUS_ACRES': 9, 'VL_AJUS_REDUC': 10,
            'VL_CONT_DIFER': 11, 'VL_CONT_DIFER_ANT': 12, 'VL_CONT_PER': 13
        },
        'M620': {
            'REG': 1, 'IND_AJ': 2, 'VL_AJ': 3, 'COD_AJ': 4, 'NUM_DOC': 5,
            'DESCR_AJ': 6, 'DT_REF': 7
        },
        'M800': {
            'REG': 1, 'CST_COFINS': 2, 'VL_TOT_REC': 3, 'COD_CTA': 4, 'DESC_COMPL': 5

        },
        'M810': {
            'REG': 1, 'NAT_REC': 2, 'VL_REC': 3, 'COD_CTA': 4, 'DESC_COMPL_M810': 5
        },
        '1100': {
            'REG': 1, 'PER_APU_CRED': 2, 'ORIG_CRED': 3, 'CNPJ_SUC': 4,
            'COD_CRED': 5, 'VL_CRED_APU': 6, 'VL_CRED_EXT_APU': 7,
            'VL_TOT_CRED_APU': 8, 'VL_CRED_DESC_PA_ANT': 9, 
            'VL_CRED_PER_PA_ANT': 10, 'VL_CRED_DCOMP_PA_ANT': 11,
            'SD_CRED_DISP_EFD': 12, 'VL_CRED_DESC_EFD': 13,
            'VL_CRED_PER_EFD': 14, 'VL_CRED_DCOMP_EFD': 15,
            'VL_CRED_TRANS': 16, 'VL_CRED_OUT': 17, 'SLD_CRED_FIM': 18
        },
        '1500': {
            'REG': 1, 'PER_APU_CRED': 2, 'ORIG_CRED': 3, 'CNPJ_SUC': 4,
            'COD_CRED': 5, 'VL_CRED_APU': 6, 'VL_CRED_EXT_APU': 7,
            'VL_TOT_CRED_APU': 8, 'VL_CRED_DESC_PA_ANT': 9, 
            'VL_CRED_PER_PA_ANT': 10, 'VL_CRED_DCOMP_PA_ANT': 11,
            'SD_CRED_DISP_EFD': 12, 'VL_CRED_DESC_EFD': 13,
            'VL_CRED_PER_EFD': 14, 'VL_CRED_DCOMP_EFD': 15,
            'VL_CRED_TRANS': 16, 'VL_CRED_OUT': 17, 'SLD_CRED_FIM': 18
        },
        '1300': {
            'REG': 1, 'IND_NAT_RET': 2, 'PR_REC_RET': 3, 'VL_RET_APU': 4,
            'VL_RET_DED': 5, 'VL_RET_PER': 6, 'VL_RET_DCOMP': 7,
            'SLD_RET': 8
        },
        '1700': {
            'REG': 1, 'IND_NAT_RET': 2, 'PR_REC_RET': 3, 'VL_RET_APU': 4,
            'VL_RET_DED': 5, 'VL_RET_PER': 6, 'VL_RET_DCOMP': 7,
            'SLD_RET': 8
        },

    }   

class SpedFiscalSchemas:
    """
    Mapeamento e definição dos schemas e mapas de campos dos registros do SPED Fiscal
    """
    SPEDS_FISCAL_SCHEMAS = {
        # NOTA FISCAL (CÓDIGO 01), NOTA FISCAL AVULSA (CÓDIGO 1B),
        # NOTA FISCAL DE PRODUTOR (CÓDIGO 04), NF-e (CÓDIGO 55) e NFC-e (CÓDIGO 65).
        '0000': StructType([
            StructField('REG', StringType(), True), StructField('COD_VER', StringType(), True),
            StructField('COD_FIN', StringType(), True), StructField('DT_INI', StringType(), True),
            StructField('DT_FIN', StringType(), True), StructField('NOME', StringType(), True),
            StructField('CNPJ', StringType(), True), StructField('CPF', StringType(), True),        
            StructField('UF', StringType(), True), StructField('IE', StringType(), True), 
            StructField('COD_MUN', StringType(), True), StructField('IM', StringType(), True), 
            StructField('SUFRAMA', StringType(), True), StructField('IND_PERFIL', StringType(), True),
            StructField('IND_ATIV', StringType(), True)
        ]),
            "0150": StructType([
            StructField('REG', StringType(), True),
            StructField('COD_PARTICIPANTE', StringType(), True),
            StructField('NOME_PARTICIPANTE', StringType(), True),
            StructField('COD_PAIS', StringType(), True),
            StructField('CNPJ_PARTICIPANTE', StringType(), True),
            StructField('CPF_PARTICIPANTE', StringType(), True),
            StructField('IE_PARTICIPANTE', StringType(), True),
            StructField('COD_MUN_PARTICIPANTE', StringType(), True),
            StructField('SUFRAMA_PARTICIPANTE', StringType(), True),
            StructField('END_PARTICIPANTE', StringType(), True),
            StructField('NUM_PARTICIPANTE', StringType(), True),
            StructField('COMPL_PARTICIPANTE', StringType(), True),
            StructField('BAIRRO_PARTICIPANTE', StringType(), True)
        ]),
        "0200": StructType([
            StructField('REG', StringType(), True),
            StructField('COD_ITEM', StringType(), True),
            StructField('DESCR_ITEM', StringType(), True),
            StructField('COD_BARRA', StringType(), True),
            StructField('COD_ANT_ITEM', StringType(), True),
            StructField('UNID_INV', StringType(), True),
            StructField('TIPO_ITEM', StringType(), True),
            StructField('COD_NCM', StringType(), True),
            StructField('EX_IPI', StringType(), True),
            StructField('COD_GEN', StringType(), True),
            StructField('COD_LST', StringType(), True),
            StructField('ALIQ_ICMS', StringType(), True),
            StructField('CEST', StringType(), True),

        ]),
        "0300": StructType([
            StructField('REG', StringType(), True), StructField('COD_IND_BEM', StringType(), True),
            StructField('IDENT_MERC', StringType(), True), StructField('DESCR_ITEM', StringType(), True),
            StructField('COD_PRNC', StringType(), True), StructField('COD_CTA', StringType(), True),
            StructField('NR_PARC', StringType(), True)
        ]),
        "0500": StructType([
            StructField('REG', StringType(), True), StructField('DT_ALT', StringType(), True),
            StructField('COD_NAT_CC', StringType(), True), StructField('IND_CTA', StringType(), True),
            StructField('NIVEL', StringType(), True), StructField('COD_CTA', StringType(), True),
            StructField('NOME_CTA', StringType(), True)
        ]),
        'C100': StructType([
            StructField('REG', StringType(), True), StructField('IND_OPER', StringType(), True),
            StructField('IND_EMIT', StringType(), True), StructField('COD_PART', StringType(), True),
            StructField('COD_MOD', StringType(), True), StructField('COD_SIT', StringType(), True),
            StructField('SER', StringType(), True), StructField('NUM_DOC', StringType(), True),
            StructField('CHV_NFE', StringType(), True), StructField('DT_DOC', StringType(), True),
            StructField('DT_E_S', StringType(), True), StructField('VL_DOC', StringType(), True),
            StructField('IND_PGTO', StringType(), True), StructField('VL_DESC', StringType(), True), 
            StructField('VL_ABAT_NT', StringType(), True), StructField('VL_MERC', StringType(), True),
            StructField('IND_FRT', StringType(), True), StructField('VL_FRT', StringType(), True),
            StructField('VL_SEG', StringType(), True), StructField('VL_OUT_DA', StringType(), True),
            StructField('VL_BC_ICMS', StringType(), True), StructField('VL_ICMS', StringType(), True),
            StructField('VL_BC_ICMS_ST', StringType(), True), StructField('VL_ICMS_ST', StringType(), True), 
            StructField('VL_IPI', StringType(), True), StructField('VL_PIS', StringType(), True), 
            StructField('VL_COFINS', StringType(), True), StructField('VL_PIS_ST', StringType(), True),  
            StructField('VL_COFINS_ST', StringType(), True),
        ]),
        'C170': StructType([
            # ITENS DO DOCUMENTO (CÓDIGO 01, 1B, 04 e 55).
            StructField('REG', StringType(), True), StructField('NUM_ITEM', StringType(), True),
            StructField('COD_ITEM', StringType(), True), StructField('DESCR_COMPL', StringType(), True),
            StructField('QTD', StringType(), True), StructField('UNID', StringType(), True),
            StructField('VL_ITEM', StringType(), True), StructField('VL_DESC', StringType(), True),
            StructField('IND_MOV', StringType(), True), StructField('CST_ICMS', StringType(), True),
            StructField('CFOP', StringType(), True), StructField('COD_NAT', StringType(), True),
            StructField('VL_BC_ICMS', StringType(), True), StructField('ALIQ_ICMS', StringType(), True),
            StructField('VL_ICMS_ITEM', StringType(), True), StructField('VL_BC_ICMS_ST', StringType(), True),
            StructField('ALIQ_ST', StringType(), True), StructField('VL_ICMS_ST', StringType(), True),
            StructField('IND_APUR', StringType(), True), StructField('CST_IPI', StringType(), True),
            StructField('COD_ENQ', StringType(), True), StructField('VL_BC_IPI', StringType(), True),
            StructField('ALIQ_IPI', StringType(), True), StructField('VL_IPI', StringType(), True),
            StructField('CST_PIS', StringType(), True), StructField('VL_BC_PIS', StringType(), True),
            StructField('ALIQ_PIS_PERC', StringType(), True), StructField('QUANT_BC_PIS', StringType(), True),
            StructField('ALIQ_PIS_RS', StringType(), True), StructField('VL_PIS', StringType(), True),
            StructField('CST_COFINS', StringType(), True),  StructField('VL_BC_COFINS', StringType(), True),
            StructField('ALIQ_COFINS_PERC', StringType(), True), StructField('QUANT_BC_COFINS', StringType(), True),
            StructField('ALIQ_COFINS_RS', StringType(), True), StructField('VL_COFINS', StringType(), True),
            StructField('COD_CTA', StringType(), True), StructField('VL_ABAT_NT', StringType(), True)
        ]),

        'C190': StructType([
            # REGISTRO ANALÍTICO DO DOCUMENTO (CÓDIGO 01, 1B, 04, 55 e 65)
            StructField('REG', StringType(), True), StructField('CST_ICMS', StringType(), True),
            StructField('CFOP', StringType(), True), StructField('ALIQ_ICMS', StringType(), True),
            StructField('VL_OPR', StringType(), True), StructField('VL_BC_ICMS', StringType(), True),
            StructField('VL_ICMS_TOTAL', StringType(), True), StructField('VL_BC_ICMS_ST', StringType(), True),
            StructField('VL_ICMS_ST', StringType(), True), StructField('VL_RED_BC', StringType(), True),
            StructField('VL_IPI', StringType(), True), StructField('COD_OBS', StringType(), True) 
        ]),
        'C191': StructType([
            StructField('REG', StringType(), True),
            StructField('VL_FCP_OP', StringType(), True),
            StructField('VL_FCP_ST', StringType(), True),
            StructField('VL_FCP_RET', StringType(), True)
        ]),
        'C195': StructType([
            StructField('REG', StringType(), True),
            StructField('COD_OBS', StringType(), True),
            StructField('TXT_COMPL', StringType(), True)
        ]),
        'C197': StructType([
            StructField('REG', StringType(), True),
            StructField('COD_AJ', StringType(), True),
            StructField('DESCR_COMPL_AJ', StringType(), True),
            StructField('COD_ITEM', StringType(), True),
            StructField('VL_BC_ICMS', StringType(), True),
            StructField('ALIQ_ICMS', StringType(), True),
            StructField('VL_ICMS', StringType(), True),
            StructField('VL_OUTROS', StringType(), True)
        ]),
        'C500': StructType([
            # NOTA FISCAL/CONTA DE ENERGIA ELÉTRICA (CÓDIGO 06), NOTA
            # FISCAL DE ENERGIA ELÉTRICA ELETRÔNICA – NF3e (CÓDIGO 66), NOTA
            # FISCAL/CONTA DE FORNECIMENTO D'ÁGUA CANALIZADA (CÓDIGO 29) E NOTA
            # FISCAL CONSUMO FORNECIMENTO DE GÁS (CÓDIGO 28).
            
            StructField('REG', StringType(), True), StructField('IND_OPER', StringType(), True),
            StructField('IND_EMIT', StringType(), True), StructField('COD_PART', StringType(), True),
            StructField('COD_MOD', StringType(), True), StructField('COD_SIT', StringType(), True),
            StructField('SER', StringType(), True), StructField('SUB', StringType(), True),
            StructField('COD_CONS', StringType(), True), StructField('NUM_DOC', StringType(), True),
            StructField('DT_DOC', StringType(), True), StructField('DT_E_S', StringType(), True),
            StructField('VL_DOC', StringType(), True), StructField('VL_DESC', StringType(), True),
            StructField('VL_FORN', StringType(), True), StructField('VL_SERV_NT', StringType(), True),
            StructField('VL_TERC', StringType(), True), StructField('VL_DA', StringType(), True),
            StructField('VL_BC_ICMS', StringType(), True), StructField('VL_ICMS', StringType(), True),
            StructField('VL_BC_ICMS_ST', StringType(), True), StructField('VL_ICMS_ST', StringType(), True),
            StructField('COD_INF', StringType(), True), StructField('VL_PIS', StringType(), True),
            StructField('VL_COFINS', StringType(), True), StructField('TP_LIGACAO', StringType(), True),
            StructField('COD_GRUPO_TENSAO', StringType(), True), StructField('CHV_DOC_E', StringType(), True),
            StructField('FIN_DOC_E', StringType(), True), StructField('CHV_DOCE_REF', StringType(), True),
            StructField('IND_DEST', StringType(), True), StructField('COD_MUN_DEST', StringType(), True),
            StructField('COD_CTA', StringType(), True), StructField('COD_MOD_DOC_REF', StringType(), True),
            StructField('HASH_DOC_REF', StringType(), True), StructField('SERV_DOC_REF', StringType(), True),
            StructField('NUM_DOC_REF', StringType(), True), StructField('MES_DOC_REF', StringType(), True),
            StructField('ENER_INJET', StringType(), True), StructField('OUTRAS_DED', StringType(), True)
        ]),
        'C510': StructType([
            # ITENS DO DOCUMENTO NOTA FISCAL/CONTA ENERGIA ELÉTRICA
            # (CÓDIGO 06), NOTA FISCAL/CONTA DE FORNECIMENTO D'ÁGUA CANALIZADA
            # (CÓDIGO 29) E NOTA FISCAL/CONTA DE FORNECIMENTO DE GÁS (CÓDIGO 28).

            StructField('REG', StringType(), True), StructField('NUM_ITEM', StringType(), True),
            StructField('COD_ITEM', StringType(), True), StructField('COD_CLASS', StringType(), True),
            StructField('QTD', StringType(), True), StructField('UNID', StringType(), True),
            StructField('VL_ITEM', StringType(), True), StructField('VL_DESC', StringType(), True),
            StructField('CST_ICMS', StringType(), True), StructField('CFOP', StringType(), True),
            StructField('VL_BC_ICMS', StringType(), True), StructField('ALIQ_ICMS', StringType(), True),
            StructField('VL_ICMS', StringType(), True), StructField('VL_BC_ICMS_ST', StringType(), True),
            StructField('ALIQ_ST', StringType(), True), StructField('VL_ICMS_ST', StringType(), True),
            StructField('IND_REC', StringType(), True), StructField('COD_PART', StringType(), True),
            StructField('VL_PIS', StringType(), True), StructField('VL_COFINS', StringType(), True),
            StructField('COD_CTA', StringType(), True)
        ]),
        'C590': StructType([
            # REGISTRO ANALÍTICO DO DOCUMENTO – NOTA FISCAL/CONTA DE
            # ENERGIA ELÉTRICA (CÓDIGO 06), NOTA FISCAL DE ENERGIA ELÉTRICA
            # ELETRÔNICA – NF3e (CÓDIGO 66), NOTA FISCAL/CONTA DE FORNECIMENTO D'ÁGUA
            # CANALIZADA (CÓDIGO 29) E NOTA FISCAL CONSUMO FORNECIMENTO DE GÁS
            # (CÓDIGO 28).
            StructField('REG', StringType(), True), StructField('CST_ICMS', StringType(), True),
            StructField('CFOP', StringType(), True), StructField('ALIQ_ICMS', StringType(), True),
            StructField('VL_OPR', StringType(), True), StructField('VL_BC_ICMS', StringType(), True),
            StructField('VL_ICMS', StringType(), True), StructField('VL_BC_ICMS_ST', StringType(), True),
            StructField('VL_ICMS_ST', StringType(), True), StructField('VL_RED_BC', StringType(), True),
            StructField('COD_OBS', StringType(), True)
        ]),
        'D100': StructType([
            # NOTA FISCAL DE SERVIÇO DE TRANSPORTE (CÓDIGO 07) E
            # CONHECIMENTOS DE TRANSPORTE RODOVIÁRIO DE CARGAS (CÓDIGO 08),
            # CONHECIMENTOS DE TRANSPORTE DE CARGAS AVULSO (CÓDIGO 8B), AQUAVIÁRIO
            # DE CARGAS (CÓDIGO 09), AÉREO (CÓDIGO 10), FERROVIÁRIO DE CARGAS (CÓDIGO
            # 11), MULTIMODAL DE CARGAS (CÓDIGO 26), NOTA FISCAL DE TRANSPORTE
            # FERROVIÁRIO DE CARGA (CÓDIGO 27), CONHECIMENTO DE TRANSPORTE
            # ELETRÔNICO – CT-e (CÓDIGO 57), CONHECIMENTO DE TRANSPORTE ELETRÔNICO
            # PARA OUTROS SERVIÇOS - CT-e OS (CÓDIGO 67) E BILHETE DE PASSAGEM
            # ELETRÔNICO – BP-e (CÓDIGO 63)

            StructField('REG', StringType(), True), StructField('IND_OPER', StringType(), True),
            StructField('IND_EMIT', StringType(), True), StructField('COD_PART', StringType(), True),
            StructField('COD_MOD', StringType(), True), StructField('COD_SIT', StringType(), True),
            StructField('SER', StringType(), True), StructField('SUB', StringType(), True),
            StructField('NUM_DOC', StringType(), True), StructField('CHV_CTE', StringType(), True),
            StructField('DT_DOC', StringType(), True), StructField('DT_A_P', StringType(), True),
            StructField('TP_CT_E', StringType(), True), StructField('CHV_CTE_REF', StringType(), True),
            StructField('VL_DOC', StringType(), True), StructField('VL_DESC', StringType(), True),
            StructField('IND_FRT', StringType(), True), StructField('VL_SERV', StringType(), True),
            StructField('VL_BC_ICMS', StringType(), True), StructField('VL_ICMS', StringType(), True),
            StructField('VL_NT', StringType(), True), StructField('COD_INF', StringType(), True),
            StructField('COD_CTA', StringType(), True), StructField('COD_MUN_ORIG', StringType(), True), 
            StructField('COD_MUN_DEST', StringType(), True)
        ]),
        'D190': StructType([
            # REGISTRO ANALÍTICO DOS DOCUMENTOS (CÓDIGO 07, 08, 8B, 09, 10,
            # 11, 26, 27, 57, 63 e 67)

            StructField('REG', StringType(), True), StructField('CST_ICMS', StringType(), True),
            StructField('CFOP', StringType(), True), StructField('ALIQ_ICMS_ITEM', StringType(), True),
            StructField('VL_OPR_ITEM', StringType(), True), StructField('VL_BC_ICMS_ITEM', StringType(), True),
            StructField('VL_ICMS_ITEM', StringType(), True), StructField('VL_RED_BC_ITEM', StringType(), True),
            StructField('COD_OBS', StringType(), True)    
        ]),
        "D500": StructType([
            # NOTA FISCAL DE SERVIÇO DE COMUNICAÇÃO (CÓDIGO 21) E NOTA
            # FISCAL DE SERVIÇO DE TELECOMUNICAÇÃO (CÓDIGO 22).

            StructField('REG', StringType(), True), StructField('IND_OPER', StringType(), True),
            StructField('IND_EMIT', StringType(), True), StructField('COD_PART', StringType(), True),
            StructField('COD_MOD', StringType(), True), StructField('COD_SIT', StringType(), True),
            StructField('SER', StringType(), True), StructField('SUB', StringType(), True),
            StructField('NUM_DOC', StringType(), True), StructField('DT_DOC', StringType(), True),
            StructField('DT_A_P', StringType(), True), StructField('VL_DOC', StringType(), True),
            StructField('VL_DESC', StringType(), True), StructField('VL_SERV', StringType(), True),
            StructField('VL_SERV_NT', StringType(), True), StructField('VL_TERC', StringType(), True),
            StructField('VL_DA', StringType(), True), StructField('VL_BC_ICMS', StringType(), True),
            StructField('VL_ICMS_TOTAL', StringType(), True), StructField('COD_INF', StringType(), True),
            StructField('VL_PIS', StringType(), True), StructField('VL_COFINS', StringType(), True),
            StructField('COD_CTA', StringType(), True), StructField('TP_ASSINANTE', StringType(), True)
        ]),
        'D510': StructType([
            # ITENS DO DOCUMENTO – NOTA FISCAL DE SERVIÇO DE
            # COMUNICAÇÃO (CÓDIGO 21) E SERVIÇO DE TELECOMUNICAÇÃO (CÓDIGO 22).

            StructField('REG', StringType(), True), StructField('NUM_ITEM', StringType(), True),
            StructField('COD_ITEM', StringType(), True), StructField('COD_CLASS', StringType(), True),
            StructField('QTD', StringType(), True), StructField('UNID', StringType(), True),
            StructField('VL_ITEM', StringType(), True), StructField('VL_DESC', StringType(), True),
            StructField('CST_ICMS', StringType(), True), StructField('CFOP', StringType(), True),
            StructField('VL_BC_ICMS_ITEM', StringType(), True), StructField('ALIQ_ICMS', StringType(), True),
            StructField('VL_ICMS_ITEM', StringType(), True), StructField('VL_BC_ICMS_UF', StringType(), True),
            StructField('VL_ICMS_UF', StringType(), True), StructField('IND_REC', StringType(), True),
            StructField('COD_PART', StringType(), True), StructField('VL_PIS', StringType(), True),
            StructField('VL_COFINS', StringType(), True), StructField('COD_CTA', StringType(), True)
        ]),
        'D590': StructType([
            # REGISTRO ANALÍTICO DO DOCUMENTO (CÓDIGO 21 E 22). 

            StructField('REG', StringType(), True), StructField('CST_ICMS', StringType(), True),
            StructField('CFOP', StringType(), True), StructField('ALIQ_ICMS', StringType(), True),
            StructField('VL_OPR', StringType(), True), StructField('VL_BC_ICMS', StringType(), True),
            StructField('VL_ICMS_ANALITICO', StringType(), True), StructField('VL_BC_ICMS_UF', StringType(), True),
            StructField('VL_ICMS_UF', StringType(), True), StructField('VL_RED_BC', StringType(), True),
            StructField('COD_OBS', StringType(), True)
        ]),
        "E100": StructType([
            StructField('REG', StringType(), True),
            StructField('DT_INI', StringType(), True),
            StructField('DT_FIN', StringType(), True)

        ]),
        "E110": StructType([
            StructField('REG', StringType(), True),
            StructField('VL_TOT_DEBITOS', StringType(), True),
            StructField('VL_AJ_DEBITOS', StringType(), True),
            StructField('VL_TOT_AJ_DEBITOS', StringType(), True),
            StructField('VL_ESTORNOS_CRED', StringType(), True),
            StructField('VL_TOT_CREDITOS', StringType(), True),
            StructField('VL_AJ_CREDITOS', StringType(), True),
            StructField('VL_TOT_AJ_CREDITOS', StringType(), True),
            StructField('VL_ESTORNOS_DEB', StringType(), True),
            StructField('VL_SLD_CREDOR_ANT', StringType(), True),
            StructField('VL_SLD_APURADO', StringType(), True),
            StructField('VL_TOT_DED', StringType(), True),
            StructField('VL_ICMS_RECOLHER', StringType(), True),
            StructField('VL_SLD_CREDOR_TRANSPORTAR', StringType(), True),
            StructField('DEB_ESP', StringType(), True),
        ]),
        "E111": StructType([
            StructField('REG', StringType(), True),
            StructField('COD_AJ_APUR', StringType(), True),
            StructField('DESC_COMPL_AJ', StringType(), True),
            StructField('VL_AJ_APUR', StringType(), True)

        ]),
        "E200": StructType([
            StructField('REG', StringType(), True),
            StructField('UF', StringType(), True),
            StructField('DT_INI', StringType(), True),
            StructField('DT_FIN', StringType(), True)
        ]),
        "E210": StructType([
            StructField('REG', StringType(), True),
            StructField('IND_MOV_ST', StringType(), True),
            StructField('VL_SLD_CRED_ANT_ST', StringType(), True),
            StructField('VL_DEVOL_ST', StringType(), True),
            StructField('VL_RESSARC_ST', StringType(), True),
            StructField('VL_OUT_CRED_ST', StringType(), True),
            StructField('VL_AJ_CREDITOS_ST', StringType(), True),
            StructField('VL_RETENÇAO_ST', StringType(), True),
            StructField('VL_OUT_DEB_ST', StringType(), True),
            StructField('VL_AJ_DEBITOS_ST', StringType(), True),
            StructField('VL_SLD_DEV_ANT_ST', StringType(), True),
            StructField('VL_DEDUÇÕES_ST', StringType(), True),
            StructField('VL_ICMS_RECOL_ST', StringType(), True),
            StructField('VL_SLD_CRED_ST_TRANSPORTAR', StringType(), True),
            StructField('DEB_ESP_ST', StringType(), True),
        ]),
        "E220": StructType([
            StructField('REG', StringType(), True),
            StructField('COD_AJ_APUR', StringType(), True),
            StructField('DESCR_COMPL_AJ', StringType(), True),
            StructField('VL_AJ_APUR', StringType(), True),
        ]),
        "E250": StructType([
            StructField('REG', StringType(), True),
            StructField('COD_OR', StringType(), True),
            StructField('VL_OR', StringType(), True),
            StructField('DT_VCTO', StringType(), True),
            StructField('COD_REC', StringType(), True),
            StructField('NUM_PROC', StringType(), True),
            StructField('IND_PROC', StringType(), True),
            StructField('PROC', StringType(), True),
            StructField('TXT_COMPL', StringType(), True),
            StructField('MES_REF', StringType(), True),
        ]),
        'G110': StructType([
            #  ICMS – ATIVO PERMANENTE – CIAP
            StructField('REG', StringType(), True), StructField('DT_INI', StringType(), True),
            StructField('DT_FIN', StringType(), True), StructField('SALDO_IN_ICMS', StringType(), True),
            StructField('SOM_PARC', StringType(), True), StructField('VL_TRIB_EXP', StringType(), True),
            StructField('VL_TOTAL', StringType(), True), StructField('IND_PER_SAI', StringType(), True),
            StructField('ICMS_APROP', StringType(), True), StructField('SOM_ICMS_OC', StringType(), True)       
        ]),
        'G125': StructType([
            # MOVIMENTAÇÃO DE BEM OU COMPONENTE DO ATIVO IMOBILIZADO

            StructField('REG', StringType(), True), StructField('COD_IND_BEM', StringType(), True),
            StructField('DT_MOV', StringType(), True), StructField('TIPO_MOV', StringType(), True),
            StructField('VL_IMOB_ICMS_OP', StringType(), True), StructField('VL_IMOB_ICMS_ST', StringType(), True),
            StructField('VL_IMOB_ICMS_FRT', StringType(), True), StructField('VL_IMOB_ICMS_DIF', StringType(), True),
            StructField('NUM_PARC', StringType(), True), StructField('VL_PARC_PASS', StringType(), True),
        ]),
        'G130': StructType([
            StructField('REG', StringType(), True), StructField('IND_EMIT', StringType(), True), StructField('COD_PART', StringType(), True),
            StructField('COD_MOD', StringType(), True), StructField('SERIE', StringType(), True), StructField('NUM_DOC', StringType(), True),
            StructField('CHV_NFE_CTE', StringType(), True), StructField('DT_DOC', StringType(), True), StructField('NUM_DA', StringType(), True)
        ])

    }
    SPEDS_FISCAL_FIELDS_MAP = {

        '0000': {
            'REG': 1, 'COD_VER': 2, 'COD_FIN': 3, 'DT_INI': 4, 'DT_FIN': 5,
            'NOME': 6, 'CNPJ': 7, 'CPF': 8, 'UF': 9, 'IE': 10, 'COD_MUN': 11, 'IM': 12,
            'SUFRAMA': 13, 'IND_PERFIL': 14, 'IND_ATIV': 15
        },
        '0150': {
            'REG': 1, 'COD_PARTICIPANTE': 2, 'NOME_PARTICIPANTE': 3, 'COD_PAIS': 4, 'CNPJ_PARTICIPANTE': 5, 'CPF_PARTICIPANTE': 6,
            'IE_PARTICIPANTE': 7, 'COD_MUN_PARTICIPANTE': 8, 'SUFRAMA_PARTICIPANTE': 9, 'END_PARTICIPANTE': 10, 'NUM_PARTICIPANTE': 11, 'COMPL_PARTICIPANTE': 12,
            'BAIRRO_PARTICIPANTE': 13
        },
        '0200': {
            'REG': 1, 'COD_ITEM': 2, 'DESCR_ITEM': 3, 'COD_BARRA': 4, 'COD_ANT_ITEM': 5,
            'UNID_INV': 6, 'TIPO_ITEM': 7, 'COD_NCM': 8, 'EX_IPI': 9, 'COD_GEN': 10,
            'COD_LST': 11, 'ALIQ_ICMS': 12, 'CEST': 13

        },
        '0300': {
            'REG': 1, 'COD_IND_BEM': 2, 'IDENT_MERC': 3, 'DESCR_ITEM': 4, 'COD_PRNC': 5, 'COD_CTA': 6,
            'NR_PARC': 7

        },
        '0500': {
            'REG': 1, 'DT_ALT': 2, 'COD_NAT_CC': 3, 'IND_CTA': 4, 'NIVEL': 5, 'COD_CTA': 6, 'NOME_CTA': 7
        },
        'C100': { 
                'REG': 1, 'IND_OPER': 2, 'IND_EMIT': 3, 'COD_PART': 4, 'COD_MOD': 5,
                'COD_SIT': 6, 'SER': 7, 'NUM_DOC': 8, 'CHV_NFE': 9, 'DT_DOC': 10, 
                'DT_E_S': 11, 'VL_DOC': 12, 'IND_PGTO': 13, 'VL_DESC': 14, 'VL_ABAT_NT': 15,
                'VL_MERC': 16, 'IND_FRT': 17, 'VL_FRT': 18, 'VL_SEG': 19, 'VL_OUT_DA': 20,
                'VL_BC_ICMS': 21, 'VL_ICMS': 22, 'VL_BC_ICMS_ST': 23, 'VL_ICMS_ST': 24, 
                'VL_IPI': 25, 'VL_PIS': 26, 'VL_COFINS': 27,  'VL_PIS_ST': 28, 'VL_COFINS_ST': 29
                
                
            },
            'C170': { 
                'REG': 1, 'NUM_ITEM': 2, 'COD_ITEM': 3, 'DESCR_COMPL': 4, 'QTD': 5,
                'UNID': 6, 'VL_ITEM': 7, 'VL_DESC': 8, 'IND_MOV': 9, 'CST_ICMS': 10,
                'CFOP': 11, 'COD_NAT': 12, 'VL_BC_ICMS': 13, 'ALIQ_ICMS': 14,
                'VL_ICMS_ITEM': 15, 'VL_BC_ICMS_ST': 16, 'ALIQ_ST': 17, 'VL_ICMS_ST': 18,
                'IND_APUR': 19, 'CST_IPI': 20, 'COD_ENQ': 21, 'VL_BC_IPI': 22,
                'ALIQ_IPI': 23, 'VL_IPI': 24, 'CST_PIS': 25, 'VL_BC_PIS': 26, 
                'ALIQ_PIS_PERC': 27, 'QUANT_BC_PIS': 28, 'ALIQ_PIS_RS': 29, 'VL_PIS': 30,
                'CST_COFINS': 31, 'VL_BC_COFINS': 32, 'ALIQ_COFINS_PERC': 33, 'QUANT_BC_COFINS': 34,
                'ALIQ_COFINS_RS': 35, 'VL_COFINS': 36, 'COD_CTA': 37, 'VL_ABAT_NT': 38
            },
            'C190': { 
                'REG': 1, 'CST_ICMS': 2, 'CFOP': 3, 'ALIQ_ICMS': 4, 'VL_OPR': 5, 'VL_BC_ICMS': 6,
                'VL_ICMS_TOTAL': 7, 'VL_BC_ICMS_ST': 8, 'VL_ICMS_ST': 9, 'VL_RED_BC': 10, 'VL_IPI': 11,
                'COD_OBS': 12
            },
            'C191': {
                'REG': 1, 'VL_FCP_OP': 2, 'VL_FCP_ST': 3, 'VL_FCP_RET': 4, 
            },
            'C195': {
                'REG': 1, 'COD_OBS': 2, 'TXT_COMPL': 3, 
            },
            'C197': {
                'REG': 1, 'COD_AJ': 2, 'DESCR_COMPL_AJ': 3, 'COD_ITEM': 4, 'VL_BC_ICMS': 5,
                'ALIQ_ICMS': 6, 'VL_ICMS': 7, 'VL_OUTROS': 8
            },
            'C500': {
                'REG': 1, 'IND_OPER': 2, 'IND_EMIT': 3, 'COD_PART': 4, 'COD_MOD': 5,
                'COD_SIT': 6, 'SER': 7, 'SUB': 8, 'COD_CONS': 9, 'NUM_DOC': 10,
                'DT_DOC': 11, 'DT_E_S': 12, 'VL_DOC': 13, 'VL_DESC': 14, 'VL_FORN': 15,
                'VL_SERV_NT': 16, 'VL_TERC': 17, 'VL_DA': 18, 'VL_BC_ICMS': 19,
                'VL_ICMS': 20, 'VL_BC_ICMS_ST': 21, 'VL_ICMS_ST': 22, 'COD_INF': 23,
                'VL_PIS': 24, 'VL_COFINS': 25, 'TP_LIGACAO': 26, 'COD_GRUPO_TENSAO': 27,
                'CHV_DOC_E': 28, 'FIN_DOC_E': 29, 'CHV_DOCE_REF': 30, 'IND_DEST': 31,
                'COD_MUN_DEST': 32, 'COD_CTA': 33, 'COD_MOD_DOC_REF': 34, 
                'HASH_DOC_REF': 35, 'SERV_DOC_REF': 36, 'NUM_DOC_REF': 37,
                'MES_DOC_REF': 38, 'ENER_INJET': 39, 'OUTRAS_DED': 40
            },
            'C510': {
                'REG': 1, 'NUM_ITEM': 2, 'COD_ITEM': 3, 'COD_CLASS': 4, 'QTD': 5,
                'UNID': 6, 'VL_ITEM': 7, 'VL_DESC': 8, 'CST_ICMS': 9, 'CFOP': 10, 'VL_BC_ICMS': 11,
                'ALIQ_ICMS': 12, 'VL_ICMS': 13, 'VL_BC_ICMS_ST': 14, 'ALIQ_ST': 15, 'VL_ICMS_ST': 16,
                'IND_REC': 17, 'COD_PART': 18, 'VL_PIS': 19, 'VL_COFINS': 20, 'COD_CTA': 21
            },
            'C590': {
                'REG': 1, 'CST_ICMS': 2, 'CFOP': 3, 'ALIQ_ICMS': 4, 'VL_OPR': 5,
                'VL_BC_ICMS': 6, 'VL_ICMS': 7, 'VL_BC_ICMS_ST': 8, 'VL_ICMS_ST': 9,
                'VL_RED_BC': 10, 'COD_OBS': 11
            },
            'D100': {
                'REG': 1, 'IND_OPER': 2, 'IND_EMIT': 3, 'COD_PART': 4, 'COD_MOD': 5,
                'COD_SIT': 6, 'SER': 7, 'SUB': 8, 'NUM_DOC': 9, 'CHV_CTE': 10, 'DT_DOC': 11,
                'DT_A_P': 12, 'TP_CT_E': 13, 'CHV_CTE_REF': 14, 'VL_DOC': 15, 'VL_DESC': 16,    
                'IND_FRT': 17, 'VL_SERV': 18, 'VL_BC_ICMS': 19, 'VL_ICMS': 20, 'VL_NT': 21,
                'COD_INF': 22, 'COD_CTA': 23, 'COD_MUN_ORIG': 24, 'COD_MUN_DEST': 25
            },
            'D190': {
                'REG': 1, 'CST_ICMS': 2, 'CFOP': 3, 'ALIQ_ICMS_ITEM': 4, 'VL_OPR_ITEM': 5,
                'VL_BC_ICMS_ITEM': 6, 'VL_ICMS_ITEM': 7, 'VL_RED_BC_ITEM': 8, 'COD_OBS': 9
            },
            'D500': {
                'REG': 1, 'IND_OPER': 2, 'IND_EMIT': 3, 'COD_PART': 4, 'COD_MOD': 5,
                'COD_SIT': 6, 'SER': 7, 'SUB': 8, 'NUM_DOC': 9, 'DT_DOC': 10,
                'DT_A_P': 11, 'VL_DOC': 12, 'VL_DESC': 13, 'VL_SERV': 14, 'VL_SERV_NT': 15,
                'VL_TERC': 16, 'VL_DA': 17, 'VL_BC_ICMS': 18, 'VL_ICMS_TOTAL': 19, 'COD_INF': 20,
                'VL_PIS': 21, 'VL_COFINS': 22, 'COD_CTA': 23, 'TP_ASSINANTE': 24
            },
            'D510': {
                'REG': 1, 'NUM_ITEM': 2, 'COD_ITEM': 3, 'COD_CLASS': 4, 'QTD': 5,
                'UNID': 6, 'VL_ITEM': 7, 'VL_DESC': 8, 'CST_ICMS': 9, 'CFOP': 10, 'VL_BC_ICMS_ITEM': 11,
                'ALIQ_ICMS': 12, 'VL_ICMS_ITEM': 13, 'VL_BC_ICMS_UF': 14, 'VL_ICMS_UF': 15,
                'IND_REC': 16, 'COD_PART': 17, 'VL_PIS': 18, 'VL_COFINS': 19, 'COD_CTA': 20
            },
            'D590': {
                'REG': 1, 'CST_ICMS': 2, 'CFOP': 3, 'ALIQ_ICMS': 4, 'VL_OPR': 5,
                'VL_BC_ICMS': 6, 'VL_ICMS_ANALITICO': 7, 'VL_BC_ICMS_UF': 8, 'VL_ICMS_UF': 9,
                'VL_RED_BC': 10, 'COD_OBS': 11
            },
            'E100': {
                'REG': 1, 'DT_INI': 2, 'DT_FIN': 3
            },
            'E110': {
                'REG': 1, 'VL_TOT_DEBITOS': 2, 'VL_AJ_DEBITOS': 3, 'VL_TOT_AJ_DEBITOS': 4, 'VL_ESTORNOS_CRED': 5, 'VL_TOT_CREDITOS': 6,
                'VL_AJ_CREDITOS': 7, 'VL_TOT_AJ_CREDITOS': 8, 'VL_ESTORNOS_DEB': 9, 'VL_SLD_CREDOR_ANT': 10, 'VL_SLD_APURADO': 11, 
                'VL_TOT_DED': 12, 'VL_ICMS_RECOLHER': 13, 'VL_SLD_CREDOR_TRANSPORTAR': 14, 'DEB_ESP': 15
            },
            'E111': {
                'REG': 1, 'COD_AJ_APUR': 2, 'DESC_COMPL_AJ': 3, 'VL_AJ_APUR': 4
            },
            'E200': {
                'REG': 1, 'UF': 2, 'DT_INI': 3, 'DT_FIN': 4
            },

            'E210': {
                'REG': 1, 'IND_MOV_ST': 2, 'VL_SLD_CRED_ANT_ST': 3, 'VL_DEVOL_ST': 4, 'VL_RESSARC_ST': 5,
                'VL_OUT_CRED_ST': 6, 'VL_AJ_CREDITOS_ST': 7, 'VL_RETENÇAO_ST': 8, 'VL_OUT_DEB_ST': 9,
                'VL_AJ_DEBITOS_ST': 10, 'VL_SLD_DEV_ANT_ST': 11, 'VL_DEDUÇÕES_ST': 12, 'VL_ICMS_RECOL_ST': 13,
                'VL_SLD_CRED_ST_TRANSPORTAR': 14, 'DEB_ESP_ST': 15
            },
            'E220': {
                'REG': 1, 'COD_AJ_APUR': 2, 'DESCR_COMPL_AJ': 3, 'VL_AJ_APUR': 4
            },
            'E250': {
                'REG': 1, 'COD_OR': 2, 'VL_OR': 3, 'DT_VCTO': 4, 'COD_REC': 5, 'NUM_PROC': 6,
                'IND_PROC': 7, 'PROC': 8, 'TXT_COMPL': 9, 'MES_REF': 10
            },

            'G110': {
                'REG': 1, 'DT_INI': 2, 'DT_FIN': 3, 'SALDO_IN_ICMS': 4, 'SOM_PARC': 5,
                'VL_TRIB_EXP': 6, 'VL_TOTAL': 7, 'IND_PER_SAI': 8, 'ICMS_APROP': 9,
                'SOM_ICMS_OC': 10
            },
            'G125': {
                'REG': 1, 'COD_IND_BEM': 2, 'DT_MOV': 3, 'TIPO_MOV': 4, 'VL_IMOB_ICMS_OP': 5,
                'VL_IMOB_ICMS_ST': 6, 'VL_IMOB_ICMS_FRT': 7, 'VL_IMOB_ICMS_DIF': 8, 
                'NUM_PARC': 9, 'VL_PARC_PASS': 10
            },
            'G130': {
                'REG': 1, 'IND_EMIT': 2, 'COD_PART': 3,
                'COD_MOD': 4, 'SERIE': 5, 'NUM_DOC': 6,
                'CHV_NFE_CTE': 7, 'DT_DOC': 8, 'NUM_DA': 9
            }


            
    }
       
class SpedEcdSchemas:
    """
    Mapeamento e definição dos schemas e mapas de campos dos registros do SPED ECD
    """
    SPEDS_ECD_SCHEMAS = {
        '0000': StructType([
            StructField('REG', StringType(), True),
            StructField('LECD', StringType(), True),
            StructField('DT_INI', StringType(), True),
            StructField('DT_FIN', StringType(), True),
            StructField('NOME', StringType(), True),
            StructField('CNPJ', StringType(), True),
            StructField('UF', StringType(), True),
            StructField('IE', StringType(), True),
            StructField('COD_MUN', StringType(), True),
            StructField('IM', StringType(), True),
            StructField('IND_SIT_ESP', StringType(), True),
            StructField('IND_SIT_INI_PER', StringType(), True),
            StructField('IND_NIRE', StringType(), True),
            StructField('IND_FIN_ESC', StringType(), True),
            StructField('COD_HASH_SUB', StringType(), True),
            StructField('IND_GRANDE_PORTE', StringType(), True),
            StructField('TIP_ECD', StringType(), True),
            StructField('COD_SCP', StringType(), True),
            StructField('IDENT_ME', StringType(), True),
            StructField('IND_ESC_CONS', StringType(), True),
            StructField('IND_CENTRALIZADA', StringType(), True),
            StructField('IND_MUDANC_PC', StringType(), True),
            StructField('COD_PLAN_REF', StringType(), True),
        ]),
        'I050': StructType([
            StructField('REG', StringType(), True),
            StructField('DT_ALT', StringType(), True),
            StructField('COD_NAT', StringType(), True),
            StructField('IND_CTA', StringType(), True),
            StructField('NIVEL', StringType(), True),
            StructField('COD_CTA', StringType(), True),
            StructField('COD_CTA_SUP', StringType(), True),
            StructField('CTA', StringType(), True),
            
        ]),
        'I051': StructType([
            StructField('REG', StringType(), True),
            StructField('COD_CCUS', StringType(), True),
            StructField('COD_CTA_REF', StringType(), True),

        ]),
        'I052': StructType([
            StructField('REG', StringType(), True),
            StructField('COD_CCUS', StringType(), True),
            StructField('COD_AGL', StringType(), True),

        ]),
        'I200': StructType([
            StructField('REG', StringType(), True),
            StructField('NUM_LCTO', StringType(), True),
            StructField('DT_LCTO', StringType(), True),
            StructField('VL_LCTO', StringType(), True),
            StructField('IND_LCTO', StringType(), True),
            StructField('DT_LCTO_EXT', StringType(), True),
            StructField('VL_LCTO_MF', StringType(), True),
            
        ]),
        'I250': StructType([
            StructField('REG', StringType(), True),
            StructField('COD_CTA', StringType(), True),
            StructField('COD_CCUS', StringType(), True),
            StructField('VL_DC', StringType(), True),
            StructField('IND_DC', StringType(), True),
            StructField('NUM_ARQ', StringType(), True),
            StructField('COD_HIST_PAD', StringType(), True),
            StructField('HIST', StringType(), True),
            StructField('COD_PART', StringType(), True),
        ]),
        'I350': StructType([
            StructField('REG', StringType(), True),
            StructField('DT_RES', StringType(), True),
                    
        ]),
        'I355': StructType([
            StructField('REG', StringType(), True),
            StructField('COD_CTA', StringType(), True),
            StructField('COD_CCUS', StringType(), True),
            StructField('VL_CTA', StringType(), True),
            StructField('IND_DC', StringType(), True),
            StructField('VL_CTA_MF', StringType(), True),
            StructField('IND_DC_MF', StringType(), True),
        ]),
        'J005': StructType([
            StructField('REG', StringType(), True),
            StructField('DT_INI', StringType(), True),
            StructField('DT_FIN', StringType(), True),
            StructField('ID_DEM', StringType(), True),
            StructField('CAB_DEM', StringType(), True),
        ]),
        'J100': StructType([
            StructField('REG', StringType(), True),
            StructField('COD_AGL', StringType(), True),
            StructField('IND_COD_AGL', StringType(), True),
            StructField('NIVEL_AGL', StringType(), True),
            StructField('COD_AGL_SUP', StringType(), True),
            StructField('IND_GRP_BAL', StringType(), True),
            StructField('DESCR_COD_AGL', StringType(), True),
            StructField('VL_CTA_INI', StringType(), True),
            StructField('IND_DC_CTA_INI', StringType(), True),
            StructField('VL_CTA_FIN', StringType(), True),
            StructField('IND_DC_CTA_FIN', StringType(), True),
            StructField('NOTA_EXP_REF', StringType(), True),

        ]),
        'J150': StructType([
            StructField('REG', StringType(), True),
            StructField('NUM_ORDEM', StringType(), True),
            StructField('COD_AGL', StringType(), True),
            StructField('IND_COD_AGL', StringType(), True),
            StructField('NIVEL_AGL', StringType(), True),
            StructField('COD_AGL_SUP', StringType(), True),
            StructField('DESCR_COD_AGL', StringType(), True),
            StructField('VL_CTA_INI', StringType(), True),
            StructField('IND_DC_CTA_INI', StringType(), True),
            StructField('VL_CTA_FIN', StringType(), True),
            StructField('IND_DC_CTA_FIN', StringType(), True),
            StructField('IND_GRP_DRE', StringType(), True),
            StructField('NOTA_EXP_REF', StringType(), True),
        ]),
        'J930': StructType([
            StructField('REG', StringType(), True),
            StructField('IDENT_NOM', StringType(), True),
            StructField('IDENT_CPF_CNPJ', StringType(), True),
            StructField('IDENT_QUALIF', StringType(), True),
            StructField('COD_ASSIN', StringType(), True),
            StructField('IND_CRC', StringType(), True),
            StructField('EMAIL', StringType(), True),
            StructField('FONE', StringType(), True),
            StructField('UF_CRC', StringType(), True),
            StructField('NUM_SEQ_CRC', StringType(), True),
            StructField('DT_CRC', StringType(), True),
            StructField('IND_RESP_LEGAL', StringType(), True),
        ])
        }   
    SPEDS_ECD_FIELDS_MAP = {
        '0000': {
        'REG': 1,         
        'LECD': 2,
        'DT_INI': 3,
        'DT_FIN': 4,
        'NOME': 5,
        'CNPJ': 6,
        'UF': 7,
        'IE': 8,
        'COD_MUN': 9,
        'IM': 10,
        'IND_SIT_ESP': 11,
        'IND_SIT_INI_PER': 12,
        'IND_NIRE': 13,
        'IND_FIN_ESC': 14,
        'COD_HASH_SUB': 15,
        'IND_GRANDE_PORTE': 16,
        'TIP_ECD': 17,
        'COD_SCP': 18,
        'IDENT_ME': 19,
        'IND_ESC_CONS': 20,
        'IND_CENTRALIZADA': 21,
        'IND_MUDANC_PC': 22,
        'COD_PLAN_REF': 23,
        },
        'I050': {
            'REG': 1,
            'DT_ALT': 2,
            'COD_NAT': 3,
            'IND_CTA': 4,
            'NIVEL': 5,
            'COD_CTA': 6,
            'COD_CTA_SUP': 7,
            'CTA': 8,
            
        },
        'I051': {
            'REG': 1,
            'COD_CCUS': 2,
            'COD_CTA_REF': 3,
        },
        'I052': {
            'REG': 1,
            'COD_CCUS': 2,
            'COD_AGL': 3,
        },
        'I200': {
            'REG': 1,
            'NUM_LCTO': 2,
            'DT_LCTO': 3,
            'VL_LCTO': 4,
            'IND_LCTO': 5,
            'DT_LCTO_EXT': 6,
            'VL_LCTO_MF': 7,
        },
        'I250': {
            'REG': 1,
            'COD_CTA': 2,
            'COD_CCUS': 3,
            'VL_DC': 4,
            'IND_DC': 5,
            'NUM_ARQ': 6,
            'COD_HIST_PAD': 7,
            'HIST': 8,
            'COD_PART': 9,
        },
        'I350': {
            'REG': 1,
            'DT_RES': 2,
        },
        'I355': {
            'REG': 1,
            'COD_CTA': 2,
            'COD_CCUS': 3,
            'VL_CTA': 4,
            'IND_DC': 5,
            'VL_CTA_MF': 6,
            'IND_DC_MF': 7,
        },
        'J005': {
            'REG': 1,
            'DT_INI': 2,
            'DT_FIN': 3,
            'ID_DEM': 4,
            'CAB_DEM': 5,
        },
        'J100': {
            'REG': 1,
            'COD_AGL': 2,
            'IND_COD_AGL': 3,
            'NIVEL_AGL': 4,
            'COD_AGL_SUP': 5,
            'IND_GRP_BAL': 6,
            'DESCR_COD_AGL': 7,
            'VL_CTA_INI': 8,
            'IND_DC_CTA_INI': 9,
            'VL_CTA_FIN': 10,
            'IND_DC_CTA_FIN': 11,
            'NOTA_EXP_REF': 12,
        },
        'J150': {
            'REG': 1,
            'NUM_ORDEM': 2,
            'COD_AGL': 3,
            'IND_COD_AGL': 4,
            'NIVEL_AGL': 5,
            'COD_AGL_SUP': 6,
            'DESCR_COD_AGL': 7,
            'VL_CTA_INI': 8,
            'IND_DC_CTA_INI': 9,
            'VL_CTA_FIN': 10,
            'IND_DC_CTA_FIN': 11,
            'IND_GRP_DRE': 12,
            'NOTA_EXP_REF': 13,
        },
        'J930': {
            'REG': 1,
            'IDENT_NOM': 2,
            'IDENT_CPF_CNPJ': 3,
            'IDENT_QUALIF': 4,
            'COD_ASSIN': 5,
            'IND_CRC': 6,
            'EMAIL': 7,
            'FONE': 8,
            'UF_CRC': 9,
            'NUM_SEQ_CRC': 10,
            'DT_CRC': 11,
            'IND_RESP_LEGAL': 12,
        }
        }

class SpedEcfSchemas:
    """
    Mapeamento e definição dos schemas e mapas de campos dos registros do SPED ECF
    """
    
    D19_2 = DecimalType(19, 2)

    PERIODO_SCHEMA = StructType([
        StructField("REG", StringType(), True),
        StructField("DT_INI", StringType(), True),
        StructField("DT_FIN", StringType(), True),
        StructField("PER_APUR", StringType(), True),
    ])

    DINAMICO_4_SCHEMA = StructType([
        StructField("REG", StringType(), True),
        StructField("CODIGO", StringType(), True),
        StructField("DESCRICAO", StringType(), True),
        StructField("VALOR", StringType(), True),
    ])

    REG_INI_LINE = StructType([
        StructField("REG", StringType(), True),
        StructField("IND_DAD", StringType(), True),
    ])

    REG_FIN_LINE = StructType([
        StructField("REG", StringType(), True),
        StructField("QTD_LIN", StringType(), True)
    ])

    SPEDS_ECF_SCHEMAS = {
        "0000": StructType([
            StructField("REG", StringType(), True),
            StructField("NOME_ESC", StringType(), True),
            StructField("COD_VER", StringType(), True),
            StructField("CNPJ", StringType(), True),
            StructField("NOME", StringType(), True),
            StructField("IND_SIT_INI_PER", StringType(), True),
            StructField("SIT_ESPECIAL", StringType(), True),
            StructField("PAT_REMAN_CIS", StringType(), True),
            StructField("DT_SIT_ESP", StringType(), True),
            StructField("DT_INI", StringType(), True),
            StructField("DT_FIN", StringType(), True),
            StructField("RETIFICADORA", StringType(), True),
            StructField("NUM_REC", StringType(), True),
            StructField("TIP_ECF", StringType(), True),
            StructField("COD_SCP", StringType(), True),
        ]),
        "0001": REG_INI_LINE,
        "0010": StructType([
            StructField("REG", StringType(), True),
            StructField("HASH_ECF_ANTERIOR", StringType(), True),
            StructField("OPT_REFIS", StringType(), True),
            StructField("FORMA_TRIB", StringType(), True),
            StructField("FORMA_APUR", StringType(), True),
            StructField("COD_QUALIF_PJ", StringType(), True),
            StructField("FORMA_TRIB_PER", StringType(), True),
            StructField("MES_BAL_RED", StringType(), True),
            StructField("TIP_ESC_PRE", StringType(), True),
            StructField("TIP_ENT", StringType(), True),
            StructField("FORMA_APUR_I", StringType(), True),
            StructField("APUR_CSLL", StringType(), True),
            StructField("IND_REC_RECEITA", StringType(), True),
        ]),
        "0020": StructType([
            StructField("REG", StringType(), True),
            StructField("IND_ALIQ_CSLL", StringType(), True),
            StructField("IND_QTE_SCP", StringType(), True),
            StructField("IND_ADM_FUN_CLU", StringType(), True),
            StructField("IND_PART_CONS", StringType(), True),
            StructField("IND_OP_EXT", StringType(), True),
            StructField("IND_OP_VINC", StringType(), True),
            StructField("IND_PJ_ENQUAD", StringType(), True),
            StructField("IND_PART_EXT", StringType(), True),
            StructField("IND_ATIV_RURAL", StringType(), True),
            StructField("IND_LUC_EXP", StringType(), True),
            StructField("IND_RED_ISEN", StringType(), True),
            StructField("IND_FIN", StringType(), True),
            StructField("IND_PART_COLIG", StringType(), True),
            StructField("IND_REC_EXT", StringType(), True),
            StructField("IND_ATIV_EXT", StringType(), True),
            StructField("IND_PGTO_EXT", StringType(), True),
            StructField("IND_E_COM_TI", StringType(), True),
            StructField("IND_ROY_REC", StringType(), True),
            StructField("IND_ROY_PAG", StringType(), True),
            StructField("IND_REND_SERV", StringType(), True),
            StructField("IND_PGTO_REM", StringType(), True),
            StructField("IND_INOV_TEC", StringType(), True),
            StructField("IND_CAP_INF", StringType(), True),
            StructField("IND_PJ_HAB", StringType(), True),
            StructField("IND_POLO_AM", StringType(), True),
            StructField("IND_ZON_EXP", StringType(), True),
            StructField("IND_AREA_COM", StringType(), True),
            StructField("IND_PAIS_A_PAIS", StringType(), True),
            StructField("IND_DEREX", StringType(), True),
            StructField("POSSUI_CEBAS", StringType(), True),
            StructField("CEBAS", StringType(), True),
        ]),
        "0021": StructType([
            StructField("REG", StringType(), True),
            StructField("IND_REPES", StringType(), True),
            StructField("IND_RECAP", StringType(), True),
            StructField("IND_PADIS", StringType(), True),
            StructField("IND_REIDI", StringType(), True),
            StructField("IND_RECINE", StringType(), True),
            StructField("IND_RETID", StringType(), True),
            StructField("IND_OLEO_BUNKER", StringType(), True),
            StructField("IND_REPORTO", StringType(), True),
            StructField("IND_RET_II", StringType(), True),
            StructField("IND_RET_PMCMV", StringType(), True),
            StructField("IND_RET_EEI", StringType(), True),
            StructField("IND_EBAS", StringType(), True),
            StructField("IND_REPETRO_INDUSTRIALIZACAO", StringType(), True),
            StructField("IND_REPETRO_NACIONAL", StringType(), True),
            StructField("IND_REPETRO_PERMANENTE", StringType(), True),
            StructField("IND_REPETRO_TEMPORARIO", StringType(), True)
        ]),  
        "0030": StructType([
            StructField("REG", StringType(), True),
            StructField("COD_NAT", StringType(), True),
            StructField("CNAE_FISCAL", StringType(), True),
            StructField("ENDERECO", StringType(), True),
            StructField("NUM", StringType(), True),
            StructField("COMPL", StringType(), True),
            StructField("BAIRRO", StringType(), True),
            StructField("UF", StringType(), True),
            StructField("COD_MUN", StringType(), True),
            StructField("CEP", StringType(), True),
            StructField("NUM_TEL", StringType(), True),
            StructField("EMAIL", StringType(), True),
        ]),
        "0035": StructType([
            StructField("REG", StringType(), True),
            StructField("COD_SCP", StringType(), True),
            StructField("NOME_SCP", StringType(), True),
            ]),
        "0930": StructType([
            StructField("REG", StringType(), True),
            StructField("IDENT_NOM", StringType(), True),
            StructField("IDENT_CPF_CNPJ", StringType(), True),
            StructField("IDENT_QUALIF", StringType(), True),
            StructField("IND_CRC", StringType(), True),
            StructField("EMAIL", StringType(), True),
            StructField("FONE", StringType(), True)
        ]),
        "0990": REG_FIN_LINE,
        "9001": REG_INI_LINE,
        "9100": StructType([
            StructField("REG", StringType(), True),
            StructField("NOM_REGRA", StringType(), True),
            StructField("MSG_REGRA", StringType(), True),
            StructField("REGISTRO", StringType(), True),
            StructField("CAMPO", StringType(), True),
            StructField("CONTEÃšDO", StringType(), True),
            StructField("VALOR_ESPERADO", StringType(), True),
            StructField("PER_APUR", StringType(), True),
            StructField("COD_CTA", StringType(), True),
            StructField("COD_CCUS", StringType(), True),
            StructField("COD_CTA_REF", StringType(), True),
            StructField("CODIGO", StringType(), True),
            StructField("NUM_ORDEM", StringType(), True),
            StructField("CNPJ_ESTAB", StringType(), True),
            StructField("CNAE", StringType(), True),
            StructField("COD_CTA_B", StringType(), True),
            StructField("COD_TRIBUTO", StringType(), True)
        ]),
        "9900": StructType([
            StructField("REG", StringType(), True),
            StructField("REG_BLC", StringType(), True),
            StructField("QTD_REG_BLC", StringType(), True),
            StructField("VERSAO", StringType(), True),
            StructField("ID_TAB_DIN", StringType(), True)
        ]),
        "9990": REG_FIN_LINE,
        "9999": REG_FIN_LINE,
        "C001": REG_INI_LINE,
        "C040": StructType([
            StructField("REG", StringType(), True),
            StructField("HASH_ECD", StringType(), True),
            StructField("DT_INI", StringType(), True),
            StructField("DT_FIN", StringType(), True),
            StructField("IND_SIT_ESP", StringType(), True),
            StructField("CNPJ", StringType(), True),
            StructField("NUM_ORD", StringType(), True),
            StructField("NIRE", StringType(), True),
            StructField("NAT_LIVR", StringType(), True),
            StructField("COD_VER_LC", StringType(), True),
            StructField("IND_ESC", StringType(), True),
            StructField("IDENT_MF", StringType(), True),
            StructField("IND_ESC_CONS", StringType(), True),
            StructField("IND_CENTRALIZADA", StringType(), True),
            StructField("IND_MUDANC_PC", StringType(), True),
            StructField("COD_PLAN_REF", StringType(), True)
        ]),
        "C050": StructType([
            StructField("REG", StringType(), True),
            StructField("DT_ALT", StringType(), True),
            StructField("COD_NAT", StringType(), True),
            StructField("IND_CTA", StringType(), True),
            StructField("NÃVEL", StringType(), True),
            StructField("COD_CTA", StringType(), True),
            StructField("COD_CTA_SUP", StringType(), True),
            StructField("CTA", StringType(), True)
        ]),
        "C051": StructType([
            StructField("REG", StringType(), True),
            StructField("COD_CCUS", StringType(), True),
            StructField("COD_CTA_REF", StringType(), True)
        ]),
        "C150": StructType([
            StructField("REG", StringType(), True),
            StructField("DT_INI", StringType(), True),
            StructField("DT_FIN", StringType(), True)
        ]),
        "C155": StructType([
            StructField("REG", StringType(), True),
            StructField("COD_CTA", StringType(), True),
            StructField("COD_CCUS", StringType(), True),
            StructField("VL_SLD_INI", StringType(), True),
            StructField("IND_VL_SLD_INI", StringType(), True),
            StructField("VL_DEB", StringType(), True),
            StructField("VL_CRED", StringType(), True),
            StructField("VL_SLD_FIN", StringType(), True),
            StructField("IND_VL_SLD_FIN", StringType(), True),
            StructField("LINHA_ECD", StringType(), True)
        ]),
        "C157": StructType([
            StructField("REG", StringType(), True),
            StructField("COD_CTA", StringType(), True),
            StructField("COD_CCUS", StringType(), True),
            StructField("VL_SLD_FIN", StringType(), True),
            StructField("IND_VL_SLD_FIN", StringType(), True),
            StructField("LINHA_ECD", StringType(), True)
        ]),
        "C350": StructType([
            StructField("REG", StringType(), True),
            StructField("DT_RES", StringType(), True)
        ]),
        "C355": StructType([
            StructField("REG", StringType(), True),
            StructField("COD_CTA", StringType(), True),
            StructField("COD_CCUS", StringType(), True),
            StructField("VL_CTA", StringType(), True),
            StructField("IND_VL_CTA", StringType(), True),
            StructField("LINHA_ECD", StringType(), True)
        ]),
        "C990": REG_FIN_LINE,
        "E001": REG_INI_LINE,
        "C053": StructType([
            StructField("REG", StringType(), True),
            StructField("COD_IDT", StringType(), True),
            StructField("COD_CNT_CORR", StringType(), True),
            StructField("NAT_SUB_CNT", StringType(), True)
        ]),
        "C100": StructType([
            StructField("REG", StringType(), True),
            StructField("DT_ALT", StringType(), True),
            StructField("COD_CCUS", StringType(), True),
            StructField("CCUS", StringType(), True)
        ]),
        "C990": REG_FIN_LINE,
        "E001": REG_INI_LINE,
        "E010": StructType([
            StructField("REG", StringType(), True), 
            StructField("COD_NAT", StringType(), True), 
            StructField("COD_CTA_REF", StringType(), True), 
            StructField("DESC_CTA_REF", StringType(), True), 
            StructField("VAL_CTA_REF", StringType(), True), 
            StructField("IND_VAL_CTA_REF", StringType(), True)
        ]),
        "E015": StructType([
            StructField("REG", StringType(), True), 
            StructField("COD_CTA", StringType(), True), 
            StructField("COD_CCUS", StringType(), True), 
            StructField("DESC_CTA", StringType(), True), 
            StructField("VAL_CTA", StringType(), True), 
            StructField("IND_VAL_CTA", StringType(), True)
        ]),
        "E020": StructType([
            StructField("REG", StringType(), True), 
            StructField("COD_CTA_B", StringType(), True), 
            StructField("DESC_CTA_LAL", StringType(), True), 
            StructField("DT_AP_LAL", StringType(), True), 
            StructField("DT_LIM_LAL", StringType(), True), 
            StructField("TRIBUTO", StringType(), True), 
            StructField("VL_SALDO_FIN", StringType(), True), 
            StructField("IND_VL_SALDO_FIN", StringType(), True), 
            StructField("COD_PB_RFB", StringType(), True)
        ]),
        "E030": PERIODO_SCHEMA,
        "E155": StructType([
            StructField("REG", StringType(), True), 
            StructField("COD_CTA", StringType(), True), 
            StructField("COD_CCUS", StringType(), True), 
            StructField("VL_SLD_INI", StringType(), True), 
            StructField("IND_VL_SLD_INI", StringType(), True), 
            StructField("VL_DEB", StringType(), True), 
            StructField("VL_CRED", StringType(), True), 
            StructField("VL_SLD_FIN", StringType(), True), 
            StructField("IND_VL_SLD_FIN", StringType(), True)
        ]),
        "E355": StructType([
            StructField("REG", StringType(), True), 
            StructField("COD_CTA", StringType(), True), 
            StructField("COD_CCUS", StringType(), True), 
            StructField("VL_SLD_FIN", StringType(), True), 
            StructField("IND_VL_SLD_FIN", StringType(), True)
        ]),
        "E990": REG_FIN_LINE,
        "J001": REG_INI_LINE,
        "J050": StructType([
            StructField("REG", StringType(), True),
            StructField("DT_ALT", StringType(), True),
            StructField("COD_NAT", StringType(), True),
            StructField("IND_CTA", StringType(), True),
            StructField("NIVEL", StringType(), True),
            StructField("COD_CTA", StringType(), True),
            StructField("COD_CTA_SUP", StringType(), True),
            StructField("CTA", StringType(), True),
        ]),
        "J051": StructType([
            StructField("REG", StringType(), True),
            StructField("COD_CCUS", StringType(), True),
            StructField("COD_CTA_REF", StringType(), True),
        ]),
        "J053": StructType([
            StructField("REG", StringType(), True), 
            StructField("COD_IDT", StringType(), True), 
            StructField("COD_CNT_CORR", StringType(), True), 
            StructField("NAT_SUB_CNT", StringType(), True)
        ]),
        "J100": StructType([
            StructField("REG", StringType(), True),
            StructField("DT_ALT", StringType(), True),
            StructField("COD_CCUS", StringType(), True),
            StructField("CCUS", StringType(), True),
        ]),
        "J990": REG_FIN_LINE,
        "K001": REG_INI_LINE,
        "K030": PERIODO_SCHEMA,
        "K155": StructType([
            StructField("REG", StringType(), True),
            StructField("COD_CTA", StringType(), True),
            StructField("COD_CCUS", StringType(), True),
            StructField("VL_SLD_INI", StringType(), True),
            StructField("IND_VL_SLD_INI", StringType(), True),
            StructField("VL_DEB", StringType(), True),
            StructField("VL_CRED", StringType(), True),
            StructField("VL_SLD_FIN", StringType(), True),
            StructField("IND_VL_SLD_FIN", StringType(), True),
        ]),
        "K156": StructType([
            StructField("REG", StringType(), True),
            StructField("COD_CTA_REF", StringType(), True),
            StructField("VL_SLD_INI", StringType(), True),
            StructField("IND_VL_SLD_INI", StringType(), True),
            StructField("VL_DEB", StringType(), True),
            StructField("VL_CRED", StringType(), True),
            StructField("VL_SLD_FIN", StringType(), True),
            StructField("IND_VL_SLD_FIN", StringType(), True),
        ]),
        "K355": StructType([
            StructField("REG", StringType(), True),
            StructField("COD_CTA", StringType(), True),
            StructField("COD_CCUS", StringType(), True),
            StructField("VL_SLD_FIN", StringType(), True),
            StructField("IND_VL_SLD_FIN", StringType(), True),
        ]),
        "K356": StructType([
            StructField("REG", StringType(), True),
            StructField("COD_CTA_REF", StringType(), True),
            StructField("VL_SLD_FIN", StringType(), True),
            StructField("IND_VL_SLD_FIN", StringType(), True),
        ]),
        "K915": StructType([
            StructField("REG", StringType(), True), 
            StructField("PER_APUR", StringType(), True), 
            StructField("COD_CTA", StringType(), True), 
            StructField("COD_CCUS", StringType(), True), 
            StructField("ID_REGRA", StringType(), True), 
            StructField("VL_SLD_INI_ESP", StringType(), True), 
            StructField("IND_VL_SLD_INI_ESP", StringType(), True), 
            StructField("VL_DEB_ESP", StringType(), True), 
            StructField("VL_CRED_ESP", StringType(), True), 
            StructField("VL_SLD_FIN_ESP", StringType(), True), 
            StructField("IND_VL_SLD_FIN_ESP", StringType(), True), 
            StructField("SLD_INI_PRE", StringType(), True), 
            StructField("IND_SLD_INI_PRE", StringType(), True), 
            StructField("VL_DEB_PRE", StringType(), True), 
            StructField("VL_CRED_PRE", StringType(), True), 
            StructField("SLD_FIN_PRE", StringType(), True), 
            StructField("IND_SLD_FIN_PRE", StringType(), True), 
            StructField("JUSTIFICATIVA", StringType(), True)
        ]),
        "K935": StructType([
            StructField("REG", StringType(), True), 
            StructField("PER_APUR", StringType(), True), 
            StructField("COD_CTA", StringType(), True), 
            StructField("COD_CCUS", StringType(), True), 
            StructField("ID_REGRA", StringType(), True), 
            StructField("VL_SLD_FIN_ESP", StringType(), True), 
            StructField("IND_VL_SLD_FIN_ESP", StringType(), True), 
            StructField("SLD_FIN_PRE", StringType(), True), 
            StructField("IND_SLD_FIN_PRE", StringType(), True), 
            StructField("JUSTIFICATIVA", StringType(), True)
        ]),
        "K990": REG_FIN_LINE,
        "L001": REG_INI_LINE,
        "L030": PERIODO_SCHEMA,
        "L100": StructType([
            StructField("REG", StringType(), True),
            StructField("CODIGO", StringType(), True),
            StructField("DESCRICAO", StringType(), True),
            StructField("TIPO", StringType(), True),
            StructField("NIVEL", StringType(), True),
            StructField("COD_NAT", StringType(), True),
            StructField("COD_CTA_SUP", StringType(), True),
            StructField("VAL_CTA_REF_INI", StringType(), True),
            StructField("IND_VAL_CTA_REF_INI", StringType(), True),
            StructField("VAL_CTA_REF_DEB", StringType(), True),
            StructField("VAL_CTA_REF_CRED", StringType(), True),
            StructField("VAL_CTA_REF_FIN", StringType(), True),
            StructField("IND_VAL_CTA_REF_FIN", StringType(), True),
        ]),
        "L200": StructType([
            StructField("REG", StringType(), True), 
            StructField("IND_AVAL_ESTOQ", StringType(), True)
        ]),
        "L210": DINAMICO_4_SCHEMA,
        "L300": StructType([
            StructField("REG", StringType(), True),
            StructField("CODIGO", StringType(), True),
            StructField("DESCRICAO", StringType(), True),
            StructField("TIPO", StringType(), True),
            StructField("NIVEL", StringType(), True),
            StructField("COD_NAT", StringType(), True),
            StructField("COD_CTA_SUP", StringType(), True),
            StructField("VALOR", StringType(), True),
            StructField("IND_VALOR", StringType(), True),
        ]),
        "L990": REG_FIN_LINE,
        "M001": REG_INI_LINE,
        "M010": StructType([
            StructField("REG", StringType(), True), 
            StructField("COD_CTA_B", StringType(), True), 
            StructField("DESC_CTA_LAL", StringType(), True), 
            StructField("DT_AP_LAL", StringType(), True), 
            StructField("COD_PB_RFB", StringType(), True), 
            StructField("DT_LIM_LAL", StringType(), True), 
            StructField("COD_TRIBUTO", StringType(), True), 
            StructField("VL_SALDO_INI", StringType(), True), 
            StructField("IND_VL_SALDO_INI", StringType(), True), 
            StructField("CNPJ_SIT_ESP", StringType(), True)
        ]),
        "M030": PERIODO_SCHEMA,
        "M300": StructType([
            StructField("REG", StringType(), True),
            StructField("CODIGO", StringType(), True),
            StructField("DESCRICAO", StringType(), True),
            StructField("TIPO_LANCAMENTO", StringType(), True),
            StructField("IND_RELACAO", StringType(), True),
            StructField("VALOR", StringType(), True),
            StructField("HIST_LAN_LAL", StringType(), True),
        ]),
        "M305": StructType([
            StructField("REG", StringType(), True),
            StructField("COD_CTA_B", StringType(), True), 
            StructField("VL_CTA", StringType(), True), 
            StructField("IND_VL_CTA", StringType(), True)
        ]),
        "M310": StructType([
            StructField("REG", StringType(), True),
            StructField("COD_CTA", StringType(), True),
            StructField("COD_CCUS", StringType(), True),
            StructField("VL_CTA", StringType(), True),
            StructField("IND_VL_CTA", StringType(), True),
        ]),
        "M312": StructType([
            StructField("REG", StringType(), True), 
            StructField("NUM_LCTO", StringType(), True)
        ]),
        "M315": StructType([
            StructField("REG", StringType(), True), 
            StructField("IND_PROC", StringType(), True), 
            StructField("NUM_PROC", StringType(), True)
        ]),
        "M350": StructType([
            StructField("REG", StringType(), True), 
            StructField("CODIGO", StringType(), True), 
            StructField("DESCRICAO", StringType(), True), 
            StructField("TIPO_LANCAMENTO", StringType(), True), 
            StructField("IND_RELACAO", StringType(), True), 
            StructField("VALOR", StringType(), True), 
            StructField("HIST_LAN_LAL", StringType(), True)
        ]),
        "M355": StructType([
            StructField("REG", StringType(), True), 
            StructField("COD_CTA_B", StringType(), True), 
            StructField("VL_CTA", StringType(), True), 
            StructField("IND_VL_CTA", StringType(), True)
        ]),
        "M360": StructType([
            StructField("REG", StringType(), True), 
            StructField("COD_CTA", StringType(), True), 
            StructField("COD_CCUS", StringType(), True), 
            StructField("VL_CTA", StringType(), True), 
            StructField("IND_VL_CTA", StringType(), True)
        ]),
        "M362": StructType([
            StructField("REG", StringType(), True), 
            StructField("NUM_LCTO", StringType(), True)
        ]),
        "M365": StructType([
            StructField("REG", StringType(), True), 
            StructField("IND_PROC", StringType(), True), 
            StructField("NUM_PROC", StringType(), True)
        ]),
        "M410": StructType([
            StructField("REG", StringType(), True), 
            StructField("COD_CTA_B", StringType(), True), 
            StructField("COD_TRIBUTO", StringType(), True), 
            StructField("VAL_LAN_LALB_PB", StringType(), True), 
            StructField("IND_VAL_LAN_LALB_PB", StringType(), True), 
            StructField("COD_CTA_B_CTP", StringType(), True), 
            StructField("HIST_LAN_LALB", StringType(), True), 
            StructField("IND_LAN_ANT", StringType(), True)
        ]),
        "M415": StructType([
            StructField("REG", StringType(), True), 
            StructField("IND_PROC", StringType(), True), 
            StructField("NUM_PROC", StringType(), True)
        ]),
        "M500": StructType([
            StructField("REG", StringType(), True), 
            StructField("COD_CTA_B", StringType(), True), 
            StructField("COD_TRIBUTO", StringType(), True), 
            StructField("SD_INI_LAL", StringType(), True), 
            StructField("IND_SD_INI_LAL", StringType(), True), 
            StructField("VL_LCTO_PARTE_A", StringType(), True), 
            StructField("IND_VL_LCTO_PARTE_A", StringType(), True), 
            StructField("VL_LCTO_PARTE_B", StringType(), True), 
            StructField("IND_VL_LCTO_PARTE_B", StringType(), True), 
            StructField("SD_FIM_LAL", StringType(), True), 
            StructField("IND_SD_FIM_LAL", StringType(), True)
        ]),
        "M510": StructType([
            StructField("REG", StringType(), True), 
            StructField("COD_PB_RFB", StringType(), True), 
            StructField("DESCRICAO_PB_RFB", StringType(), True), 
            StructField("COD_TRIBUTO", StringType(), True), 
            StructField("SD_INI_LAL", StringType(), True), 
            StructField("IND_SD_INI_LAL", StringType(), True), 
            StructField("VL_LCTO_PARTE_A", StringType(), True), 
            StructField("IND_VL_LCTO_PARTE_A", StringType(), True), 
            StructField("VL_LCTO_PARTE_B", StringType(), True), 
            StructField("IND_VL_LCTO_PARTE_B", StringType(), True), 
            StructField("SD_FIM_LAL", StringType(), True), 
            StructField("IND_SD_FIM_LAL", StringType(), True)
        ]),
        "M990": REG_FIN_LINE,
        "N001": REG_INI_LINE,
        "N030": PERIODO_SCHEMA,
        "N500": DINAMICO_4_SCHEMA,
        "N600": DINAMICO_4_SCHEMA,
        "N605": StructType([
            StructField("REG", StringType(), True),
            StructField("COD_CTA", StringType(), True),
            StructField("COD_CCUS", StringType(), True),
            StructField("VALOR", StringType(), True),
            StructField("IND_VALOR", StringType(), True)
        ]),
        "N610": DINAMICO_4_SCHEMA,
        "N620": DINAMICO_4_SCHEMA,
        "N630": DINAMICO_4_SCHEMA,
        "N650": DINAMICO_4_SCHEMA,
        "N660": DINAMICO_4_SCHEMA,
        "N670": DINAMICO_4_SCHEMA,
        "N990": REG_FIN_LINE,
        "P001": REG_INI_LINE,
        "P030": PERIODO_SCHEMA,
        "P100": StructType([
            StructField("REG", StringType(), True),
            StructField("CODIGO", StringType(), True), 
            StructField("DESCRICAO", StringType(), True), 
            StructField("TIPO", StringType(), True), 
            StructField("NIVEL", StringType(), True), 
            StructField("COD_NAT", StringType(), True), 
            StructField("COD_CTA_SUP", StringType(), True), 
            StructField("VAL_CTA_REF_INI", StringType(), True), 
            StructField("IND_VAL_CTA_REF_INI", StringType(), True), 
            StructField("VAL_CTA_REF_DEB", StringType(), True), 
            StructField("VAL_CTA_REF_CRED", StringType(), True), 
            StructField("VAL_CTA_REF_FIN", StringType(), True), 
            StructField("IND_VAL_CTA_REF_FIN", StringType(), True)
        ]),
        "P130": DINAMICO_4_SCHEMA,
        "P150": StructType([
            StructField("REG", StringType(), True),
            StructField("CODIGO", StringType(), True), 
            StructField("DESCRICAO", StringType(), True), 
            StructField("TIPO", StringType(), True), 
            StructField("NIVEL", StringType(), True), 
            StructField("COD_NAT", StringType(), True), 
            StructField("COD_CTA_SUP", StringType(), True), 
            StructField("VALOR", StringType(), True), 
            StructField("IND_VALOR", StringType(), True)
        ]),
        "P200": DINAMICO_4_SCHEMA,
        "P230": DINAMICO_4_SCHEMA,
        "P300": DINAMICO_4_SCHEMA,
        "P400": DINAMICO_4_SCHEMA,
        "P500": DINAMICO_4_SCHEMA,
        "P990": REG_FIN_LINE,
        "Q001": REG_INI_LINE,
        "Q100": StructType([
            StructField("REG", StringType(), True), 
            StructField("DATA", StringType(), True), 
            StructField("NUM_DOC", StringType(), True), 
            StructField("HIST", StringType(), True), 
            StructField("VL_ENTRADA", StringType(), True), 
            StructField("VL_SAIDA", StringType(), True), 
            StructField("SLD_FIN", StringType(), True)
        ]),
        "Q990": REG_FIN_LINE,
        "T001": REG_INI_LINE,
        "T030": PERIODO_SCHEMA,
        "T120": DINAMICO_4_SCHEMA,
        "T150": DINAMICO_4_SCHEMA,
        "T170": DINAMICO_4_SCHEMA,
        "T181": DINAMICO_4_SCHEMA,
        "T990": REG_FIN_LINE,
        "U001": REG_INI_LINE,
        "U030": PERIODO_SCHEMA,
        "U100": StructType([
            StructField("REG", StringType(), True), 
            StructField("CODIGO", StringType(), True), 
            StructField("DESCRICAO", StringType(), True), 
            StructField("TIPO", StringType(), True), 
            StructField("NIVEL", StringType(), True), 
            StructField("COD_NAT", StringType(), True), 
            StructField("COD_CTA_SUP", StringType(), True), 
            StructField("VAL_CTA_REF_INI", StringType(), True), 
            StructField("IND_VAL_CTA_REF_INI", StringType(), True), 
            StructField("VAL_CTA_REF_DEB", StringType(), True), 
            StructField("VAL_CTA_REF_CRED", StringType(), True), 
            StructField("VAL_CTA_REF_FIN", StringType(), True), 
            StructField("IND_VAL_CTA_REF_FIN", StringType(), True)
        ]),
        "U150": StructType([
            StructField("REG", StringType(), True), 
            StructField("CODIGO", StringType(), True), 
            StructField("DESCRICAO", StringType(), True), 
            StructField("TIPO", StringType(), True), 
            StructField("NIVEL", StringType(), True), 
            StructField("COD_NAT", StringType(), True), 
            StructField("COD_CTA_SUP", StringType(), True), 
            StructField("VALOR", StringType(), True), 
            StructField("IND_VALOR", StringType(), True)
        ]),
        "U180": DINAMICO_4_SCHEMA,
        "U182": DINAMICO_4_SCHEMA,
        "U990": REG_FIN_LINE,
        "V001": REG_INI_LINE,
        "V010": StructType([
            StructField("REG", StringType(), True), 
            StructField("NOME_INSTITUICAO", StringType(), True), 
            StructField("PAIS", StringType(), True), 
            StructField("TIP_MOEDA", StringType(), True)
        ]),
        "V020": StructType([
            StructField("REG", StringType(), True), 
            StructField("NOME", StringType(), True), 
            StructField("ENDERECO", StringType(), True), 
            StructField("TIPO_DO_C", StringType(), True), 
            StructField("NI", StringType(), True), 
            StructField("IDENT_CONTA", StringType(), True)
        ]),
        "V030": StructType([
            StructField("REG", StringType(), True), 
            StructField("MES", StringType(), True)
        ]),
        "V100": DINAMICO_4_SCHEMA,
        "V990": StructType([
            StructField("REG", StringType(), True), 
            StructField("QTD_LIN_M", StringType(), True)
        ]),
        "W001": REG_INI_LINE,
        "W100": StructType([
            StructField("REG", StringType(), True), 
            StructField("NOME_MULTINACIONAL", StringType(), True), 
            StructField("IND_CONTROLADORA", StringType(), True), 
            StructField("NOME_CONTROLADORA", StringType(), True), 
            StructField("JURISDICAO_CONTROLADORA", StringType(), True), 
            StructField("TIN_CONTROLADORA", StringType(), True), 
            StructField("IND_ENTREGA", StringType(), True), 
            StructField("IND_MODALIDADE", StringType(), True), 
            StructField("NOME_SUBSTITUTA", StringType(), True), 
            StructField("JURISDICAO_SUBSTITUTA", StringType(), True), 
            StructField("TIN_SUBSTITUTA", StringType(), True), 
            StructField("DT_INI", StringType(), True), 
            StructField("DT_FIN", StringType(), True), 
            StructField("TIP_MOEDA", StringType(), True), 
            StructField("IND_IDIOMA", StringType(), True)
        ]),
        "W200": StructType([
            StructField("REG", StringType(), True), 
            StructField("JURISDICAO", StringType(), True), 
            StructField("VL_REC_NAO_REL_EST", StringType(), True), 
            StructField("VL_REC_NAO_REL", StringType(), True), 
            StructField("VL_REC_REL_EST", StringType(), True), 
            StructField("VL_REC_REL", StringType(), True), 
            StructField("VL_REC_TOTAL_EST", StringType(), True), 
            StructField("VL_REC_TOTAL", StringType(), True), 
            StructField("VL_LUC_PREJ_ANTES_IR_EST", StringType(), True), 
            StructField("VL_LUC_PREJ_ANTES_IR", StringType(), True), 
            StructField("VL_IR_PAGO_EST", StringType(), True), 
            StructField("VL_IR_PAGO", StringType(), True), 
            StructField("VL_IR_DEVIDO_EST", StringType(), True), 
            StructField("VL_IR_DEVIDO", StringType(), True), 
            StructField("VL_CAP_SOC_EST", StringType(), True), 
            StructField("VL_CAP_SOC", StringType(), True), 
            StructField("VL_LUC_ACUM_EST", StringType(), True), 
            StructField("VL_LUC_ACUM", StringType(), True), 
            StructField("VL_ATIV_TANG_EST", StringType(), True), 
            StructField("VL_ATIV_TANG", StringType(), True), 
            StructField("NUM_EMP", StringType(), True)
        ]),
        "W250": StructType([
            StructField("REG", StringType(), True), 
            StructField("JUR_DIFERENTE", StringType(), True), 
            StructField("NOME", StringType(), True), 
            StructField("TIN", StringType(), True), 
            StructField("JURISDICAO_TIN", StringType(), True), 
            StructField("NI", StringType(), True), 
            StructField("JURISDICAO_NI", StringType(), True), 
            StructField("TIPO_NI", StringType(), True), 
            StructField("TIP_END", StringType(), True), 
            StructField("ENDEREÃ‡O", StringType(), True), 
            StructField("NUM_TEL", StringType(), True), 
            StructField("EMAIL", StringType(), True), 
            StructField("ATIV_1", StringType(), True), 
            StructField("ATIV_2", StringType(), True), 
            StructField("ATIV_3", StringType(), True), 
            StructField("ATIV_4", StringType(), True), 
            StructField("ATIV_5", StringType(), True), 
            StructField("ATIV_6", StringType(), True), 
            StructField("ATIV_7", StringType(), True), 
            StructField("ATIV_8", StringType(), True), 
            StructField("ATIV_9", StringType(), True), 
            StructField("ATIV_10", StringType(), True), 
            StructField("ATIV_11", StringType(), True), 
            StructField("ATIV_12", StringType(), True), 
            StructField("ATIV_13", StringType(), True), 
            StructField("DESC_OUTROS", StringType(), True), 
            StructField("OBSERVAÃ‡ÃƒO", StringType(), True)
        ]),
        "W300": StructType([
            StructField("REG", StringType(), True), 
            StructField("JURISDICAO", StringType(), True), 
            StructField("IND_REC_NAO_REL", StringType(), True), 
            StructField("IND_REC_REL", StringType(), True), 
            StructField("IND_REC_TOTAL", StringType(), True), 
            StructField("IND_LUC_PREJ_ANTES_IR", StringType(), True), 
            StructField("IND_IR_PAGO", StringType(), True), 
            StructField("IND_IR_DEVIDO", StringType(), True), 
            StructField("IND_CAP_SOC", StringType(), True), 
            StructField("IND_LUC_ACUM", StringType(), True), 
            StructField("IND_ATIV_TANG", StringType(), True), 
            StructField("IND_NUM_EMP", StringType(), True), 
            StructField("OBSERVAÃ‡ÃƒO", StringType(), True), 
            StructField("FIM_OBSERVACAO", StringType(), True)
        ]),
        "W990": REG_FIN_LINE,
        "X001": REG_INI_LINE,
        "X280": StructType([
            StructField("REG", StringType(), True), 
            StructField("IND_ATIV", StringType(), True), 
            StructField("IND_CONCEDENTE", StringType(), True), 
            StructField("IND_PROJ", StringType(), True), 
            StructField("ATO_CONC", StringType(), True), 
            StructField("VIG_INI", StringType(), True), 
            StructField("VIG_FIM", StringType(), True), 
            StructField("CNPJ_INCENTIVO", StringType(), True), 
            StructField("NCM_INCENTIVO", StringType(), True), 
            StructField("REC_LIQ_INCENTIVO", StringType(), True), 
            StructField("VL_INCENTIVO", StringType(), True)
        ]),
        "X292": DINAMICO_4_SCHEMA,
        "X340": StructType([
            StructField("REG", StringType(), True), 
            StructField("RAZ_SOCIAL", StringType(), True), 
            StructField("NIF", StringType(), True), 
            StructField("IND_CONTROLE", StringType(), True), 
            StructField("PAIS", StringType(), True), 
            StructField("IND_ISEN_PETR", StringType(), True), 
            StructField("IND_CONSOL", StringType(), True), 
            StructField("MOT_NAO_CONSOL", StringType(), True), 
            StructField("CNPJ", StringType(), True), 
            StructField("TIP_MOEDA", StringType(), True)
        ]),
        "X350": StructType([
            StructField("REG", StringType(), True), 
            StructField("REC_LIQ", StringType(), True), 
            StructField("CUSTOS", StringType(), True), 
            StructField("LUC_BRUTO", StringType(), True), 
            StructField("REC_AUFERIDAS", StringType(), True), 
            StructField("REC_OUTRAS_OPER", StringType(), True), 
            StructField("DESP_BRASIL", StringType(), True), 
            StructField("DESP_OPER", StringType(), True), 
            StructField("LUC_OPER", StringType(), True), 
            StructField("REC_PARTIC", StringType(), True), 
            StructField("REC_OUTRAS", StringType(), True), 
            StructField("DESP_OUTRAS", StringType(), True), 
            StructField("LUC_LIQ_ANT_IR", StringType(), True), 
            StructField("LUC_ARB_ANT_IR", StringType(), True), 
            StructField("IMP_DEV", StringType(), True), 
            StructField("LUC_LIQ", StringType(), True)
        ]),
        "X351": StructType([
            StructField("REG", StringType(), True), 
            StructField("RES_INV_PER", StringType(), True), 
            StructField("RES_INV_PER_REAL", StringType(), True), 
            StructField("RES_ISEN_PETR_PER", StringType(), True), 
            StructField("RES_ISEN_PETR_PER_REAL", StringType(), True), 
            StructField("RES_NEG_ACUM", StringType(), True), 
            StructField("RES_NEG_ACUM_REAL", StringType(), True), 
            StructField("RES_POS_TRIB", StringType(), True), 
            StructField("RES_POS_TRIB_REAL", StringType(), True), 
            StructField("IMP_LUCR", StringType(), True), 
            StructField("IMP_LUCR_REAL", StringType(), True), 
            StructField("IMP_PAG_REND", StringType(), True), 
            StructField("IMP_PAG_REND_REAL", StringType(), True), 
            StructField("IMP_RET_EXT", StringType(), True), 
            StructField("IMP_RET_EXT_REAL", StringType(), True), 
            StructField("IMP_RET_BR", StringType(), True)
        ]),
        "X352": StructType([
            StructField("REG", StringType(), True), 
            StructField("RES_PER", StringType(), True), 
            StructField("RES_PER_REAL", StringType(), True), 
            StructField("LUC_DISP", StringType(), True), 
            StructField("LUC_DISP_REAL", StringType(), True)
        ]),
        "X353": StructType([
            StructField("REG", StringType(), True), 
            StructField("RES_NEG_UTIL", StringType(), True), 
            StructField("RES_NEG_UTIL_REAL", StringType(), True), 
            StructField("SALDO_RES_NEG_NAO_UTIL", StringType(), True), 
            StructField("SALDO_RES_NEG_NAO_UTIL_REAL", StringType(), True), 
            StructField("RES_PROP", StringType(), True), 
            StructField("RES_PROP_REAL", StringType(), True)
        ]),
        "X354": StructType([
            StructField("REG", StringType(), True), 
            StructField("RES_NEG_ANT", StringType(), True), 
            StructField("RES_NEG_ANT_REAL", StringType(), True), 
            StructField("SALDO_NEG_ACUM", StringType(), True)
        ]),
        "X355": StructType([
            StructField("REG", StringType(), True), 
            StructField("REND_PASS_PROP", StringType(), True), 
            StructField("REND_PASS_PROP_REAL", StringType(), True), 
            StructField("REND_TOTAL", StringType(), True), 
            StructField("REND_TOTAL_REAL", StringType(), True), 
            StructField("REND_ATIV_PROP", StringType(), True), 
            StructField("REND_ATIV_PROP_REAL", StringType(), True), 
            StructField("PERCENTUAL", StringType(), True)
        ]),
        "X356": StructType([
            StructField("REG", StringType(), True), 
            StructField("PERC_PART", StringType(), True), 
            StructField("ATIVO_TOTAL", StringType(), True), 
            StructField("PAT_LIQUIDO", StringType(), True)
        ]),
        "X357": StructType([
            StructField("REG", StringType(), True), 
            StructField("PAIS", StringType(), True), 
            StructField("NIF/CNPJ", StringType(), True), 
            StructField("RAZAO_SOCIAL", StringType(), True), 
            StructField("PERCENTUAL", StringType(), True)
        ]),
        "X360": DINAMICO_4_SCHEMA,
        "X365": StructType([
            StructField("REG", StringType(), True), 
            StructField("IDENTIFICADOR", StringType(), True), 
            StructField("NOME_ENT", StringType(), True)
        ]),
        "X366": DINAMICO_4_SCHEMA,
        "X370": StructType([
            StructField("REG", StringType(), True), 
            StructField("IDENTIFICADOR", StringType(), True), 
            StructField("TIPO_TRANSACAO", StringType(), True), 
            StructField("NOME_ENT", StringType(), True), 
            StructField("PAIS", StringType(), True), 
            StructField("COD_NCM", StringType(), True), 
            StructField("TIPO_DEMAIS", StringType(), True), 
            StructField("DESCR_BSDI", StringType(), True), 
            StructField("VL_TRANSACAO", StringType(), True), 
            StructField("IND_AJUSTES", StringType(), True), 
            StructField("VL_ESPONTANEO", StringType(), True), 
            StructField("VL_COMPENSATORIO", StringType(), True), 
            StructField("TIP_AJ_COMPENSATORIO", StringType(), True), 
            StructField("METODO", StringType(), True), 
            StructField("DESCRICAO", StringType(), True), 
            StructField("COMP_INTENCIONAL", StringType(), True), 
            StructField("SINERGIA", StringType(), True), 
            StructField("IND_TRANS_COMBINADAS", StringType(), True), 
            StructField("IND_DADOS_MULTIP", StringType(), True), 
            StructField("IND_SIMPLIFIC", StringType(), True)
        ]),
        "X371": StructType([
            StructField("REG", StringType(), True), 
            StructField("COD_CTA", StringType(), True), 
            StructField("COD_CCUS", StringType(), True), 
            StructField("VALOR", StringType(), True), 
            StructField("IND_VALOR", StringType(), True)
        ]),
        "X375": DINAMICO_4_SCHEMA,
        "X390": DINAMICO_4_SCHEMA,
        "X400": DINAMICO_4_SCHEMA,
        "X410": StructType([
            StructField("REG", StringType(), True), 
            StructField("PAIS", StringType(), True), 
            StructField("IND_HOME_DISP", StringType(), True), 
            StructField("IND_SERV_DISP", StringType(), True)
        ]),
        "X420": StructType([
            StructField("REG", StringType(), True), 
            StructField("TIP_ROY", StringType(), True), 
            StructField("PAIS", StringType(), True), 
            StructField("VL_EXPL_DIR_SW", StringType(), True), 
            StructField("VL_EXPL_DIR_AUT", StringType(), True), 
            StructField("VL_EXPL_MARCA", StringType(), True), 
            StructField("VL_EXPL_PAT", StringType(), True), 
            StructField("VL_EXPL_KNOW", StringType(), True), 
            StructField("VL_EXPL_FRANQ", StringType(), True), 
            StructField("VL_EXPL_INT", StringType(), True)
        ]),
        "X430": StructType([
            StructField("REG", StringType(), True), 
            StructField("PAIS", StringType(), True), 
            StructField("VL_SERV_ASSIST", StringType(), True), 
            StructField("VL_SERV_SEM_ASSIST", StringType(), True), 
            StructField("VL_SERV_SEM_ASSIST_EXT", StringType(), True), 
            StructField("VL_JURO", StringType(), True), 
            StructField("VL_DEMAIS_JUROS", StringType(), True), 
            StructField("VL_DIVID", StringType(), True)
        ]),
        "X450": StructType([
            StructField("REG", StringType(), True), 
            StructField("PAIS", StringType(), True)
        ]),
        "X451": DINAMICO_4_SCHEMA,
        "X460": DINAMICO_4_SCHEMA,
        "X470": DINAMICO_4_SCHEMA,
        "X480": DINAMICO_4_SCHEMA,
        "X485": StructType([
            StructField("REG", StringType(), True), 
            StructField("TIPO_BENEF", StringType(), True), 
            StructField("ATO_DECL", StringType(), True), 
            StructField("CNPJ_INCORP", StringType(), True), 
            StructField("ID_OBRA_2018", StringType(), True), 
            StructField("ID_OBRA_2020", StringType(), True), 
            StructField("ID_OBRA_EEI", StringType(), True), 
            StructField("PORT_CEBAS", StringType(), True), 
            StructField("DT_DOU_PORT_CEBAS", StringType(), True), 
            StructField("DT_INI_PORT_CEBAS", StringType(), True), 
            StructField("DT_FIN_PORT_CEBAS", StringType(), True)
        ]),
        "X490": DINAMICO_4_SCHEMA,
        "X500": DINAMICO_4_SCHEMA,
        "X510": DINAMICO_4_SCHEMA,
        "X990": REG_FIN_LINE,
        "Y001": REG_INI_LINE,
        "Y520": StructType([
            StructField("REG", StringType(), True), 
            StructField("TIP_EXT", StringType(), True), 
            StructField("PAIS", StringType(), True), 
            StructField("FORMA", StringType(), True), 
            StructField("NAT_OPER", StringType(), True), 
            StructField("VL_PERIODO", StringType(), True)
        ]),
        "Y570": StructType([
            StructField("REG", StringType(), True),
            StructField("CNPJ_FON", StringType(), True),
            StructField("NOM_EMP", StringType(), True),
            StructField("IND_ORG_PUB", StringType(), True),
            StructField("COD_REC", StringType(), True),
            StructField("VL_REND", StringType(), True),
            StructField("IR_RET", StringType(), True),
            StructField("CSLL_RET", StringType(), True),
        ]),
        "Y590": StructType([
            StructField("REG", StringType(), True), 
            StructField("TIP_ATIVO", StringType(), True), 
            StructField("PAIS", StringType(), True), 
            StructField("DISCRIMINACAO", StringType(), True), 
            StructField("VL_ANT", StringType(), True), 
            StructField("VL_ATUAL", StringType(), True)
        ]),
        "Y600": StructType([
            StructField("REG", StringType(), True), 
            StructField("DT_ALT_SOC", StringType(), True), 
            StructField("DT_FIM_SOC", StringType(), True), 
            StructField("PAIS", StringType(), True), 
            StructField("IND_QUALIF", StringType(), True), 
            StructField("CPF_CNPJ", StringType(), True), 
            StructField("NOM_EMP", StringType(), True), 
            StructField("QUALIF", StringType(), True), 
            StructField("PERC_CAP_TOT", StringType(), True), 
            StructField("PERC_CAP_VOT", StringType(), True), 
            StructField("CPF_REP_LEG", StringType(), True), 
            StructField("QUALIF_REP_LEG", StringType(), True), 
            StructField("VL_REM_TRAB", StringType(), True), 
            StructField("VL_LUC_DIV", StringType(), True), 
            StructField("VL_JUR_CAP", StringType(), True), 
            StructField("VL_DEM_REND", StringType(), True), 
            StructField("VL_IR_RET", StringType(), True)
        ]),
        "Y612": StructType([
            StructField("REG", StringType(), True), 
            StructField("CPF", StringType(), True), 
            StructField("NOME", StringType(), True), 
            StructField("QUALIF", StringType(), True), 
            StructField("VL_REM_TRAB", StringType(), True), 
            StructField("VL_DEM_REND", StringType(), True), 
            StructField("VL_IR_RET", StringType(), True)
        ]),
        "Y620": StructType([
            StructField("REG", StringType(), True), 
            StructField("DT_EVENTO", StringType(), True), 
            StructField("IND_RELAC", StringType(), True), 
            StructField("PAIS", StringType(), True), 
            StructField("CNPJ", StringType(), True), 
            StructField("NOM_EMP", StringType(), True), 
            StructField("VALOR_REAIS", StringType(), True), 
            StructField("VALOR_ESTR", StringType(), True), 
            StructField("PERC_CAP_TOT", StringType(), True), 
            StructField("PERC_CAP_VOT", StringType(), True), 
            StructField("RES_EQ_PAT", StringType(), True), 
            StructField("DATA_AQUIS", StringType(), True), 
            StructField("IND_PROC_CART", StringType(), True), 
            StructField("NUM_PROC_CART", StringType(), True), 
            StructField("NOME_CART", StringType(), True), 
            StructField("IND_PROC_RFB", StringType(), True), 
            StructField("NUM_PROC_RFB", StringType(), True)
        ]),
        "Y630": StructType([
            StructField("REG", StringType(), True), 
            StructField("CNPJ", StringType(), True), 
            StructField("QTE_QUOT", StringType(), True), 
            StructField("QTE_QUOTA", StringType(), True), 
            StructField("PATR_FIN_PER", StringType(), True), 
            StructField("DAT_ABERT", StringType(), True), 
            StructField("DAT_ENCER", StringType(), True)
        ]),
        "Y640": StructType([
            StructField("REG", StringType(), True), 
            StructField("CNPJ", StringType(), True), 
            StructField("COND_DECL", StringType(), True), 
            StructField("VL_CONS", StringType(), True), 
            StructField("CNPJ_LID", StringType(), True), 
            StructField("VL_DECL", StringType(), True)
        ]),
        "Y650": StructType([
            StructField("REG", StringType(), True), 
            StructField("CNPJ", StringType(), True), 
            StructField("VL_PART", StringType(), True)
        ]),
        "Y660": StructType([
            StructField("REG", StringType(), True), 
            StructField("CNPJ", StringType(), True), 
            StructField("NOM_EMP", StringType(), True), 
            StructField("PERC_PAT_LIQ", StringType(), True)
        ]),
        "Y672": StructType([
            StructField("REG", StringType(), True), 
            StructField("VL_CAPITAL_ANT", StringType(), True), 
            StructField("VL_CAPITAL", StringType(), True), 
            StructField("VL_ESTOQUE_ANT", StringType(), True), 
            StructField("VL_ESTOQUES", StringType(), True), 
            StructField("VL_CAIXA_ANT", StringType(), True), 
            StructField("VL_CAIXA", StringType(), True), 
            StructField("VL_APLIC_FIN_ANT", StringType(), True), 
            StructField("VL_APLIC_FIN", StringType(), True), 
            StructField("VL_CTA_REC_ANT", StringType(), True), 
            StructField("VL_CTA_REC", StringType(), True), 
            StructField("VL_CTA_PAG_ANT", StringType(), True), 
            StructField("VL_CTA_PAG", StringType(), True), 
            StructField("VL_COMPRA_MERC", StringType(), True), 
            StructField("VL_COMPRA_ATIVO", StringType(), True), 
            StructField("VL_RECEITAS", StringType(), True), 
            StructField("TOT_ATIVO", StringType(), True), 
            StructField("IND_AVAL_ESTOQ", StringType(), True)
        ]),
        "Y680": StructType([
            StructField("REG", StringType(), True), 
            StructField("MES", StringType(), True)
        ]),
        "Y681": DINAMICO_4_SCHEMA,
        "Y682": StructType([
            StructField("REG", StringType(), True), 
            StructField("MES", StringType(), True), 
            StructField("ACRES_PATR", StringType(), True)
        ]),
        "Y720": StructType([
            StructField("REG", StringType(), True), 
            StructField("LUC_LIQ", StringType(), True), 
            StructField("DT_LUC_LIQ", StringType(), True), 
            StructField("REC_BRUT_ANT", StringType(), True), 
            StructField("INTIMACAO", StringType(), True), 
            StructField("INT_ATRASO", StringType(), True)
        ]),
        "Y730": StructType([
            StructField("REG", StringType(), True), 
            StructField("DEDUCAO", StringType(), True), 
            StructField("TIPO", StringType(), True), 
            StructField("DATA", StringType(), True), 
            StructField("TIPO_DESTINATARIO", StringType(), True), 
            StructField("DESTINATARIO", StringType(), True), 
            StructField("VALOR", StringType(), True), 
            StructField("OBSERVACAO", StringType(), True)
        ]),
        "Y750": DINAMICO_4_SCHEMA,
        "Y800": StructType([
            StructField("REG", StringType(), True), 
            StructField("TIPO_DOC", StringType(), True), 
            StructField("DESCRICAO", StringType(), True), 
            StructField("HASH", StringType(), True), 
            StructField("ARQ_RTF", StringType(), True), 
            StructField("IND_FIM_RTF", StringType(), True)
        ]),
        "Y990": REG_FIN_LINE,
    }
    SPEDS_ECF_FIELDS_MAP = {
        "0000": {
            "REG": 1,
            "NOME_ESC": 2,
            "COD_VER": 3,
            "CNPJ": 4,
            "NOME": 5,
            "IND_SIT_INI_PER": 6,
            "SIT_ESPECIAL": 7,
            "PAT_REMAN_CIS": 8,
            "DT_SIT_ESP": 9,
            "DT_INI": 10,
            "DT_FIN": 11,
            "RETIFICADORA": 12,
            "NUM_REC": 13,
            "TIP_ECF": 14,
            "COD_SCP": 15,
        },
        "0001": {
            "REG": 1,
            "IND_DAD": 2
        },
        "0010": {
            "REG": 1,
            "HASH_ECF_ANTERIOR": 2,
            "OPT_REFIS": 3,
            "FORMA_TRIB": 4,
            "FORMA_APUR": 5,
            "COD_QUALIF_PJ": 6,
            "FORMA_TRIB_PER": 7,
            "MES_BAL_RED": 8,
            "TIP_ESC_PRE": 9,
            "TIP_ENT": 10,
            "FORMA_APUR_I": 11,
            "APUR_CSLL": 12,
            "IND_REC_RECEITA": 13,
        },
        "0020": {
            "REG": 1,
            "IND_ALIQ_CSLL": 2,
            "IND_QTE_SCP": 3,
            "IND_ADM_FUN_CLU": 4,
            "IND_PART_CONS": 5,
            "IND_OP_EXT": 6,
            "IND_OP_VINC": 7,
            "IND_PJ_ENQUAD": 8,
            "IND_PART_EXT": 9,
            "IND_ATIV_RURAL": 10,
            "IND_LUC_EXP": 11,
            "IND_RED_ISEN": 12,
            "IND_FIN": 13,
            "IND_PART_COLIG": 14,
            "IND_REC_EXT": 15,
            "IND_ATIV_EXT": 16,
            "IND_PGTO_EXT": 17,
            "IND_E_COM_TI": 18,
            "IND_ROY_REC": 19,
            "IND_ROY_PAG": 20,
            "IND_REND_SERV": 21,
            "IND_PGTO_REM": 22,
            "IND_INOV_TEC": 23,
            "IND_CAP_INF": 24,
            "IND_PJ_HAB": 25,
            "IND_POLO_AM": 26,
            "IND_ZON_EXP": 27,
            "IND_AREA_COM": 28,
            "IND_PAIS_A_PAIS": 29,
            "IND_DEREX": 30,
            "POSSUI_CEBAS": 31,
            "CEBAS": 32,
        },
        "0021": {
            "REG": 1,
            "IND_REPES": 2,
            "IND_RECAP": 3,
            "IND_PADIS": 4,
            "IND_REIDI": 5,
            "IND_RECINE": 6,
            "IND_RETID": 7,
            "IND_OLEO_BUNKER": 8,
            "IND_REPORTO": 9,
            "IND_RET_II": 10,
            "IND_RET_PMCMV": 11,
            "IND_RET_EEI": 12,
            "IND_EBAS": 13,
            "IND_REPETRO_INDUSTRIALIZACAO": 14,
            "IND_REPETRO_NACIONAL": 15,
            "IND_REPETRO_PERMANENTE": 16,
            "IND_REPETRO_TEMPORARIO": 17
        },  
        "0030": {
            "REG": 1,
            "COD_NAT": 2,
            "CNAE_FISCAL": 3,
            "ENDERECO": 4,
            "NUM": 5,
            "COMPL": 6,
            "BAIRRO": 7,
            "UF": 8,
            "COD_MUN": 9,
            "CEP": 10,
            "NUM_TEL": 11,
            "EMAIL": 12,
        },
        "0035": {
            "REG": 1,
            "COD_SCP": 2,
            "NOME_SCP": 3
            },
        "0930": {
            "REG": 1,
            "IDENT_NOM": 2,
            "IDENT_CPF_CNPJ": 3,
            "IDENT_QUALIF": 4,
            "IND_CRC": 5,
            "EMAIL": 6,
            "FONE": 7
        },
        "0990": {
            "REG": 1,
            "QTD_LIN": 2
        },
        "C001": {
            "REG": 1,
            "IND_DAD": 2
        },
        "C040": {
            "REG": 1,
            "HASH_ECD": 2,
            "DT_INI": 3,
            "DT_FIN": 4,
            "IND_SIT_ESP": 5,
            "CNPJ": 6,
            "NUM_ORD": 7,
            "NIRE": 8,
            "NAT_LIVR": 9,
            "COD_VER_LC": 10,
            "IND_ESC": 11,
            "IDENT_MF": 12,
            "IND_ESC_CONS": 13,
            "IND_CENTRALIZADA": 14,
            "IND_MUDANC_PC": 15,
            "COD_PLAN_REF": 16
        },
        "C050": {
            "REG": 1,
            "DT_ALT": 2,
            "COD_NAT": 3,
            "IND_CTA": 4,
            "NÃVEL": 5,
            "COD_CTA": 6,
            "COD_CTA_SUP": 7,
            "CTA": 8
        },
        "C051": {
            "REG": 1,
            "COD_CCUS": 2,
            "COD_CTA_REF": 3
        },
        "C053": {
            "REG": 1,
            "COD_IDT": 2,
            "COD_CNT_CORR": 3,
            "NAT_SUB_CNT": 4
        },
        "C100": {
            "REG": 1,
            "DT_ALT": 2,
            "COD_CCUS": 3,
            "CCUS": 4
        },
        "C150": {
            "REG": 1,
            "DT_INI": 2,
            "DT_FIN": 3
        },
        "C155": {
            "REG": 1, 
            "COD_CTA": 2, 
            "COD_CCUS": 3, 
            "VL_SLD_INI": 4, 
            "IND_VL_SLD_INI": 5, 
            "VL_DEB": 6, 
            "VL_CRED": 7, 
            "VL_SLD_FIN": 8, 
            "IND_VL_SLD_FIN": 9, 
            "LINHA_ECD": 10
        },
        "C157": {
            "REG": 1, 
            "COD_CTA": 2, 
            "COD_CCUS": 3, 
            "VL_SLD_FIN": 4, 
            "IND_VL_SLD_FIN": 5, 
            "LINHA_ECD": 6
        },
        "C350": {
            "REG": 1, 
            "DT_RES": 2
        },
        "C355": {
            "REG": 1, 
            "COD_CTA": 2, 
            "COD_CCUS": 3, 
            "VL_CTA": 4, 
            "IND_VL_CTA": 5, 
            "LINHA_ECD": 6
        },
        "C990": {
            "REG": 1, 
            "QTD_LIN": 2
        },
        "E001": {
            "REG": 1, 
            "IND_DAD": 2
        },
        "E010": {
            "REG": 1, 
            "COD_NAT": 2, 
            "COD_CTA_REF": 3, 
            "DESC_CTA_REF": 4, 
            "VAL_CTA_REF": 5, 
            "IND_VAL_CTA_REF": 6
        },
        "E015": {
            "REG": 1, 
            "COD_CTA": 2, 
            "COD_CCUS": 3, 
            "DESC_CTA": 4, 
            "VAL_CTA": 5, 
            "IND_VAL_CTA": 6
        },
        "E020": {
            "REG": 1, 
            "COD_CTA_B": 2, 
            "DESC_CTA_LAL": 3, 
            "DT_AP_LAL": 4, 
            "DT_LIM_LAL": 5, 
            "TRIBUTO": 6, 
            "VL_SALDO_FIN": 7, 
            "IND_VL_SALDO_FIN": 8, 
            "COD_PB_RFB": 9
        },
        "E030": {
            "REG": 1, 
            "DT_INI": 2, 
            "DT_FIN": 3, 
            "PER_APUR": 4
        },
        "E155": {
            "REG": 1, 
            "COD_CTA": 2, 
            "COD_CCUS": 3, 
            "VL_SLD_INI": 4, 
            "IND_VL_SLD_INI": 5, 
            "VL_DEB": 6, 
            "VL_CRED": 7, 
            "VL_SLD_FIN": 8, 
            "IND_VL_SLD_FIN": 9
        },
        "E355": {
            "REG": 1, 
            "COD_CTA": 2, 
            "COD_CCUS": 3, 
            "VL_SLD_FIN": 4, 
            "IND_VL_SLD_FIN": 5
        },
        "E990": {
            "REG": 1, 
            "QTD_LIN": 2
        },
        "J001": {
            "REG": 1, 
            "IND_DAD": 2
        },
        "J050": {
            "REG": 1,
            "DT_ALT": 2,
            "COD_NAT": 3,
            "IND_CTA": 4,
            "NIVEL": 5,
            "COD_CTA": 6,
            "COD_CTA_SUP": 7,
            "CTA": 8,
        },
        "J051": {
            "REG": 1,
            "COD_CCUS": 2,
            "COD_CTA_REF": 3,
        },
        "J053": {
            "REG": 1, 
            "COD_IDT": 2, 
            "COD_CNT_CORR": 3, 
            "NAT_SUB_CNT": 4
        },
        "J100": {
            "REG": 1,
            "DT_ALT": 2,
            "COD_CCUS": 3,
            "CCUS": 4,
        },
        "J990": {
            "REG": 1, 
            "QTD_LIN": 2
        },
        "K001": {
            "REG": 1, 
            "IND_DAD": 2
        },
        "K030": {
            "REG": 1,
            "DT_INI": 2,
            "DT_FIN": 3,
            "PER_APUR": 4,
        },
        "K155": {
            "REG": 1,
            "COD_CTA": 2,
            "COD_CCUS": 3,
            "VL_SLD_INI": 4,
            "IND_VL_SLD_INI": 5,
            "VL_DEB": 6,
            "VL_CRED": 7,
            "VL_SLD_FIN": 8,
            "IND_VL_SLD_FIN": 9,
        },
        "K156": {
            "REG": 1,
            "COD_CTA_REF": 2,
            "VL_SLD_INI": 3,
            "IND_VL_SLD_INI": 4,
            "VL_DEB": 5,
            "VL_CRED": 6,
            "VL_SLD_FIN": 7,
            "IND_VL_SLD_FIN": 8,
        },
        "K355": {
            "REG": 1,
            "COD_CTA": 2,
            "COD_CCUS": 3,
            "VL_SLD_FIN": 4,
            "IND_VL_SLD_FIN": 5,
        },
        "K356": {
            "REG": 1,
            "COD_CTA_REF": 2,
            "VL_SLD_FIN": 3,
            "IND_VL_SLD_FIN": 4,
        },
        "K915": {
            "REG": 1, 
            "PER_APUR": 2, 
            "COD_CTA": 3, 
            "COD_CCUS": 4, 
            "ID_REGRA": 5, 
            "VL_SLD_INI_ESP": 6, 
            "IND_VL_SLD_INI_ESP": 7, 
            "VL_DEB_ESP": 8, 
            "VL_CRED_ESP": 9, 
            "VL_SLD_FIN_ESP": 10, 
            "IND_VL_SLD_FIN_ESP": 11, 
            "SLD_INI_PRE": 12, 
            "IND_SLD_INI_PRE": 13, 
            "VL_DEB_PRE": 14, 
            "VL_CRED_PRE": 15, 
            "SLD_FIN_PRE": 16, 
            "IND_SLD_FIN_PRE": 17, 
            "JUSTIFICATIVA": 18
        },
        "K935": {
            "REG": 1, 
            "PER_APUR": 2, 
            "COD_CTA": 3, 
            "COD_CCUS": 4, 
            "ID_REGRA": 5, 
            "VL_SLD_FIN_ESP": 6, 
            "IND_VL_SLD_FIN_ESP": 7, 
            "SLD_FIN_PRE": 8, 
            "IND_SLD_FIN_PRE": 9, 
            "JUSTIFICATIVA": 10
        },
        "K990": {
            "REG": 1, 
            "QTD_LIN": 2
        },
        "L001": {
            "REG": 1, 
            "IND_DAD": 2
        },
        "L030": {
            "REG": 1,
            "DT_INI": 2,
            "DT_FIN": 3,
            "PER_APUR": 4,
        },
        "L100":{
            "REG": 1,
            "CODIGO": 2,
            "DESCRICAO": 3,
            "TIPO": 4,
            "NIVEL": 5,
            "COD_NAT": 6,
            "COD_CTA_SUP": 7,
            "VAL_CTA_REF_INI": 8,
            "IND_VAL_CTA_REF_INI": 9,
            "VAL_CTA_REF_DEB": 10,
            "VAL_CTA_REF_CRED": 11,
            "VAL_CTA_REF_FIN": 12,
            "IND_VAL_CTA_REF_FIN": 13,
        },
        "L200": {
            "REG": 1, 
            "IND_AVAL_ESTOQ": 2
        },
        "L210": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "L300":{
            "REG": 1,
            "CODIGO": 2,
            "DESCRICAO": 3,
            "TIPO": 4,
            "NIVEL": 5,
            "COD_NAT": 6,
            "COD_CTA_SUP": 7,
            "VALOR": 8,
            "IND_VALOR": 9,
        },
        "L990": {
            "REG": 1, 
            "QTD_LIN": 2
        },
        "M001": {
            "REG": 1, 
            "IND_DAD": 2
        },
        "M010": {
            "REG": 1, 
            "COD_CTA_B": 2, 
            "DESC_CTA_LAL": 3, 
            "DT_AP_LAL": 4, 
            "COD_PB_RFB": 5, 
            "DT_LIM_LAL": 6, 
            "COD_TRIBUTO": 7, 
            "VL_SALDO_INI": 8, 
            "IND_VL_SALDO_INI": 9, 
            "CNPJ_SIT_ESP": 10
        },
        "M030":{
            "REG": 1,
            "DT_INI": 2,
            "DT_FIN": 3,
            "PER_APUR": 4,
        },
        "M300":{
            "REG": 1,
            "CODIGO": 2,
            "DESCRICAO": 3,
            "TIPO_LANCAMENTO": 4,
            "IND_RELACAO": 5,
            "VALOR": 6,
            "HIST_LAN_LAL": 7,
        },
        "M305": {
            "REG": 1,
            "COD_CTA_B": 2, 
            "VL_CTA": 3, 
            "IND_VL_CTA": 4
        },
        "M310":{
            "REG": 1,
            "COD_CTA": 2,
            "COD_CCUS": 3,
            "VL_CTA": 4,
            "IND_VL_CTA": 5,
        },
        "M312": {
            "REG": 1, 
            "NUM_LCTO": 2
        },
        "M315": {
            "REG": 1, 
            "IND_PROC": 2, 
            "NUM_PROC": 3
        },
        "M350": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "TIPO_LANCAMENTO": 4, 
            "IND_RELACAO": 5, 
            "VALOR": 6, 
            "HIST_LAN_LAL": 7
        },
        "M355": {
            "REG": 1, 
            "COD_CTA_B": 2, 
            "VL_CTA": 3, 
            "IND_VL_CTA": 4
        },
        "M360": {
            "REG": 1, 
            "COD_CTA": 2, 
            "COD_CCUS": 3, 
            "VL_CTA": 4, 
            "IND_VL_CTA": 5
        },
        "M362": {
            "REG": 1, 
            "NUM_LCTO": 2
        },
        "M365": {
            "REG": 1, 
            "IND_PROC": 2, 
            "NUM_PROC": 3
        },
        "M410": {
            "REG": 1, 
            "COD_CTA_B": 2, 
            "COD_TRIBUTO": 3, 
            "VAL_LAN_LALB_PB": 4, 
            "IND_VAL_LAN_LALB_PB": 5, 
            "COD_CTA_B_CTP": 6, 
            "HIST_LAN_LALB": 7, 
            "IND_LAN_ANT": 8
        },
        "M415": {
            "REG": 1, 
            "IND_PROC": 2, 
            "NUM_PROC": 3
        },
        "M500": {
            "REG": 1, 
            "COD_CTA_B": 2, 
            "COD_TRIBUTO": 3, 
            "SD_INI_LAL": 4, 
            "IND_SD_INI_LAL": 5, 
            "VL_LCTO_PARTE_A": 6, 
            "IND_VL_LCTO_PARTE_A": 7, 
            "VL_LCTO_PARTE_B": 8, 
            "IND_VL_LCTO_PARTE_B": 9, 
            "SD_FIM_LAL": 10, 
            "IND_SD_FIM_LAL": 11
        },
        "M510": {
            "REG": 1, 
            "COD_PB_RFB": 2, 
            "DESCRICAO_PB_RFB": 3, 
            "COD_TRIBUTO": 4, 
            "SD_INI_LAL": 5, 
            "IND_SD_INI_LAL": 6, 
            "VL_LCTO_PARTE_A": 7, 
            "IND_VL_LCTO_PARTE_A": 8, 
            "VL_LCTO_PARTE_B": 9, 
            "IND_VL_LCTO_PARTE_B": 10, 
            "SD_FIM_LAL": 11, 
            "IND_SD_FIM_LAL": 12
        },
        "M990": {
            "REG": 1, 
            "QTD_LIN": 2
        },
        "N001": {
            "REG": 1, 
            "IND_DAD": 2
        },
        "N030":{
            "REG": 1,
            "DT_INI": 2,
            "DT_FIN": 3,
            "PER_APUR": 4,
        },
        "N500":{
            "REG": 1,
            "CODIGO": 2,
            "DESCRICAO": 3,
            "VALOR": 4
        },
        "N600": {
            "REG": 1,
            "CODIGO": 2,
            "DESCRICAO": 3,
            "VALOR": 4
        },
        "N605": {
            "REG": 1,
            "COD_CTA": 2,
            "COD_CCUS": 3,
            "VALOR": 4,
            "IND_VALOR": 5
        },
        "N610": {
            "REG": 1,
            "CODIGO": 2,
            "DESCRICAO": 3,
            "VALOR": 4
        },
        "N620": {
            "REG": 1,
            "CODIGO": 2,
            "DESCRICAO": 3,
            "VALOR": 4
        },
        "N630":{
            "REG": 1,
            "CODIGO": 2,
            "DESCRICAO": 3,
            "VALOR": 4
        },
        "N650": {
            "REG": 1,
            "CODIGO": 2,
            "DESCRICAO": 3,
            "VALOR": 4
        },
        "N660": {
            "REG": 1,
            "CODIGO": 2,
            "DESCRICAO": 3,
            "VALOR": 4
        },
        "N670":{
            "REG": 1,
            "CODIGO": 2,
            "DESCRICAO": 3,
            "VALOR": 4
        },
        "N990": {
            "REG": 1, 
            "QTD_LIN": 2
        },
        "P001": {
            "REG": 1, 
            "IND_DAD": 2
        },
        "P030": {
            "REG": 1, "DT_INI": 2, "DT_FIN": 3, "PER_APUR": 4
        },
        "P100": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "TIPO": 4, 
            "NIVEL": 5, 
            "COD_NAT": 6, 
            "COD_CTA_SUP": 7, 
            "VAL_CTA_REF_INI": 8, 
            "IND_VAL_CTA_REF_INI": 9, 
            "VAL_CTA_REF_DEB": 10, 
            "VAL_CTA_REF_CRED": 11, 
            "VAL_CTA_REF_FIN": 12, 
            "IND_VAL_CTA_REF_FIN": 13
        },
        "P130": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "P150": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "TIPO": 4, 
            "NIVEL": 5, 
            "COD_NAT": 6, 
            "COD_CTA_SUP": 7, 
            "VALOR": 8, 
            "IND_VALOR": 9
        },
        "P200": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "P230": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "P300": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "P400": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "P500": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "P990": {
            "REG": 1, 
            "QTD_LIN": 2
        },
        "Q001": {
            "REG": 1, 
            "IND_DAD": 2
        },
        "Q100": {
            "REG": 1, 
            "DATA": 2, 
            "NUM_DOC": 3, 
            "HIST": 4, 
            "VL_ENTRADA": 5, 
            "VL_SAIDA": 6, 
            "SLD_FIN": 7
        },
        "Q990": {
            "REG": 1, 
            "QTD_LIN": 2
        },
        "T001": {
            "REG": 1, 
            "IND_DAD": 2
        },
        "T030": {
            "REG": 1, 
            "DT_INI": 2, 
            "DT_FIN": 3, 
            "PER_APUR": 4
        },
        "T120": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "T150": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "T170": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "T181": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "T990": {
            "REG": 1, 
            "QTD_LIN": 2
        },
        "U001": {
            "REG": 1, 
            "IND_DAD": 2
        },
        "U030": {
            "REG": 1, 
            "DT_INI": 2, 
            "DT_FIN": 3, 
            "PER_APUR": 4
        },
        "U100": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "TIPO": 4, 
            "NIVEL": 5, 
            "COD_NAT": 6, 
            "COD_CTA_SUP": 7, 
            "VAL_CTA_REF_INI": 8, 
            "IND_VAL_CTA_REF_INI": 9, 
            "VAL_CTA_REF_DEB": 10, 
            "VAL_CTA_REF_CRED": 11, 
            "VAL_CTA_REF_FIN": 12, 
            "IND_VAL_CTA_REF_FIN": 13
        },
        "U150": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "TIPO": 4, 
            "NIVEL": 5, 
            "COD_NAT": 6, 
            "COD_CTA_SUP": 7, 
            "VALOR": 8, 
            "IND_VALOR": 9
        },
        "U180": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "U182": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "U990": {
            "REG": 1, 
            "QTD_LIN": 2
        },
        "V001": {
            "REG": 1, 
            "IND_DAD": 2
        },
        "V010": {
            "REG": 1, 
            "NOME_INSTITUICAO": 2, 
            "PAIS": 3, 
            "TIP_MOEDA": 4
        },
        "V020": {
            "REG": 1, 
            "NOME": 2, 
            "ENDERECO": 3, 
            "TIPO_DO_C": 4, 
            "NI": 5, 
            "IDENT_CONTA": 6
        },
        "V030": {
            "REG": 1, 
            "MES": 2
        },
        "V100": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "V990": {
            "REG": 1, 
            "QTD_LIN_M": 2
        },
        "W001": {
            "REG": 1, 
            "IND_DAD": 2
        },
        "W100": {
            "REG": 1, 
            "NOME_MULTINACIONAL": 2, 
            "IND_CONTROLADORA": 3, 
            "NOME_CONTROLADORA": 4, 
            "JURISDICAO_CONTROLADORA": 5, 
            "TIN_CONTROLADORA": 6, 
            "IND_ENTREGA": 7, 
            "IND_MODALIDADE": 8, 
            "NOME_SUBSTITUTA": 9, 
            "JURISDICAO_SUBSTITUTA": 10, 
            "TIN_SUBSTITUTA": 11, 
            "DT_INI": 12, 
            "DT_FIN": 13, 
            "TIP_MOEDA": 14, 
            "IND_IDIOMA": 15
        },
        "W200": {
            "REG": 1, 
            "JURISDICAO": 2, 
            "VL_REC_NAO_REL_EST": 3, 
            "VL_REC_NAO_REL": 4, 
            "VL_REC_REL_EST": 5, 
            "VL_REC_REL": 6, 
            "VL_REC_TOTAL_EST": 7, 
            "VL_REC_TOTAL": 8, 
            "VL_LUC_PREJ_ANTES_IR_EST": 9, 
            "VL_LUC_PREJ_ANTES_IR": 10, 
            "VL_IR_PAGO_EST": 11, 
            "VL_IR_PAGO": 12, 
            "VL_IR_DEVIDO_EST": 13, 
            "VL_IR_DEVIDO": 14, 
            "VL_CAP_SOC_EST": 15, 
            "VL_CAP_SOC": 16, 
            "VL_LUC_ACUM_EST": 17, 
            "VL_LUC_ACUM": 18, 
            "VL_ATIV_TANG_EST": 19, 
            "VL_ATIV_TANG": 20, 
            "NUM_EMP": 21
        },
        "W250": {
            "REG": 1, 
            "JUR_DIFERENTE": 2, 
            "NOME": 3, 
            "TIN": 4, 
            "JURISDICAO_TIN": 5, 
            "NI": 6, 
            "JURISDICAO_NI": 7, 
            "TIPO_NI": 8, 
            "TIP_END": 9, 
            "ENDEREÃ‡O": 10, 
            "NUM_TEL": 11, 
            "EMAIL": 12, 
            "ATIV_1": 13, 
            "ATIV_2": 14, 
            "ATIV_3": 15, 
            "ATIV_4": 16, 
            "ATIV_5": 17, 
            "ATIV_6": 18, 
            "ATIV_7": 19, 
            "ATIV_8": 20, 
            "ATIV_9": 21, 
            "ATIV_10": 22, 
            "ATIV_11": 23, 
            "ATIV_12": 24, 
            "ATIV_13": 25, 
            "DESC_OUTROS": 26, 
            "OBSERVAÃ‡ÃƒO": 27
        },
        "W300": {
            "REG": 1, 
            "JURISDICAO": 2, 
            "IND_REC_NAO_REL": 3, 
            "IND_REC_REL": 4, 
            "IND_REC_TOTAL": 5, 
            "IND_LUC_PREJ_ANTES_IR": 6, 
            "IND_IR_PAGO": 7, 
            "IND_IR_DEVIDO": 8, 
            "IND_CAP_SOC": 9, 
            "IND_LUC_ACUM": 10, 
            "IND_ATIV_TANG": 11, 
            "IND_NUM_EMP": 12, 
            "OBSERVAÃ‡ÃƒO": 13, 
            "FIM_OBSERVACAO": 14
        },
        "W990": {
            "REG": 1, "QTD_LIN": 2
        },
        "X001": {
            "REG": 1, 
            "IND_DAD": 2
        },
        "X280": {
            "REG": 1, 
            "IND_ATIV": 2, 
            "IND_CONCEDENTE": 3, 
            "IND_PROJ": 4, 
            "ATO_CONC": 5, 
            "VIG_INI": 6, 
            "VIG_FIM": 7, 
            "CNPJ_INCENTIVO": 8, 
            "NCM_INCENTIVO": 9, 
            "REC_LIQ_INCENTIVO": 10, 
            "VL_INCENTIVO": 11
        },
        "X292": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "X340": {
            "REG": 1, 
            "RAZ_SOCIAL": 2, 
            "NIF": 3, 
            "IND_CONTROLE": 4, 
            "PAIS": 5, 
            "IND_ISEN_PETR": 6, 
            "IND_CONSOL": 7, 
            "MOT_NAO_CONSOL": 8, 
            "CNPJ": 9, 
            "TIP_MOEDA": 10
        },
        "X350": {
            "REG": 1, 
            "REC_LIQ": 2, 
            "CUSTOS": 3, 
            "LUC_BRUTO": 4, 
            "REC_AUFERIDAS": 5, 
            "REC_OUTRAS_OPER": 6, 
            "DESP_BRASIL": 7, 
            "DESP_OPER": 8, 
            "LUC_OPER": 9, 
            "REC_PARTIC": 10, 
            "REC_OUTRAS": 11, 
            "DESP_OUTRAS": 12, 
            "LUC_LIQ_ANT_IR": 13, 
            "LUC_ARB_ANT_IR": 14, 
            "IMP_DEV": 15, 
            "LUC_LIQ": 16
        },
        "X351": {
            "REG": 1, 
            "RES_INV_PER": 2, 
            "RES_INV_PER_REAL": 3, 
            "RES_ISEN_PETR_PER": 4, 
            "RES_ISEN_PETR_PER_REAL": 5, 
            "RES_NEG_ACUM": 6, 
            "RES_NEG_ACUM_REAL": 7, 
            "RES_POS_TRIB": 8, 
            "RES_POS_TRIB_REAL": 9, 
            "IMP_LUCR": 10, 
            "IMP_LUCR_REAL": 11, 
            "IMP_PAG_REND": 12, 
            "IMP_PAG_REND_REAL": 13, 
            "IMP_RET_EXT": 14, 
            "IMP_RET_EXT_REAL": 15, 
            "IMP_RET_BR": 16
        },
        "X352": {
            "REG": 1, 
            "RES_PER": 2, 
            "RES_PER_REAL": 3, 
            "LUC_DISP": 4, 
            "LUC_DISP_REAL": 5
        },
        "X353": {
            "REG": 1, 
            "RES_NEG_UTIL": 2, 
            "RES_NEG_UTIL_REAL": 3, 
            "SALDO_RES_NEG_NAO_UTIL": 4, 
            "SALDO_RES_NEG_NAO_UTIL_REAL": 5, 
            "RES_PROP": 6, 
            "RES_PROP_REAL": 7
        },
        "X354": {
            "REG": 1, 
            "RES_NEG_ANT": 2, 
            "RES_NEG_ANT_REAL": 3, 
            "SALDO_NEG_ACUM": 4
        },
        "X355": {
            "REG": 1, 
            "REND_PASS_PROP": 2, 
            "REND_PASS_PROP_REAL": 3, 
            "REND_TOTAL": 4, 
            "REND_TOTAL_REAL": 5, 
            "REND_ATIV_PROP": 6, 
            "REND_ATIV_PROP_REAL": 7, 
            "PERCENTUAL": 8
        },
        "X356": {
            "REG": 1, 
            "PERC_PART": 2, 
            "ATIVO_TOTAL": 3, 
            "PAT_LIQUIDO": 4
        },
        "X357": {
            "REG": 1, 
            "PAIS": 2, 
            "NIF/CNPJ": 3, 
            "RAZAO_SOCIAL": 4, 
            "PERCENTUAL": 5
        },
        "X360": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "X365": {
            "REG": 1, 
            "IDENTIFICADOR": 2, 
            "NOME_ENT": 3
        },
        "X366": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "X370": {
            "REG": 1, 
            "IDENTIFICADOR": 2, 
            "TIPO_TRANSACAO": 3, 
            "NOME_ENT": 4, 
            "PAIS": 5, 
            "COD_NCM": 6, 
            "TIPO_DEMAIS": 7, 
            "DESCR_BSDI": 8, 
            "VL_TRANSACAO": 9, 
            "IND_AJUSTES": 10, 
            "VL_ESPONTANEO": 11, 
            "VL_COMPENSATORIO": 12, 
            "TIP_AJ_COMPENSATORIO": 13, 
            "METODO": 14, 
            "DESCRICAO": 15, 
            "COMP_INTENCIONAL": 16, 
            "SINERGIA": 17, 
            "IND_TRANS_COMBINADAS": 18, 
            "IND_DADOS_MULTIP": 19, 
            "IND_SIMPLIFIC": 20
        },
        "X371": {
            "REG": 1, 
            "COD_CTA": 2, 
            "COD_CCUS": 3, 
            "VALOR": 4, 
            "IND_VALOR": 5
        },
        "X375": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "X390": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "X400": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "X410": {
            "REG": 1, 
            "PAIS": 2, 
            "IND_HOME_DISP": 3, 
            "IND_SERV_DISP": 4
        },
        "X420": {
            "REG": 1, 
            "TIP_ROY": 2, 
            "PAIS": 3, 
            "VL_EXPL_DIR_SW": 4, 
            "VL_EXPL_DIR_AUT": 5, 
            "VL_EXPL_MARCA": 6, 
            "VL_EXPL_PAT": 7, 
            "VL_EXPL_KNOW": 8, 
            "VL_EXPL_FRANQ": 9, 
            "VL_EXPL_INT": 10
        },
        "X430": {
            "REG": 1, 
            "PAIS": 2, 
            "VL_SERV_ASSIST": 3, 
            "VL_SERV_SEM_ASSIST": 4, 
            "VL_SERV_SEM_ASSIST_EXT": 5, 
            "VL_JURO": 6, 
            "VL_DEMAIS_JUROS": 7, 
            "VL_DIVID": 8
        },
        "X450": {
            "REG": 1, 
            "PAIS": 2
        },
        "X451": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "X460": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "X470": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "X480": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "X485": {
            "REG": 1, 
            "TIPO_BENEF": 2, 
            "ATO_DECL": 3, 
            "CNPJ_INCORP": 4, 
            "ID_OBRA_2018": 5, 
            "ID_OBRA_2020": 6, 
            "ID_OBRA_EEI": 7, 
            "PORT_CEBAS": 8, 
            "DT_DOU_PORT_CEBAS": 9, 
            "DT_INI_PORT_CEBAS": 10, 
            "DT_FIN_PORT_CEBAS": 11
        },
        "X490": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "X500": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "X510": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "X990": {
            "REG": 1, 
            "QTD_LIN": 2
        },
        "Y001": {
            "REG": 1, 
            "IND_DAD": 2
        },
        "Y520": {
            "REG": 1, 
            "TIP_EXT": 2, 
            "PAIS": 3, 
            "FORMA": 4, 
            "NAT_OPER": 5, 
            "VL_PERIODO": 6
        },
        "Y570":{
            "REG": 1,
            "CNPJ_FON": 2,
            "NOM_EMP": 3,
            "IND_ORG_PUB": 4,
            "COD_REC": 5,
            "VL_REND": 6,
            "IR_RET": 7,
            "CSLL_RET": 8,
        },
        "Y590": {
            "REG": 1, 
            "TIP_ATIVO": 2, 
            "PAIS": 3, 
            "DISCRIMINACAO": 4, 
            "VL_ANT": 5, 
            "VL_ATUAL": 6},
        "Y600": {
            "REG": 1, 
            "DT_ALT_SOC": 2, 
            "DT_FIM_SOC": 3, 
            "PAIS": 4, 
            "IND_QUALIF": 5, 
            "CPF_CNPJ": 6, 
            "NOM_EMP": 7, 
            "QUALIF": 8, 
            "PERC_CAP_TOT": 9, 
            "PERC_CAP_VOT": 10, 
            "CPF_REP_LEG": 11, 
            "QUALIF_REP_LEG": 12, 
            "VL_REM_TRAB": 13, 
            "VL_LUC_DIV": 14, 
            "VL_JUR_CAP": 15, 
            "VL_DEM_REND": 16, 
            "VL_IR_RET": 17
        },
        "Y612": {
            "REG": 1, 
            "CPF": 2, 
            "NOME": 3, 
            "QUALIF": 4, 
            "VL_REM_TRAB": 5, 
            "VL_DEM_REND": 6, 
            "VL_IR_RET": 7
        },
        "Y620": {
            "REG": 1, 
            "DT_EVENTO": 2, 
            "IND_RELAC": 3, 
            "PAIS": 4, 
            "CNPJ": 5, 
            "NOM_EMP": 6, 
            "VALOR_REAIS": 7, 
            "VALOR_ESTR": 8, 
            "PERC_CAP_TOT": 9, 
            "PERC_CAP_VOT": 10, 
            "RES_EQ_PAT": 11, 
            "DATA_AQUIS": 12, 
            "IND_PROC_CART": 13, 
            "NUM_PROC_CART": 14, 
            "NOME_CART": 15, 
            "IND_PROC_RFB": 16, 
            "NUM_PROC_RFB": 17
        },
        "Y630": {
            "REG": 1, 
            "CNPJ": 2, 
            "QTE_QUOT": 3, 
            "QTE_QUOTA": 4, 
            "PATR_FIN_PER": 5, 
            "DAT_ABERT": 6, 
            "DAT_ENCER": 7
        },
        "Y640": {
            "REG": 1, 
            "CNPJ": 2, 
            "COND_DECL": 3, 
            "VL_CONS": 4, 
            "CNPJ_LID": 5, 
            "VL_DECL": 6
        },
        "Y650": {
            "REG": 1, 
            "CNPJ": 2, 
            "VL_PART": 3
        },
        "Y660": {
            "REG": 1, 
            "CNPJ": 2, 
            "NOM_EMP": 3, 
            "PERC_PAT_LIQ": 4
        },
        "Y672": {
            "REG": 1, 
            "VL_CAPITAL_ANT": 2, 
            "VL_CAPITAL": 3, 
            "VL_ESTOQUE_ANT": 4, 
            "VL_ESTOQUES": 5, 
            "VL_CAIXA_ANT": 6, 
            "VL_CAIXA": 7, 
            "VL_APLIC_FIN_ANT": 8, 
            "VL_APLIC_FIN": 9, 
            "VL_CTA_REC_ANT": 10, 
            "VL_CTA_REC": 11, 
            "VL_CTA_PAG_ANT": 12, 
            "VL_CTA_PAG": 13, 
            "VL_COMPRA_MERC": 14, 
            "VL_COMPRA_ATIVO": 15, 
            "VL_RECEITAS": 16, 
            "TOT_ATIVO": 17, 
            "IND_AVAL_ESTOQ": 18
        },
        "Y680": {
            "REG": 1, 
            "MES": 2
        },
        "Y681": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "Y682": {
            "REG": 1, 
            "MES": 2, 
            "ACRES_PATR": 3
        },
        "Y720": {
            "REG": 1, 
            "LUC_LIQ": 2, 
            "DT_LUC_LIQ": 3, 
            "REC_BRUT_ANT": 4, 
            "INTIMACAO": 5, 
            "INT_ATRASO": 6
        },
        "Y730": {
            "REG": 1, 
            "DEDUCAO": 2, 
            "TIPO": 3, 
            "DATA": 4, 
            "TIPO_DESTINATARIO": 5, 
            "DESTINATARIO": 6, 
            "VALOR": 7, 
            "OBSERVACAO": 8
        },
        "Y750": {
            "REG": 1, 
            "CODIGO": 2, 
            "DESCRICAO": 3, 
            "VALOR": 4
        },
        "Y800": {
            "REG": 1, 
            "TIPO_DOC": 2, 
            "DESCRICAO": 3, 
            "HASH": 4, 
            "ARQ_RTF": 5, 
            "IND_FIM_RTF": 6
        },
        "Y990": {
            "REG": 1, 
            "QTD_LIN": 2
        },
        "9001": {
            "REG": 1,
            "IND_DAD": 2
        },
        "9100": {
            "REG": 1,
            "NOM_REGRA": 2,
            "MSG_REGRA": 3,
            "REGISTRO": 4,
            "CAMPO": 5,
            "CONTEÃšDO": 6,
            "VALOR_ESPERADO": 7,
            "PER_APUR": 8,
            "COD_CTA": 9,
            "COD_CCUS": 10,
            "COD_CTA_REF": 11,
            "CODIGO": 12,
            "NUM_ORDEM": 13,
            "CNPJ_ESTAB": 14,
            "CNAE": 15,
            "COD_CTA_B": 16,
            "COD_TRIBUTO": 17
        },
        "9900": {
            "REG": 1,
            "REG_BLC": 2,
            "QTD_REG_BLC": 3,
            "VERSAO": 4,
            "ID_TAB_DIN": 5
        },
        "9990": {
            "REG": 1,
            "QTD_LIN": 2
        },
        "9999": {
            "REG": 1,
            "QTD_LIN": 2
        },
    }


