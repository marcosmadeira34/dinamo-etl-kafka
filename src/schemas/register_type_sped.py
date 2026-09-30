# ./src/dinamo_web/schemas/register_type_sped.py
from schemas.default_schemas import SpedContribSchemas, SpedFiscalSchemas, SpedEcdSchemas, SpedEcfSchemas

SPEDS_REGISTRY_TYPES = {
    "EFD_CONTRIB": {
        "schemas": SpedContribSchemas().SPED_CONTRIB_SCHEMAS,
        "fields_map": SpedContribSchemas().SPED_CONTRIB_FIELDS_MAP
    },
    
    "EFD_FISCAL": {
        "schemas": SpedFiscalSchemas().SPEDS_FISCAL_SCHEMAS,
        "fields_map": SpedFiscalSchemas().SPEDS_FISCAL_FIELDS_MAP
    },

    "ECD": {
        "schemas": SpedEcdSchemas().SPEDS_ECD_SCHEMAS,
        "fields_map": SpedEcdSchemas().SPEDS_ECD_FIELDS_MAP
    },
    
    "ECF": {
        "schemas": SpedEcfSchemas().SPEDS_ECF_SCHEMAS,
        "fields_map": SpedEcfSchemas().SPEDS_ECF_FIELDS_MAP
    }
}