# converter_referencias_para_parquet.py
#
# Converte as 3 tabelas de referencia da Subvencao de .xlsx para .parquet
# e sobe pro bucket, nos NOVOS caminhos configurados em subvencao_config.yaml.
#
# Roda UMA VEZ (nao faz parte do pipeline — e uma migracao pontual de dado).
#
# Uso (dentro de um pod com a imagem dinamo-spark:dev, ou localmente com
# pandas + boto3 instalados e AWS_* configurados):
#   python converter_referencias_para_parquet.py
import io
import os

import boto3
import pandas as pd

BUCKET = os.environ["BUCKET_NAME"]

# (chave_antiga_xlsx, chave_nova_parquet)
CONVERSOES = [
    ("TABELA_ALIQUOTAS_INTERESTADUAIS/aliquotas_interestaduais.xlsx",
     "TABELA_ALIQUOTAS_INTERESTADUAIS/aliquotas_interestaduais.parquet"),
    # ATENCAO: confirmado que o arquivo real de municipios NAO tem extensao
    # .xlsx no bucket hoje (e so "municipios_brasil", sem sufixo) — mesmo
    # assim pandas.read_excel funciona (le pelo conteudo, nao pela extensao).
    ("TABELA_MUNICIPIO/municipios_brasil.xlsx",
     "TABELA_MUNICIPIO/municipios_brasil.parquet"),
    ("TABELA_CFOP_SUBVENCAO/tabela_cfop_subvencao.xlsx",
     "TABELA_CFOP_SUBVENCAO/tabela_cfop_subvencao.parquet"),
]


def main():
    endpoint_url = os.environ.get("AWS_ENDPOINT_URL")
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )

    for chave_xlsx, chave_parquet in CONVERSOES:
        print(f"Baixando s3://{BUCKET}/{chave_xlsx} ...")
        obj = s3.get_object(Bucket=BUCKET, Key=chave_xlsx)
        conteudo = obj["Body"].read()

        pdf = pd.read_excel(io.BytesIO(conteudo))
        pdf.columns = [str(c).strip() for c in pdf.columns]
        print(f"  -> {len(pdf)} linhas, colunas: {list(pdf.columns)}")

        buffer = io.BytesIO()
        pdf.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)

        print(f"Subindo s3://{BUCKET}/{chave_parquet} ...")
        s3.put_object(Bucket=BUCKET, Key=chave_parquet, Body=buffer.getvalue())
        print(f"  -> OK\n")

    print("Conversao concluida. Confira com:")
    for _, chave_parquet in CONVERSOES:
        print(f"  aws s3 ls s3://{BUCKET}/{chave_parquet} --endpoint-url $AWS_ENDPOINT_URL")


if __name__ == "__main__":
    main()
