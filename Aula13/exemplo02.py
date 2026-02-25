import polars as pl
from datetime import datetime

try:
    inicio = datetime.now()
    print('Lendo arquivos parquet')
    df_bolsa_familia = pl.read_parquet('bolsa_familia.parquet')

    print(df_bolsa_familia.head())

    fim = datetime.now()
    print(f'Tempo: {fim - inicio}')



except Exception as e:
    print(f'Erro ao obter dados parquet: {e}')