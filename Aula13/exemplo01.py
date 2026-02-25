import pandas as pd
import polars as pl
from datetime import datetime
import os

ENDERECO_DADOS = './../DADOS/'

try:
    print('Obtendo dados...')
    inicio = datetime.now()

    df_bolsa_familia = None

    lista_arquivos = []

    lista_dir_arquivos = os.listdir(ENDERECO_DADOS)

    for arquivo in lista_dir_arquivos:
        if arquivo.endswith('.csv'):
            lista_arquivos.append(arquivo)

    print(lista_arquivos)

    for arquivo in lista_arquivos:
        print(f'Processando o mês {arquivo}')
        df = pl.read_csv(ENDERECO_DADOS + arquivo, separator=';', encoding='iso-8859-1')

        if df_bolsa_familia is None:
            df_bolsa_familia = df
        else:
            df_bolsa_familia = pl.concat([df_bolsa_familia, df])
        
        print(df)
    
    del df

    print('Meses concatenados com sucesso!')


except Exception as e:
    print(f'Erro ao obter dados: {e}')



# Salvando em arquivo parquet
try:
    df_bolsa_familia = df_bolsa_familia.with_columns(
        pl.col('VALOR PARCELA')
        .str.replace(',', '.')
        .cast(pl.Float64)
    )

    print('Iniciando gravação de arquivo parquet...')
    df_bolsa_familia.write_parquet('bolsa_familia.parquet')

    print('Arquivo parquet salvo com sucesso')

    fim = datetime.now()
    tempo = fim - inicio
    print(tempo)

except Exception as e:
    print(f'Erro: {e}')
